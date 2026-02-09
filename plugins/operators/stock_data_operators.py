"""
Stock Data Custom Operators

종목 데이터 수집 및 처리 관련 오퍼레이터

Airflow Connections 사용:
- stock_db: PostgreSQL 연결 (Stock Predict Database)
- kis_api: 한국투자증권 API 연결
"""

from datetime import datetime, timedelta
import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Airflow가 /opt/airflow/plugins를 자동으로 PYTHONPATH에 추가함
from utils.api_client import KISAPIClient, KISMasterClient
from utils.rate_limiter import RateLimiter
from utils.db_writer import StockDataWriter
from utils.common import (
    get_db_url,
    get_kis_credentials,
    RATE_LIMIT_PER_BATCH,
)


class SymbolLoaderOperator(BaseOperator):
    """
    종목 리스트 로드

    Args:
        load_mode: 'all' (KIS 마스터 파일에서 전체 조회) | 'active' (DB에서 ACTIVE만 조회)
        nxt_filter: NXT 필터링 (None: 전체, 'krx_only': KRX만, 'nxt_only': NXT 가능만)
        db_conn_id: Airflow DB connection ID (기본값: stock_db)
    """

    template_fields = ('load_mode', 'nxt_filter')

    @apply_defaults
    def __init__(
        self,
        load_mode='active',
        nxt_filter=None,
        db_conn_id='stock_db',
        kis_conn_id='kis_api',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.load_mode = load_mode
        self.nxt_filter = nxt_filter
        self.db_conn_id = db_conn_id
        self.kis_conn_id = kis_conn_id

    def execute(self, context):
        filter_label = f", nxt_filter: {self.nxt_filter}" if self.nxt_filter else ""
        print("=" * 80)
        print(f"Load Symbol List (mode: {self.load_mode}{filter_label})")
        print("=" * 80)

        db_url = get_db_url(self.db_conn_id)
        db_writer = StockDataWriter(db_url)

        if self.load_mode == 'all':
            # KIS 마스터 파일에서 전체 종목 조회
            kis_master = KISMasterClient()

            symbols = kis_master.get_listed_symbols(market="ALL")
            print(f"✓ 조회된 종목 수: {len(symbols)}")

            # NXT 거래 가능 여부 조회
            kis_app_key, kis_app_secret = get_kis_credentials(self.kis_conn_id)
            rate_limiter = RateLimiter(max_calls=RATE_LIMIT_PER_BATCH, time_window=1.0)
            kis_client = KISAPIClient(
                kis_app_key,
                kis_app_secret,
                rate_limiter=rate_limiter
            )

            print("\nNXT 거래 가능 여부 조회 중...")
            nxt_count = 0
            for i, sym in enumerate(symbols):
                try:
                    stock_info = kis_client.get_stock_info(sym['symbol'])
                    if stock_info:
                        sym['is_nxt_tradable'] = stock_info.get('is_nxt_tradable', False)
                        sym['is_nxt_stopped'] = stock_info.get('is_nxt_stopped', True)
                        if sym['is_nxt_tradable'] and not sym['is_nxt_stopped']:
                            nxt_count += 1
                    else:
                        sym['is_nxt_tradable'] = None
                        sym['is_nxt_stopped'] = None
                except Exception as e:
                    print(f"  [{i+1}] {sym['symbol']}: NXT 조회 실패 - {e}")
                    sym['is_nxt_tradable'] = None
                    sym['is_nxt_stopped'] = None

                if (i + 1) % 100 == 0:
                    print(f"  진행: {i+1}/{len(symbols)} (NXT 가능: {nxt_count}개)")

            print(f"✓ NXT 조회 완료: {nxt_count}개 거래 가능")

            # DB 저장
            saved_count = db_writer.upsert_stock_metadata(symbols)
            print(f"✓ DB 저장 완료: {saved_count}개")

            symbol_codes = [s['symbol'] for s in symbols]

        else:  # 'active'
            # DB에서 ACTIVE 종목 조회 (NXT 필터 적용)
            with db_writer.engine.connect() as conn:
                from sqlalchemy import text

                if self.nxt_filter == 'nxt_only':
                    # NXT 거래 가능 종목만
                    query = text("""
                        SELECT symbol FROM stock_metadata
                        WHERE status = 'ACTIVE'
                          AND is_nxt_tradable = true
                          AND (is_nxt_stopped = false OR is_nxt_stopped IS NULL)
                    """)
                elif self.nxt_filter == 'krx_only':
                    # KRX 전용 종목 (NXT 불가 또는 NXT 정지)
                    query = text("""
                        SELECT symbol FROM stock_metadata
                        WHERE status = 'ACTIVE'
                          AND (is_nxt_tradable = false
                               OR is_nxt_tradable IS NULL
                               OR is_nxt_stopped = true)
                    """)
                else:
                    # 전체 ACTIVE 종목
                    query = text("SELECT symbol FROM stock_metadata WHERE status = 'ACTIVE'")

                result = conn.execute(query)
                symbol_codes = [row[0] for row in result]

            filter_msg = {
                'nxt_only': 'NXT 거래 가능',
                'krx_only': 'KRX 전용',
                None: 'ACTIVE 전체'
            }.get(self.nxt_filter, 'ACTIVE 전체')
            print(f"✓ {filter_msg} 종목: {len(symbol_codes)}개")

        # XCom에 푸시
        context['task_instance'].xcom_push(key='symbol_list', value=symbol_codes)

        return len(symbol_codes)


class StockDataOperator(BaseOperator):
    """
    종목 OHLCV 데이터 수집 및 저장

    Args:
        data_start_date: 시작일 (YYYY-MM-DD)
        data_end_date: 종료일 (YYYY-MM-DD)
        db_conn_id: Airflow DB connection ID (기본값: stock_db)
        kis_conn_id: Airflow KIS API connection ID (기본값: kis_api)
        batch_size: 배치 크기 (진행 로그 출력 단위)
        symbol_batch_index: 배치 인덱스 (None이면 전체 처리)
        symbols_per_batch: 배치당 종목 수 (기본 100)
        market_code: 시장 구분 (J: KRX, NX: NXT)
    """

    template_fields = ('data_start_date', 'data_end_date', 'symbol_batch_index', 'market_code')

    @apply_defaults
    def __init__(
        self,
        data_start_date=None,
        data_end_date=None,
        db_conn_id='stock_db',
        kis_conn_id='kis_api',
        batch_size=100,
        symbol_batch_index=None,
        symbols_per_batch=100,
        market_code='J',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.data_start_date = data_start_date
        self.data_end_date = data_end_date
        self.db_conn_id = db_conn_id
        self.kis_conn_id = kis_conn_id
        self.batch_size = batch_size
        self.symbol_batch_index = symbol_batch_index
        self.symbols_per_batch = symbols_per_batch
        self.market_code = market_code

    def execute(self, context):
        print("=" * 80)
        print(f"Stock Data Pipeline (fetch → save) [market: {self.market_code}]")
        print("=" * 80)

        # Airflow Connection에서 자격증명 가져오기
        db_url = get_db_url(self.db_conn_id)
        kis_app_key, kis_app_secret = get_kis_credentials(self.kis_conn_id)

        # XCom에서 종목 리스트 가져오기
        full_symbol_list = context['task_instance'].xcom_pull(key='symbol_list')

        # 배치 인덱스가 있으면 해당 배치만 처리
        if self.symbol_batch_index is not None:
            start_idx = self.symbol_batch_index * self.symbols_per_batch
            end_idx = start_idx + self.symbols_per_batch
            symbol_list = full_symbol_list[start_idx:end_idx]
            print(f"배치 {self.symbol_batch_index}: 종목 {start_idx}~{end_idx-1} ({len(symbol_list)}개)")
        else:
            symbol_list = full_symbol_list

        # 날짜 처리
        start = self.data_start_date or context['execution_date'].strftime('%Y-%m-%d')
        end = self.data_end_date or context['execution_date'].strftime('%Y-%m-%d')

        print(f"수집 대상: {len(symbol_list)}개 종목")
        print(f"기간: {start} ~ {end}")

        # Fetch Stock Prices
        rate_limiter = RateLimiter(max_calls=RATE_LIMIT_PER_BATCH, time_window=1.0)
        kis_client = KISAPIClient(
            kis_app_key,
            kis_app_secret,
            rate_limiter=rate_limiter
        )

        all_prices = []
        fail_count = 0

        for i, symbol in enumerate(symbol_list):
            try:
                data = kis_client.get_stock_ohlcv(
                    symbol=symbol,
                    start_date=start.replace('-', ''),
                    end_date=end.replace('-', ''),
                    market_code=self.market_code
                )
                for row in data:
                    row['symbol'] = symbol
                    all_prices.append(row)
            except Exception as e:
                fail_count += 1
                if fail_count <= 5:
                    print(f"  ⚠️ {symbol}: {e}")

            if (i + 1) % self.batch_size == 0:
                print(f"  진행: {i + 1}/{len(symbol_list)} 종목")

        print(f"✓ 수집 완료: {len(all_prices)}건 (실패: {fail_count}건)")

        if not all_prices:
            print("⚠️ 수집된 데이터가 없습니다.")
            return 0

        # Save to DB (단일 UPSERT)
        prices_df = pd.DataFrame(all_prices)
        db_writer = StockDataWriter(db_url)
        saved_count = db_writer.upsert_stock_prices(prices_df)

        print(f"✓ DB 저장 완료: {saved_count}행")

        return saved_count


class UpdatePredictionResultsOperator(BaseOperator):
    """
    GapPredictions 테이블의 실제 결과 업데이트
    
    daily_stock_data 완료 후 실행되어
    prediction_date가 실행 날짜인 예측들의 실제 결과를 계산하여 업데이트합니다.
    
    Args:
        target_date: 대상 날짜 (YYYY-MM-DD, 기본값: execution_date)
        db_conn_id: Airflow DB connection ID (기본값: stock_db)
    """
    
    template_fields = ('target_date',)
    
    @apply_defaults
    def __init__(
        self,
        target_date=None,
        db_conn_id='stock_db',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.target_date = target_date
        self.db_conn_id = db_conn_id
    
    def execute(self, context):
        from sqlalchemy import text
        
        print("=" * 80)
        print("예측 결과 업데이트 (Update Prediction Results)")
        print("=" * 80)
        
        # 날짜 처리
        target_date = self.target_date or context['execution_date'].strftime('%Y-%m-%d')
        print(f"대상 날짜: {target_date}")
        
        # DB 연결
        db_url = get_db_url(self.db_conn_id)
        db_writer = StockDataWriter(db_url)
        
        # 1. prediction_date가 target_date인 예측들 조회
        with db_writer.engine.connect() as conn:
            query = text("""
                SELECT id, stock_code, stock_open, predicted_direction,
                       expected_return, max_return_if_up
                FROM predictions
                WHERE prediction_date = :target_date
                  AND (actual_close IS NULL OR actual_return IS NULL)
                ORDER BY stock_code
            """)
            result = conn.execute(query, {"target_date": target_date})
            predictions = [dict(row._mapping) for row in result]
        
        if not predictions:
            print(f"✓ {target_date} 날짜의 업데이트할 예측이 없습니다.")
            return 0
        
        print(f"✓ 조회된 예측: {len(predictions)}개")
        
        # 2. stock_prices에서 실제 가격 조회
        stock_codes = [p['stock_code'] for p in predictions]
        
        with db_writer.engine.connect() as conn:
            query = text("""
                SELECT symbol, close, high, low
                FROM stock_prices
                WHERE symbol = ANY(:symbols)
                  AND date = :target_date
            """)
            result = conn.execute(query, {
                "symbols": stock_codes,
                "target_date": target_date
            })
            actual_prices = {row[0]: {'close': row[1], 'high': row[2], 'low': row[3]} 
                            for row in result}
        
        if not actual_prices:
            print(f"⚠️  {target_date} 날짜의 실제 가격 데이터가 없습니다.")
            return 0
        
        print(f"✓ 실제 가격 조회: {len(actual_prices)}개 종목")
        
        # 3. 예측 결과 계산 및 업데이트 (db_writer 메서드 사용)
        # 실제 가격 데이터가 없는 예측 필터링
        valid_predictions = []
        for pred in predictions:
            stock_code = pred['stock_code']
            if stock_code not in actual_prices:
                print(f"  ⚠️  {stock_code}: 실제 가격 데이터 없음")
                continue
            valid_predictions.append(pred)
        
        if not valid_predictions:
            print("⚠️  업데이트할 예측이 없습니다.")
            return 0
        
        updated_count = db_writer.update_prediction_results(valid_predictions, actual_prices)
        
        print(f"✓ 업데이트 완료: {updated_count}개 예측")
        
        # 4. 통계 출력
        if updated_count > 0:
            with db_writer.engine.connect() as conn:
                stats_query = text("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(direction_correct) as correct_count,
                        AVG(return_diff) as avg_return_diff,
                        AVG(ABS(return_diff)) as avg_abs_return_diff
                    FROM predictions
                    WHERE prediction_date = :target_date
                      AND direction_correct IS NOT NULL
                """)
                result = conn.execute(stats_query, {"target_date": target_date}).fetchone()
                
                if result:
                    total = result[0] or 0
                    correct = result[1] or 0
                    accuracy = (correct / total * 100) if total > 0 else 0
                    
                    print(f"\n[예측 정확도 통계]")
                    print(f"  - 총 예측 수: {total}개")
                    print(f"  - 정확한 예측: {correct}개")
                    print(f"  - 정확도: {accuracy:.2f}%")
                    if result[2]:
                        print(f"  - 평균 수익률 차이: {result[2]:.2f}%")
                        print(f"  - 평균 절대 수익률 차이: {result[3]:.2f}%")
        
        return updated_count

class SearchStockDataOperator(BaseOperator):
    """
    예상체결 상승/하락 종목 조회 (장전 시간외)

    Args:
        market: 시장 구분 (J: 전체, K: KOSPI, Q: KOSDAQ)
        sort_cls: 정렬 구분 (1: 상승률, 2: 하락률)
        blng_cls: 순위 구분 (0: 평균거래량, 1: 거래증가율, 2: 평균거래회전율, 3: 거래금액순)
        kis_conn_id: Airflow KIS API connection ID (기본값: kis_api)

    Note:
        - 장전 시간외 (08:30~09:00) 에 유효한 데이터입니다.
        - 한 번에 최대 30건 정도 반환됩니다.
    """

    template_fields = ('market', 'sort_cls', 'mkop_cls', 'fid_input')

    @apply_defaults
    def __init__(
        self,
        market='J',      # J: 전체, K: KOSPI, Q: KOSDAQ
        fid_input="0001",
        sort_cls='1',    # 0:상승률1:상승폭2:보합3:하락율4:하락폭5:체결량6:거래대금
        mkop_cls='0',    # 0:장전예상1:장마감예상
        kis_conn_id='kis_api',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.market = market
        self.sort_cls = sort_cls
        self.mkop_cls = mkop_cls
        self.kis_conn_id = kis_conn_id
        self.fid_input = fid_input
        
    def execute(self, context):
        sort_name = "상승" if self.sort_cls == "1" else "하락"
        market_name = {"J": "전체", "K": "KOSPI", "Q": "KOSDAQ"}.get(self.market, "전체")

        print("=" * 80)
        print(f"예상체결 {sort_name} 순위 조회 (시장: {market_name})")
        print("=" * 80)

        execution_date = context['execution_date']
        print(f"실행 시간: {execution_date}")

        # 한투 API 클라이언트 초기화
        kis_app_key, kis_app_secret = get_kis_credentials(self.kis_conn_id)
        rate_limiter = RateLimiter(max_calls=RATE_LIMIT_PER_BATCH, time_window=1.0)
        kis_client = KISAPIClient(
            kis_app_key,
            kis_app_secret,
            rate_limiter=rate_limiter
        )

        # 예상체결 순위 조회
        ranking_data = kis_client.get_expected_fluctuation_ranking(
            market=self.market,
            sort_cls=self.sort_cls,
            mkop_cls=self.mkop_cls,
            fid_input=self.fid_input,
        )

        print(f"✓ 조회 완료: {len(ranking_data)}건")

        # 상위 10개 출력
        print("\n[상위 10 종목]")
        for item in ranking_data[:10]:
            print(f"  {item['rank']:2d}. {item['name']:15s} ({item['symbol']}) "
                  f"| 현재가: {item['current_price']:,}원 "
                  f"| 예상등락률: {item['expected_change_rate']:+.2f}%")

        # XCom에 저장 (다음 task에서 사용)
        xcom_key = f"expected_{sort_name}_ranking"
        context['task_instance'].xcom_push(key=xcom_key, value=ranking_data)

        # 종목 코드 리스트도 별도 저장
        symbol_list = [item['symbol'] for item in ranking_data]
        context['task_instance'].xcom_push(key=f"expected_{sort_name}_symbols", value=symbol_list)

        return len(ranking_data)


class WebSocketStartOperator(BaseOperator):
    """
    WebSocket 시작 명령 발행

    한투 API 토큰 발급 후 Kafka로 WebSocket START 명령 발행

    Args:
        target: 대상 (PRICE, ACCOUNT, ALL)
        exchange_type: 거래소 구분 (NXT, KRX, None)
        kafka_topic: 카프카 토픽 (기본값: kis_websocket_commands)
        kafka_conn_id: Airflow Kafka connection ID (기본값: kafka_default)
        kis_conn_id: Airflow KIS API connection ID (기본값: kis_api)
    """

    template_fields = ('target', 'kafka_topic', 'exchange_type')

    @apply_defaults
    def __init__(
        self,
        target='PRICE',
        exchange_type=None,
        kafka_topic='kis_websocket_commands',
        kafka_conn_id='kafka_default',
        kis_conn_id='kis_api',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.target = target
        self.exchange_type = exchange_type
        self.kafka_topic = kafka_topic
        self.kafka_conn_id = kafka_conn_id
        self.kis_conn_id = kis_conn_id

    def execute(self, context):
        import json

        exchange_label = f", exchange: {self.exchange_type}" if self.exchange_type else ""
        print("=" * 80)
        print(f"WebSocket 시작 명령 발행 (target: {self.target}{exchange_label})")
        print("=" * 80)

        # 1. 한투 API 자격증명 가져오기
        app_key, app_secret = get_kis_credentials(self.kis_conn_id)
        print(f"✓ 자격증명 로드 완료 (conn: {self.kis_conn_id})")

        # 2. 토큰 발급 (기존 로직 - Airflow Variable 캐싱)
        kis_client = KISAPIClient(app_key, app_secret)
        access_token = kis_client.get_access_token()
        print(f"✓ 접근 토큰 발급 완료")

        # 3. 웹소켓 접속키 발급 (별도 API)
        ws_token = kis_client.get_websocket_token()
        print(f"✓ 웹소켓 접속키 발급 완료")

        # 4. WebSocket START 명령 메시지 구성
        command_message = {
            "command": "START",
            "timestamp": datetime.now().isoformat(),
            "target": self.target,
            "tokens": {
                "access_token": access_token,
                "token_type": "Bearer",
                "expires_in": 86400,
                "ws_token": ws_token,
                "ws_expires_in": 86400
            },
            "config": {
                "env_dv": "real",
                "appkey": app_key,
                "accounts": [],
                "stocks": ["005930"]  # 기본 종목: 삼성전자 (연결 유지용)
            }
        }

        # exchange_type이 지정된 경우 메시지에 포함
        if self.exchange_type:
            command_message["exchange_type"] = self.exchange_type

        # 5. Kafka 발행 (기존 KafkaPublishOperator와 동일한 방식)
        try:
            from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook

            producer_hook = KafkaProducerHook(kafka_config_id=self.kafka_conn_id)
            producer = producer_hook.get_producer()

            kafka_key = f"websocket_{self.target}"
            if self.exchange_type:
                kafka_key = f"websocket_{self.target}_{self.exchange_type}"

            producer.produce(
                topic=self.kafka_topic,
                key=kafka_key.encode('utf-8'),
                value=json.dumps(command_message, ensure_ascii=False).encode('utf-8')
            )
            producer.flush()

            print(f"✓ WebSocket START 명령 발행 완료 - topic={self.kafka_topic}")
            print(f"  - access_token: {access_token[:20]}...")
            print(f"  - ws_token: {ws_token[:20]}...")
            if self.exchange_type:
                print(f"  - exchange_type: {self.exchange_type}")

        except ImportError:
            print("⚠️  apache-airflow-providers-apache-kafka가 설치되지 않았습니다.")
            raise

        # XCom에 저장
        context['task_instance'].xcom_push(key='websocket_command', value=command_message)

        return "START command sent"
