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
from utils.technical_calculator import TechnicalCalculator
from utils.db_writer import StockDataWriter
from utils.common import (
    get_db_url,
    get_kis_credentials,
    MAX_PARALLEL_BATCHES,
    RATE_LIMIT_PER_BATCH,
)


class SymbolLoaderOperator(BaseOperator):
    """
    종목 리스트 로드

    Args:
        load_mode: 'all' (KIS 마스터 파일에서 전체 조회) | 'active' (DB에서 ACTIVE만 조회)
        db_conn_id: Airflow DB connection ID (기본값: stock_db)
    """

    template_fields = ('load_mode',)

    @apply_defaults
    def __init__(
        self,
        load_mode='active',
        db_conn_id='stock_db',
        kis_conn_id='kis_api',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.load_mode = load_mode
        self.db_conn_id = db_conn_id
        self.kis_conn_id = kis_conn_id

    def execute(self, context):
        print("=" * 80)
        print(f"Load Symbol List (mode: {self.load_mode})")
        print("=" * 80)

        db_url = get_db_url(self.db_conn_id)
        db_writer = StockDataWriter(db_url)

        if self.load_mode == 'all':
            # KIS 마스터 파일에서 전체 종목 조회
            # Reference: https://github.com/koreainvestment/open-trading-api/tree/main/stocks_info
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
            # DB에서 ACTIVE 종목만 조회
            # Note: status는 Enum(StockStatus)이므로 대문자 'ACTIVE' 사용
            with db_writer.engine.connect() as conn:
                from sqlalchemy import text
                query = text("SELECT symbol FROM stock_metadata WHERE status = 'ACTIVE'")
                result = conn.execute(query)
                symbol_codes = [row[0] for row in result]

            print(f"✓ ACTIVE 종목: {len(symbol_codes)}개")

        # XCom에 푸시
        context['task_instance'].xcom_push(key='symbol_list', value=symbol_codes)

        return len(symbol_codes)


class StockDataOperator(BaseOperator):
    """
    종목 데이터 수집 및 기술지표 계산 (fetch + save + calc + update 통합)

    Args:
        data_start_date: 시작일 (YYYY-MM-DD)
        data_end_date: 종료일 (YYYY-MM-DD)
        include_historical: 과거 데이터 포함 여부 (기술지표 계산용)
        historical_days: 과거 데이터 조회 일수 (기본 60일)
        db_conn_id: Airflow DB connection ID (기본값: stock_db)
        kis_conn_id: Airflow KIS API connection ID (기본값: kis_api)
        rate_limit: 초당 API 호출 제한
        batch_size: 배치 크기 (진행 로그 출력 단위)
        symbol_batch_index: 배치 인덱스 (None이면 전체 처리)
        symbols_per_batch: 배치당 종목 수 (기본 100)
    """

    template_fields = ('data_start_date', 'data_end_date', 'symbol_batch_index', 'market_code')

    @apply_defaults
    def __init__(
        self,
        data_start_date=None,
        data_end_date=None,
        include_historical=False,
        historical_days=60,
        db_conn_id='stock_db',
        kis_conn_id='kis_api',
        rate_limit=20,
        batch_size=100,
        symbol_batch_index=None,  # 배치 인덱스 (0, 1, 2, ...)
        symbols_per_batch=100,     # 배치당 종목 수
        market_code='J',           # 시장 구분 (J: KRX, NX: NXT, UN: 통합)
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.data_start_date = data_start_date
        self.data_end_date = data_end_date
        self.include_historical = include_historical
        self.historical_days = historical_days
        self.db_conn_id = db_conn_id
        self.kis_conn_id = kis_conn_id
        self.rate_limit = rate_limit
        self.batch_size = batch_size
        self.symbol_batch_index = symbol_batch_index
        self.symbols_per_batch = symbols_per_batch
        self.market_code = market_code

    def execute(self, context):
        print("=" * 80)
        print("Stock Data Pipeline (fetch → save → calc → update)")
        print("=" * 80)

        # Airflow Connection에서 자격증명 가져오기
        db_url = get_db_url(self.db_conn_id)
        kis_app_key, kis_app_secret = get_kis_credentials(self.kis_conn_id)

        # XCom에서 종목 리스트 가져오기
        full_symbol_list = context['task_instance'].xcom_pull(key='symbol_list')
        market_data = context['task_instance'].xcom_pull(key='market_data_with_gaps')

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

        # 1. Fetch Stock Prices (전역 RateLimiter 사용)
        rate_limiter = RateLimiter(max_calls=RATE_LIMIT_PER_BATCH, time_window=1.0)
        kis_client = KISAPIClient(
            kis_app_key,
            kis_app_secret,
            rate_limiter=rate_limiter
        )

        all_prices = []

        try:
            for i, symbol in enumerate(symbol_list):
                data = kis_client.get_stock_ohlcv(
                    symbol=symbol,
                    start_date=start.replace('-', ''),
                    end_date=end.replace('-', ''),
                    market_code=self.market_code
                )
                for row in data:
                    row['symbol'] = symbol
                    all_prices.append(row)

                if (i + 1) % self.batch_size == 0:
                    print(f"  진행: {i + 1}/{len(symbol_list)} 종목")

            print(f"✓ 수집 완료: {len(all_prices)}개 데이터")

        except NotImplementedError:
            print("⚠️  한투 API가 아직 구현되지 않았습니다.")
            print("⚠️  임시 샘플 데이터를 생성합니다.")

            dates = pd.date_range(start=start, end=end, freq='D')
            for symbol in symbol_list[:2]:
                for date in dates:
                    all_prices.append({
                        'symbol': symbol,
                        'date': date.strftime('%Y-%m-%d'),
                        'open': 50000,
                        'high': 51000,
                        'low': 49000,
                        'close': 50500,
                        'volume': 10000000
                    })

        # 2. Save Raw Prices
        prices_df = pd.DataFrame(all_prices)
        db_writer = StockDataWriter(db_url)
        saved_count = db_writer.upsert_stock_prices(prices_df)

        print(f"✓ 원본 데이터 저장 완료: {saved_count}행")

        # 3. Calculate Technical Indicators
        if self.include_historical:
            # 과거 데이터 포함 (daily용)
            with db_writer.engine.connect() as conn:
                from sqlalchemy import text
                exec_date = context['execution_date']
                query = text("""
                    SELECT symbol, date, open, high, low, close, volume
                    FROM stock_prices
                    WHERE date >= :start_date AND date < :target_date
                    ORDER BY symbol, date
                """)
                result = conn.execute(query, {
                    "start_date": (exec_date - timedelta(days=self.historical_days)).strftime('%Y-%m-%d'),
                    "target_date": start
                })
                historical_data = [dict(row._mapping) for row in result]

            all_data = historical_data + all_prices
            prices_df = pd.DataFrame(all_data)

        market_df = pd.DataFrame(market_data)

        calculator = TechnicalCalculator()
        prices_with_features = calculator.calculate_all_features(prices_df, market_df, db_url=db_url)

        # include_historical인 경우 당일만 필터링
        if self.include_historical:
            prices_with_features = prices_with_features[
                prices_with_features['date'] == start
            ].copy()

        print(f"✓ 기술지표 계산 완료: {len(prices_with_features)}행")

        # 검증
        validation = calculator.validate_features(prices_with_features)
        print(f"\n검증 결과:")
        print(f"  - 총 행 수: {validation['total_rows']}")
        print(f"  - 경고: {len(validation['warnings'])}개")
        for warning in validation['warnings']:
            print(f"    ⚠️  {warning}")

        # 4. Update Technical Features
        updated_count = db_writer.upsert_stock_prices(prices_with_features)

        print(f"✓ 기술지표 업데이트 완료: {updated_count}행")

        # 최적화 (backfill인 경우만)
        if not self.include_historical:
            print("\n최적화 실행 중...")
            db_writer.vacuum_analyze()
            print("✓ VACUUM ANALYZE 완료")

        # 통계
        stats = db_writer.get_stats()
        print(f"\n=== DB 통계 ===")
        print(f"총 종목 수: {stats['total_symbols']}")
        print(f"활성 종목 수: {stats['active_symbols']}")
        print(f"가격 데이터: {stats['total_price_records']:,}행")

        return updated_count


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
        kafka_topic: 카프카 토픽 (기본값: kis_websocket_commands)
        kafka_conn_id: Airflow Kafka connection ID (기본값: kafka_default)
        kis_conn_id: Airflow KIS API connection ID (기본값: kis_api)
    """

    template_fields = ('target', 'kafka_topic')

    @apply_defaults
    def __init__(
        self,
        target='PRICE',
        kafka_topic='kis_websocket_commands',
        kafka_conn_id='kafka_default',
        kis_conn_id='kis_api',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.target = target
        self.kafka_topic = kafka_topic
        self.kafka_conn_id = kafka_conn_id
        self.kis_conn_id = kis_conn_id

    def execute(self, context):
        import json

        print("=" * 80)
        print(f"WebSocket 시작 명령 발행 (target: {self.target})")
        print("=" * 80)

        # 1. 한투 API 자격증명 가져오기
        app_key, app_secret = get_kis_credentials(self.kis_conn_id)
        print(f"✓ 자격증명 로드 완료")

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

        # 4. Kafka 발행 (기존 KafkaPublishOperator와 동일한 방식)
        try:
            from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook

            producer_hook = KafkaProducerHook(kafka_config_id=self.kafka_conn_id)
            producer = producer_hook.get_producer()

            producer.produce(
                topic=self.kafka_topic,
                key=f"websocket_{self.target}".encode('utf-8'),
                value=json.dumps(command_message, ensure_ascii=False).encode('utf-8')
            )
            producer.flush()

            print(f"✓ WebSocket START 명령 발행 완료 - topic={self.kafka_topic}")
            print(f"  - access_token: {access_token[:20]}...")
            print(f"  - ws_token: {ws_token[:20]}...")

        except ImportError:
            print("⚠️  apache-airflow-providers-apache-kafka가 설치되지 않았습니다.")
            raise

        # XCom에 저장
        context['task_instance'].xcom_push(key='websocket_command', value=command_message)

        return "START command sent"
