"""
Stock Data Custom Operators

Airflow Connections 사용:
- stock_db: PostgreSQL 연결 (Stock Predict Database)
- kis_api: 한국투자증권 API 연결
"""

from datetime import datetime, timedelta
import pandas as pd
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults

# Airflow가 /opt/airflow/plugins를 자동으로 PYTHONPATH에 추가함
from utils.api_client import KRXAPIClient, KISAPIClient, KISMasterClient
from utils.rate_limiter import RateLimiter
from utils.technical_calculator import TechnicalCalculator
from utils.db_writer import StockDataWriter


# ============================================================
# RateLimiter 설정
# 한투 API 초당 20건 제한
# 병렬 배치 4개 동시 실행 시: 20 / 4 = 5건/초 per batch
# ============================================================
MAX_PARALLEL_BATCHES = 4
RATE_LIMIT_PER_BATCH = 20 // MAX_PARALLEL_BATCHES  # 5건/초


def get_db_url(conn_id: str = 'stock_db') -> str:
    """
    Airflow Connection에서 DB URL 가져오기

    Args:
        conn_id: Airflow connection ID (기본값: stock_db)

    Returns:
        PostgreSQL 연결 문자열
    """
    conn = BaseHook.get_connection(conn_id)
    # psycopg2 드라이버 사용을 위해 URI 직접 생성
    return f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"


def get_kis_credentials(conn_id: str = 'kis_api') -> tuple:
    """
    Airflow Connection에서 한투 API 자격증명 가져오기

    Args:
        conn_id: Airflow connection ID (기본값: kis_api)

    Returns:
        (app_key, app_secret) 튜플
    """
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson
    return extra.get('app_key'), extra.get('app_secret')


def get_krx_service_key(conn_id: str = 'krx_api') -> str:
    """
    Airflow Connection에서 금융위원회 API 서비스 키 가져오기

    Args:
        conn_id: Airflow connection ID (기본값: krx_api)

    Returns:
        service_key
    """
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson
    return extra.get('service_key')


class MarketOpenCheckOperator(BaseOperator):
    """
    거래일(개장일) 여부 확인 Operator

    한투 API를 통해 해당 날짜가 거래일인지 확인합니다.
    주말/공휴일을 모두 체크하여 정확한 거래일 여부를 반환합니다.

    Args:
        kis_conn_id: Airflow KIS API connection ID (기본값: kis_api)
        branch_if_open: 개장일인 경우 실행할 task_id 리스트
        branch_if_closed: 휴장일인 경우 실행할 task_id

    Returns:
        branch_if_open (개장일) 또는 branch_if_closed (휴장일)

    Note:
        ⚠️ 이 API는 원장서비스와 연동되어 있어 1일 1회 호출 권장
        - is_market_open (opnd_yn): 주문 가능 여부
        - is_trading_day (tr_day_yn): 거래일 여부
    """

    template_fields = ('branch_if_open', 'branch_if_closed')

    @apply_defaults
    def __init__(
        self,
        kis_conn_id='kis_api',
        branch_if_open=None,  # 개장일인 경우 실행할 task_id (리스트 가능)
        branch_if_closed='skip_collection',  # 휴장일인 경우 실행할 task_id
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.kis_conn_id = kis_conn_id
        self.branch_if_open = branch_if_open or ['get_expected_kospi', 'get_expected_kosdaq']
        self.branch_if_closed = branch_if_closed

    def execute(self, context):
        print("=" * 80)
        print("거래일(개장일) 여부 확인")
        print("=" * 80)

        execution_date = context['execution_date']
        target_date = execution_date.strftime('%Y%m%d')
        target_date_fmt = execution_date.strftime('%Y-%m-%d')

        print(f"조회 날짜: {target_date_fmt}")
        print(f"요일: {execution_date.strftime('%A')}")

        # 1. 주말 먼저 체크 (API 호출 최소화)
        if execution_date.weekday() >= 5:  # 토요일(5), 일요일(6)
            print("⚠️  주말입니다. API 호출 없이 휴장일 처리합니다.")
            context['task_instance'].xcom_push(key='is_market_open', value=False)
            context['task_instance'].xcom_push(key='market_status', value='WEEKEND')
            return self.branch_if_closed

        # 2. 한투 API로 휴장일 체크 (평일인 경우만)
        kis_app_key, kis_app_secret = get_kis_credentials(self.kis_conn_id)
        rate_limiter = RateLimiter(max_calls=RATE_LIMIT_PER_BATCH, time_window=1.0)
        kis_client = KISAPIClient(
            kis_app_key,
            kis_app_secret,
            rate_limiter=rate_limiter
        )

        holiday_info = kis_client.check_holiday(target_date)

        if holiday_info is None:
            # API 실패 시 주말만 체크하고 평일은 개장으로 간주
            print("⚠️  휴장일 API 호출 실패. 평일이므로 개장일로 간주합니다.")
            context['task_instance'].xcom_push(key='is_market_open', value=True)
            context['task_instance'].xcom_push(key='market_status', value='ASSUMED_OPEN')
            return self.branch_if_open

        # 3. 결과 확인
        is_open = holiday_info.get('is_market_open', False)
        
        print(f"\n[휴장일 조회 결과]")
        print(f"  - 날짜: {holiday_info['date']}")
        print(f"  - 영업일: {'Y' if holiday_info['is_business_day'] else 'N'}")
        print(f"  - 거래일: {'Y' if holiday_info['is_trading_day'] else 'N'}")
        print(f"  - 개장일(주문가능): {'Y' if is_open else 'N'}")
        print(f"  - 결제일: {'Y' if holiday_info['is_settlement_day'] else 'N'}")

        # XCom에 저장
        context['task_instance'].xcom_push(key='is_market_open', value=is_open)
        context['task_instance'].xcom_push(key='market_status', value='OPEN' if is_open else 'CLOSED')
        context['task_instance'].xcom_push(key='holiday_info', value=holiday_info)

        if is_open:
            print(f"\n✓ 개장일입니다. 데이터 수집을 시작합니다.")
            return self.branch_if_open
        else:
            print(f"\n⚠️  휴장일(공휴일)입니다. 데이터 수집을 건너뜁니다.")
            return self.branch_if_closed


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
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.load_mode = load_mode
        self.db_conn_id = db_conn_id

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

            # DB 저장
            saved_count = db_writer.upsert_stock_metadata(symbols)
            print(f"✓ DB 저장 완료: {saved_count}개")

            symbol_codes = [s['symbol'] for s in symbols]

        else:  # 'active'
            # DB에서 ACTIVE 종목만 조회
            with db_writer.engine.connect() as conn:
                from sqlalchemy import text
                query = text("SELECT symbol FROM stock_metadata WHERE status = 'ACTIVE'")
                result = conn.execute(query)
                symbol_codes = [row[0] for row in result]

            print(f"✓ ACTIVE 종목: {len(symbol_codes)}개")

        # XCom에 푸시
        context['task_instance'].xcom_push(key='symbol_list', value=symbol_codes)

        return len(symbol_codes)


class MarketDataOperator(BaseOperator):
    """
    시장 지수 데이터 수집 (fetch + calc_gaps + save 통합)

    Args:
        data_start_date: 시작일 (YYYY-MM-DD)
        data_end_date: 종료일 (YYYY-MM-DD)
        db_conn_id: Airflow DB connection ID (기본값: stock_db)
        kis_conn_id: Airflow KIS API connection ID (기본값: kis_api)
    """

    template_fields = ('data_start_date', 'data_end_date')

    @apply_defaults
    def __init__(
        self,
        data_start_date=None,
        data_end_date=None,
        db_conn_id='stock_db',
        kis_conn_id='kis_api',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.data_start_date = data_start_date
        self.data_end_date = data_end_date
        self.db_conn_id = db_conn_id
        self.kis_conn_id = kis_conn_id

    def execute(self, context):
        print("=" * 80)
        print("Market Data Pipeline (fetch → calc gaps → save)")
        print("=" * 80)

        # 날짜 처리
        start = self.data_start_date or context['execution_date'].strftime('%Y-%m-%d')
        end = self.data_end_date or context['execution_date'].strftime('%Y-%m-%d')

        print(f"기간: {start} ~ {end}")

        # 1. Fetch Market Indices
        kis_app_key, kis_app_secret = get_kis_credentials(self.kis_conn_id)
        rate_limiter = RateLimiter(max_calls=RATE_LIMIT_PER_BATCH, time_window=1.0)
        kis_client = KISAPIClient(
            kis_app_key,
            kis_app_secret,
            rate_limiter=rate_limiter
        )

        try:
            kospi_data = kis_client.get_market_index(
                index_code="0001",
                start_date=start.replace('-', ''),
                end_date=end.replace('-', '')
            )

            kosdaq_data = kis_client.get_market_index(
                index_code="1001",
                start_date=start.replace('-', ''),
                end_date=end.replace('-', '')
            )

            kospi200_data = kis_client.get_market_index(
                index_code="2001",
                start_date=start.replace('-', ''),
                end_date=end.replace('-', '')
            )


            kospi_df = pd.DataFrame(kospi_data)
            kosdaq_df = pd.DataFrame(kosdaq_data)
            kospi200_df = pd.DataFrame(kospi200_data)

            # kospi + kosdaq 합치기
            market_df = kospi_df.merge(kosdaq_df, on='date', suffixes=('_kospi', '_kosdaq'))
            # kospi200 합치기
            market_df = market_df.merge(kospi200_df, on='date')
            
            market_df = market_df.rename(columns={
                'open_kospi': 'kospi_open',
                'high_kospi': 'kospi_high',
                'low_kospi': 'kospi_low',
                'close_kospi': 'kospi_close',
                'volume_kospi': 'kospi_volume',
                'open_kosdaq': 'kosdaq_open',
                'high_kosdaq': 'kosdaq_high',
                'low_kosdaq': 'kosdaq_low',
                'close_kosdaq': 'kosdaq_close',
                'volume_kosdaq': 'kosdaq_volume',
                'open': 'kospi200_open',
                'high': 'kospi200_high',
                'low': 'kospi200_low',
                'close': 'kospi200_close',
                'volume': 'kospi200_volume',
            })

            print(f"✓ 수집된 시장 지수 데이터: {len(market_df)}일")

        except NotImplementedError:
            print("⚠️  한투 API가 아직 구현되지 않았습니다.")
            print("⚠️  임시 샘플 데이터를 생성합니다.")

            dates = pd.date_range(start=start, end=end, freq='D')
            market_df = pd.DataFrame({
                'date': dates,
                'kospi_open': 2500,
                'kospi_high': 2520,
                'kospi_low': 2480,
                'kospi_close': 2510,
                'kospi_volume': 500000000,
                'kosdaq_open': 850,
                'kosdaq_high': 860,
                'kosdaq_low': 840,
                'kosdaq_close': 855,
                'kosdaq_volume': 800000000,
                'kospi200_open': 280,
                'kospi200_high': 282,
                'kospi200_low': 278,
                'kospi200_close': 281,
                'kospi200_volume': 300000000,
            })

        # 2. Calculate Market Gaps (DB에서 전일 종가 조회)
        db_url = get_db_url(self.db_conn_id)
        calculator = TechnicalCalculator()
        market_df = calculator._calculate_market_gaps(market_df, db_url=db_url)

        print(f"✓ 갭 계산 완료: {len(market_df)}일")

        # 3. Save to DB
        db_writer = StockDataWriter(db_url)
        saved_count = db_writer.upsert_market_indices(market_df)

        print(f"✓ DB 저장 완료: {saved_count}행")

        # XCom에 저장
        context['task_instance'].xcom_push(
            key='market_data_with_gaps',
            value=market_df.to_dict('records')
        )

        return saved_count


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

    template_fields = ('data_start_date', 'data_end_date', 'symbol_batch_index')

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
                    end_date=end.replace('-', '')
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
                FROM gap_predictions
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
                    FROM gap_predictions
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


class GapUpFilterOperator(BaseOperator):
    """
    갭상승 종목 필터링

    예상체결 상승 종목 중 실제 장 시작 후 갭상승한 종목만 필터링

    Args:
        gap_threshold: 갭상승 기준 (기본값: 0.0, 0% 이상이면 갭상승)
        kis_conn_id: Airflow KIS API connection ID (기본값: kis_api)
        xcom_keys: 입력 XCom 키 리스트 (예상상승종목 리스트들)
        xcom_task_ids: XCom을 가져올 task_id 리스트 (xcom_keys와 매핑)

    Note:
        - 장 시작 직후 (09:00~09:10) 에 실행하는 것이 효과적입니다.
        - 시가와 전일종가를 비교하여 갭상승 여부 판단
        - 여러 소스(KOSPI, KOSDAQ)에서 데이터를 병합하여 처리
    """

    template_fields = ('gap_threshold',)

    @apply_defaults
    def __init__(
        self,
        gap_threshold=0.0,  # 갭상승 기준 (%)
        kis_conn_id='kis_api',
        xcom_keys=None,  # 여러 XCom 키 지원 (리스트)
        xcom_task_ids=None,  # 해당 task_id 리스트
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.gap_threshold = gap_threshold
        self.kis_conn_id = kis_conn_id
        # 기본값: 단일 키 (하위 호환성)
        self.xcom_keys = xcom_keys or ['expected_상승_ranking']
        self.xcom_task_ids = xcom_task_ids

    def execute(self, context):
        print("=" * 80)
        print(f"갭상승 종목 필터링 (기준: {self.gap_threshold}% 이상)")
        print("=" * 80)

        # XCom에서 예상체결 상승 종목 가져오기 (여러 소스 병합)
        expected_ranking = []
        
        for i, key in enumerate(self.xcom_keys):
            task_id = self.xcom_task_ids[i] if self.xcom_task_ids else None
            data = context['task_instance'].xcom_pull(key=key, task_ids=task_id)
            if data:
                # 데이터에 출처(exchange) 정보 추가
                source_name = "KOSPI" if "kospi" in (task_id or "").lower() else "KOSDAQ" if "kosdaq" in (task_id or "").lower() else "UNKNOWN"
                for item in data:
                    if 'exchange' not in item:
                        item['exchange'] = source_name
                expected_ranking.extend(data)
                print(f"  ✓ {task_id or key}: {len(data)}개")

        if not expected_ranking:
            print("⚠️  예상체결 상승 종목이 없습니다.")
            return 0

        # 중복 제거 (동일 종목코드)
        seen = set()
        unique_ranking = []
        for item in expected_ranking:
            if item['symbol'] not in seen:
                seen.add(item['symbol'])
                unique_ranking.append(item)
        
        expected_ranking = unique_ranking
        print(f"✓ 예상체결 상승 종목 (중복제거): {len(expected_ranking)}개")

        # 한투 API 클라이언트 초기화
        kis_app_key, kis_app_secret = get_kis_credentials(self.kis_conn_id)
        rate_limiter = RateLimiter(max_calls=RATE_LIMIT_PER_BATCH, time_window=1.0)
        kis_client = KISAPIClient(
            kis_app_key,
            kis_app_secret,
            rate_limiter=rate_limiter
        )

        # 현재가 조회 및 갭상승 필터링
        gap_up_stocks = []
        symbols = [item['symbol'] for item in expected_ranking]

        current_prices = kis_client.get_current_price_batch(symbols)

        # DB에서 전일 종가 일괄 조회
        db_url = get_db_url()
        db_writer = StockDataWriter(db_url)
        prev_close_map = db_writer.get_prev_close_batch(symbols)
        print(f"✓ DB에서 전일종가 조회: {len(prev_close_map)}개 종목")

        for price_data in current_prices:
            symbol = price_data['symbol']
            open_price = price_data['open_price']
            # DB에서 전일 종가 가져오기 (없으면 현재가-전일대비로 계산)
            if symbol in prev_close_map:
                prev_close = prev_close_map[symbol]
            else:
                # fallback: 현재가 - 전일대비 = 전일종가
                prev_close = price_data['current_price'] - price_data.get('change_price', 0)

            # 갭 계산 (시가 vs 전일종가)
            if prev_close > 0:
                gap_rate = ((open_price - prev_close) / prev_close) * 100
            else:
                gap_rate = 0.0

            # 갭상승 조건 충족 시 추가
            if gap_rate >= self.gap_threshold:
                # 원본 예상체결 데이터와 현재가 데이터 병합
                original_data = next(
                    (item for item in expected_ranking if item['symbol'] == symbol),
                    {}
                )

                gap_up_stocks.append({
                    **original_data,
                    "open_price": open_price,
                    "prev_close": prev_close,
                    "gap_rate": round(gap_rate, 2),
                    "current_price": price_data['current_price'],
                    "volume": price_data['volume'],
                    "name": original_data.get('name', ''),  # 예상체결 API에서 가져온 종목명 사용
                })

        # 갭상승률 순으로 정렬
        gap_up_stocks.sort(key=lambda x: x['gap_rate'], reverse=True)

        print(f"✓ 갭상승 종목: {len(gap_up_stocks)}개")

        # 상위 종목 출력
        print("\n[갭상승 종목 TOP 10]")
        for i, item in enumerate(gap_up_stocks[:10], 1):
            print(f"  {i:2d}. {item.get('name', ''):15s} ({item['symbol']}) "
                  f"| 시가: {item['open_price']:,}원 "
                  f"| 갭: {item['gap_rate']:+.2f}%")

        # XCom에 저장
        context['task_instance'].xcom_push(key='gap_up_stocks', value=gap_up_stocks)

        # 종목 코드 리스트도 별도 저장
        symbol_list = [item['symbol'] for item in gap_up_stocks]
        context['task_instance'].xcom_push(key='gap_up_symbols', value=symbol_list)

        return len(gap_up_stocks)


class IndexCurrentOperator(BaseOperator):
    """
    시장 지수 현재가 조회

    KOSPI, KOSDAQ, KOSPI200 현재지수를 조회합니다.

    Args:
        kis_conn_id: Airflow KIS API connection ID (기본값: kis_api)

    Note:
        - 장 중에 실행해야 유효한 데이터입니다.
        - 장 시작 전에는 전일 종가 기준 데이터가 반환됩니다.
    """

    @apply_defaults
    def __init__(
        self,
        kis_conn_id='kis_api',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.kis_conn_id = kis_conn_id

    def execute(self, context):
        print("=" * 80)
        print("시장 지수 현재가 조회")
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

        # 지수 현재가 조회
        index_data = kis_client.get_all_index_current_prices()

        if not index_data:
            print("⚠️  시장 지수 조회 실패")
            return 0

        # 결과 출력
        print("\n[시장 지수 현재가]")
        for name, data in index_data.items():
            print(f"  {data['index_name']:10s}: {data['current_value']:,.2f} "
                  f"({data['change_rate']:+.2f}%) "
                  f"| 시가: {data['open_value']:,.2f}")

        # XCom에 저장
        context['task_instance'].xcom_push(key='market_indices', value=index_data)

        return len(index_data)


class KafkaPublishOperator(BaseOperator):
    """
    카프카 메시지 발행

    갭상승 종목과 시장 지수를 결합하여 하나의 배치 메시지로 카프카 발행

    Args:
        kafka_topic: 카프카 토픽 (기본값: extract_daily_candidate)
        kafka_conn_id: Airflow Kafka connection ID (기본값: kafka_default)
        gap_up_xcom_key: 갭상승 종목 XCom 키
        index_xcom_key: 시장 지수 XCom 키

    메시지 형식 (배치 메시지):
        {
            "timestamp": "2026-01-13T09:05:00+09:00",
            "kospi_open": 2550.12,
            "kosdaq_open": 850.45,
            "kospi200_open": 350.5,
            "total_count": 2,
            "stocks": [
                {
                    "stock_code": "005930",
                    "stock_name": "삼성전자",
                    "exchange": "KOSPI",
                    "stock_open": 58000.0,
                    "gap_rate": 2.35,
                    "expected_change_rate": 2.1,
                    "volume": 1000000
                },
                ...
            ]
        }
    """

    template_fields = ('kafka_topic',)

    @apply_defaults
    def __init__(
        self,
        kafka_topic='extract_daily_candidate',
        kafka_conn_id='kafka_default',
        gap_up_xcom_key='gap_up_stocks',
        index_xcom_key='market_indices',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.kafka_topic = kafka_topic
        self.kafka_conn_id = kafka_conn_id
        self.gap_up_xcom_key = gap_up_xcom_key
        self.index_xcom_key = index_xcom_key

    def execute(self, context):
        import json
        from datetime import timezone as tz
        
        print("=" * 80)
        print(f"카프카 메시지 발행 (토픽: {self.kafka_topic})")
        print("=" * 80)

        execution_date = context['execution_date']

        # XCom에서 데이터 가져오기
        gap_up_stocks = context['task_instance'].xcom_pull(key=self.gap_up_xcom_key) or []
        market_indices = context['task_instance'].xcom_pull(key=self.index_xcom_key) or {}

        if not gap_up_stocks:
            print("⚠️  발행할 갭상승 종목이 없습니다.")
            return 0

        print(f"✓ 갭상승 종목: {len(gap_up_stocks)}개")
        print(f"✓ 시장 지수: {len(market_indices)}개")

        # 시장 지수 시가 추출
        kospi_open = market_indices.get('kospi', {}).get('open_value', 0)
        kosdaq_open = market_indices.get('kosdaq', {}).get('open_value', 0)
        kospi200_open = market_indices.get('kospi200', {}).get('open_value', 0)

        # 타임스탬프 (KST)
        from datetime import timezone, timedelta
        kst = timezone(timedelta(hours=9))
        timestamp = datetime.now(kst).isoformat()

        # 메시지 생성 (개별 종목 정보)
        stock_messages = []
        for stock in gap_up_stocks:
            stock_msg = {
                "stock_code": stock['symbol'],
                "stock_name": stock.get('name', ''),
                "exchange": stock.get('exchange', 'UNKNOWN'),  # GapUpFilterOperator에서 설정된 exchange 사용
                "stock_open": float(stock.get('open_price', 0)),
                "gap_rate": float(stock.get('gap_rate', 0)),
                "expected_change_rate": float(stock.get('expected_change_rate', 0)),
                "volume": int(stock.get('volume', 0)),
            }
            stock_messages.append(stock_msg)

        # 배치 메시지 생성 (모든 종목을 하나의 메시지로 묶음)
        batch_message = {
            "timestamp": timestamp,
            "kospi_open": float(kospi_open),
            "kosdaq_open": float(kosdaq_open),
            "kospi200_open": float(kospi200_open),
            "stocks": stock_messages,
            "total_count": len(stock_messages)
        }

        # 카프카 발행 (한 번에 하나의 메시지로 발행)
        try:
            from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
            
            # Note: Airflow Kafka Provider에서는 kafka_config_id 사용 (kafka_conn_id X)
            producer_hook = KafkaProducerHook(kafka_config_id=self.kafka_conn_id)
            producer = producer_hook.get_producer()

            # 배치 메시지를 한 번에 발행
            producer.produce(
                topic=self.kafka_topic,
                key=f"gap_up_{timestamp}".encode('utf-8'),  # 배치 식별용 키
                value=json.dumps(batch_message, ensure_ascii=False).encode('utf-8')
            )

            producer.flush()
            print(f"✓ 카프카 발행 완료: {len(stock_messages)}개 종목을 하나의 배치 메시지로 발행")

        except ImportError:
            # 카프카 프로바이더가 없는 경우 로컬 파일로 저장 (개발용)
            print("⚠️  apache-airflow-providers-apache-kafka가 설치되지 않았습니다.")
            print("⚠️  메시지를 JSON 파일로 저장합니다.")

            import os
            output_dir = "/opt/airflow/data/kafka_messages"
            os.makedirs(output_dir, exist_ok=True)

            output_file = f"{output_dir}/stock_signals_{execution_date.strftime('%Y%m%d_%H%M%S')}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(batch_message, f, ensure_ascii=False, indent=2)

            print(f"✓ 파일 저장 완료: {output_file}")

        except Exception as e:
            print(f"⚠️  카프카 발행 실패: {str(e)}")
            print("⚠️  메시지를 JSON 파일로 저장합니다.")

            import os
            output_dir = "/opt/airflow/data/kafka_messages"
            os.makedirs(output_dir, exist_ok=True)

            output_file = f"{output_dir}/stock_signals_{execution_date.strftime('%Y%m%d_%H%M%S')}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(batch_message, f, ensure_ascii=False, indent=2)

            print(f"✓ 파일 저장 완료: {output_file}")

        # 상위 메시지 출력
        print("\n[발행 메시지 샘플 (전체)]")
        for stock_msg in stock_messages:
            stock_display = stock_msg['stock_name'] or stock_msg['stock_code']  # name이 없으면 코드 출력
            print(f"  {stock_msg['exchange']:6s} {stock_display} ({stock_msg['stock_code']}): "
                  f"시가 {stock_msg['stock_open']:,}원, 갭 {stock_msg['gap_rate']:+.2f}%")

        # XCom에 저장
        context['task_instance'].xcom_push(key='published_messages', value=batch_message)

        return len(stock_messages)


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
