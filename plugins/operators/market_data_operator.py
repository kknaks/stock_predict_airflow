"""
Market Data Custom Operators

시장 지수 및 거래일 관련 오퍼레이터

Airflow Connections 사용:
- stock_db: PostgreSQL 연결 (Stock Predict Database)
- kis_api: 한국투자증권 API 연결
"""

import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from utils.common import (
    get_db_url,
    get_kis_credentials,
    MAX_PARALLEL_BATCHES,
    RATE_LIMIT_PER_BATCH,
)
from utils.api_client import KISAPIClient
from utils.rate_limiter import RateLimiter
from utils.technical_calculator import TechnicalCalculator
from utils.db_writer import StockDataWriter


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

        from datetime import timezone, timedelta
        KST = timezone(timedelta(hours=9))
        execution_date = context['data_interval_end'].astimezone(KST)
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
            start_fmt = start.replace('-', '')
            end_fmt = end.replace('-', '')

            print(f"\n[1/3] KOSPI 지수 조회 중... (0001, {start_fmt}~{end_fmt})")
            kospi_data = kis_client.get_market_index(
                index_code="0001", start_date=start_fmt, end_date=end_fmt
            )
            print(f"  → KOSPI: {len(kospi_data)}건")

            print(f"[2/3] KOSDAQ 지수 조회 중... (1001, {start_fmt}~{end_fmt})")
            kosdaq_data = kis_client.get_market_index(
                index_code="1001", start_date=start_fmt, end_date=end_fmt
            )
            print(f"  → KOSDAQ: {len(kosdaq_data)}건")

            print(f"[3/3] KOSPI200 지수 조회 중... (2001, {start_fmt}~{end_fmt})")
            kospi200_data = kis_client.get_market_index(
                index_code="2001", start_date=start_fmt, end_date=end_fmt
            )
            print(f"  → KOSPI200: {len(kospi200_data)}건")

            kospi_df = pd.DataFrame(kospi_data)
            kosdaq_df = pd.DataFrame(kosdaq_data)
            kospi200_df = pd.DataFrame(kospi200_data)

            # 빈 데이터 체크
            if kospi_df.empty or kosdaq_df.empty or kospi200_df.empty:
                empty_indices = []
                if kospi_df.empty:
                    empty_indices.append("KOSPI")
                if kosdaq_df.empty:
                    empty_indices.append("KOSDAQ")
                if kospi200_df.empty:
                    empty_indices.append("KOSPI200")
                raise ValueError(f"시장 지수 데이터가 비어있습니다: {', '.join(empty_indices)} (기간: {start} ~ {end})")


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
