"""
Gap Up Filter Operator

갭상승 종목 필터링을 위한 Airflow Operator
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from utils.rate_limiter import RateLimiter
from utils.api_client import KISAPIClient
from utils.db_writer import StockDataWriter
from utils.common import (
    get_db_url,
    get_kis_credentials,
    RATE_LIMIT_PER_BATCH,
)


class GapUpFilterOperator(BaseOperator):
    """
    갭상승 종목 필터링

    예상체결 상승 종목 중 실제 장 시작 후 갭상승한 종목만 필터링

    Args:
        gap_threshold: 갭상승 기준 (기본값: 0.0, 0% 이상이면 갭상승)
        kis_conn_id: Airflow KIS API connection ID (기본값: kis_api)
        xcom_keys: 입력 XCom 키 리스트 (예상상승종목 리스트들)
        xcom_task_ids: XCom을 가져올 task_id 리스트 (xcom_keys와 매핑)
        market_code: 시장 구분 코드
            - "J": KRX (정규장, 09:00~)
            - "NX": NXT (08:00~)
            - "UN": 통합 (NXT + KRX)

    Note:
        - KRX: 장 시작 직후 (09:00~09:10) 에 실행
        - NXT: 08:00~ 에 실행
        - 시가와 전일종가를 비교하여 갭상승 여부 판단
        - 여러 소스(KOSPI, KOSDAQ)에서 데이터를 병합하여 처리
    """

    template_fields = ('gap_threshold', 'market_code')

    @apply_defaults
    def __init__(
        self,
        gap_threshold=0.0,  # 갭상승 기준 (%)
        kis_conn_id='kis_api',
        xcom_keys=None,  # 여러 XCom 키 지원 (리스트)
        xcom_task_ids=None,  # 해당 task_id 리스트
        market_code='J',  # 시장 구분 (J: KRX, NX: NXT, UN: 통합)
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.gap_threshold = gap_threshold
        self.kis_conn_id = kis_conn_id
        self.market_code = market_code
        # 기본값: 단일 키 (하위 호환성)
        self.xcom_keys = xcom_keys or ['expected_상승_ranking']
        self.xcom_task_ids = xcom_task_ids

    def execute(self, context):
        market_name = {"J": "KRX", "NX": "NXT", "UN": "통합"}.get(self.market_code, self.market_code)
        print("=" * 80)
        print(f"갭상승 종목 필터링 (기준: {self.gap_threshold}% 이상, 시장: {market_name})")
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

        current_prices = kis_client.get_current_price_batch(symbols, market_code=self.market_code)

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
