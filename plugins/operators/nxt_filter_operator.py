"""
NXT Filter Operator

NXT/KRX 거래소 분류를 위한 Airflow Operator
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from utils.rate_limiter import RateLimiter
from utils.api_client import KISAPIClient
from utils.common import (
    get_kis_credentials,
    RATE_LIMIT_PER_BATCH,
)


class NxtFilterOperator(BaseOperator):
    """
    NXT/KRX 거래소 분류

    예상체결 상승 종목을 NXT 거래 가능 / KRX만 가능으로 분류

    Args:
        kis_conn_id: Airflow KIS API connection ID (기본값: kis_api)
        xcom_keys: 입력 XCom 키 리스트 (예상상승종목 리스트들)
        xcom_task_ids: XCom을 가져올 task_id 리스트 (xcom_keys와 매핑)

    Output XCom:
        - nxt_stocks: NXT 거래 가능 종목 리스트
        - krx_only_stocks: KRX만 거래 가능 종목 리스트
        - nxt_symbols: NXT 거래 가능 종목 코드 리스트
        - krx_only_symbols: KRX만 거래 가능 종목 코드 리스트

    Note:
        NXT 거래 가능 조건:
        - cptt_trad_tr_psbl_yn == 'Y' (NXT 거래종목)
        - nxt_tr_stop_yn == 'N' (NXT 거래정지 아님)
    """

    template_fields = ('xcom_keys', 'xcom_task_ids')

    @apply_defaults
    def __init__(
        self,
        kis_conn_id='kis_api',
        xcom_keys=None,
        xcom_task_ids=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.kis_conn_id = kis_conn_id
        self.xcom_keys = xcom_keys or ['expected_상승_ranking']
        self.xcom_task_ids = xcom_task_ids

    def execute(self, context):
        print("=" * 80)
        print("NXT / KRX 거래소 분류")
        print("=" * 80)

        # ========================================
        # 1. XCom에서 예상체결 상승 종목 가져오기
        # ========================================
        expected_ranking = []

        for i, key in enumerate(self.xcom_keys):
            task_id = self.xcom_task_ids[i] if self.xcom_task_ids else None
            data = context['task_instance'].xcom_pull(key=key, task_ids=task_id)
            if data:
                for item in data:
                    if 'exchange' not in item:
                        source_name = "KOSPI" if "kospi" in (task_id or "").lower() else "KOSDAQ" if "kosdaq" in (task_id or "").lower() else "UNKNOWN"
                        item['exchange'] = source_name
                expected_ranking.extend(data)
                print(f"  [XCom] {task_id or key}: {len(data)}개")

        if not expected_ranking:
            print("  예상체결 상승 종목이 없습니다.")
            return 0

        # 중복 제거
        seen = set()
        unique_ranking = []
        for item in expected_ranking:
            if item['symbol'] not in seen:
                seen.add(item['symbol'])
                unique_ranking.append(item)

        expected_ranking = unique_ranking
        print(f"\n  총 종목 수 (중복제거): {len(expected_ranking)}개")

        # ========================================
        # 2. 한투 API로 NXT 거래 가능 여부 조회
        # ========================================
        kis_app_key, kis_app_secret = get_kis_credentials(self.kis_conn_id)
        rate_limiter = RateLimiter(max_calls=RATE_LIMIT_PER_BATCH, time_window=1.0)
        kis_client = KISAPIClient(
            kis_app_key,
            kis_app_secret,
            rate_limiter=rate_limiter
        )

        nxt_stocks = []
        krx_only_stocks = []

        print("\n  NXT 거래 가능 여부 조회 중...")

        for i, item in enumerate(expected_ranking):
            symbol = item['symbol']

            try:
                stock_info = kis_client.get_stock_info(symbol)

                if stock_info is None:
                    print(f"    [{i+1}/{len(expected_ranking)}] {symbol}: 조회 실패")
                    krx_only_stocks.append(item)
                    continue

                is_nxt_tradable = stock_info.get("is_nxt_tradable", False)
                is_nxt_stopped = stock_info.get("is_nxt_stopped", True)

                # NXT 거래 가능: is_nxt_tradable=Y AND is_nxt_stopped=N
                if is_nxt_tradable and not is_nxt_stopped:
                    item['is_nxt'] = True
                    nxt_stocks.append(item)
                else:
                    item['is_nxt'] = False
                    krx_only_stocks.append(item)

                if (i + 1) % 10 == 0:
                    print(f"    [{i+1}/{len(expected_ranking)}] 조회 중... (NXT: {len(nxt_stocks)}개)")

            except Exception as e:
                print(f"    [{i+1}/{len(expected_ranking)}] {symbol}: 에러 - {str(e)}")
                krx_only_stocks.append(item)

        # ========================================
        # 3. 결과 출력
        # ========================================
        print("\n" + "=" * 80)
        print("분류 결과")
        print("=" * 80)
        print(f"  NXT 거래 가능: {len(nxt_stocks)}개")
        print(f"  KRX만 가능:    {len(krx_only_stocks)}개")

        if nxt_stocks:
            print("\n  [NXT 거래 가능 종목]")
            for i, item in enumerate(nxt_stocks[:20], 1):
                print(f"    {i:2d}. {item.get('name', ''):15s} ({item['symbol']}) - {item.get('exchange', '')}")
            if len(nxt_stocks) > 20:
                print(f"    ... 외 {len(nxt_stocks) - 20}개")

        if krx_only_stocks:
            print("\n  [KRX만 가능 종목]")
            for i, item in enumerate(krx_only_stocks[:10], 1):
                print(f"    {i:2d}. {item.get('name', ''):15s} ({item['symbol']}) - {item.get('exchange', '')}")
            if len(krx_only_stocks) > 10:
                print(f"    ... 외 {len(krx_only_stocks) - 10}개")

        # ========================================
        # 4. XCom에 저장 (같은 DAG 내에서 사용)
        # ========================================
        context['task_instance'].xcom_push(key='nxt_stocks', value=nxt_stocks)
        context['task_instance'].xcom_push(key='krx_only_stocks', value=krx_only_stocks)
        context['task_instance'].xcom_push(key='nxt_symbols', value=[s['symbol'] for s in nxt_stocks])
        context['task_instance'].xcom_push(key='krx_only_symbols', value=[s['symbol'] for s in krx_only_stocks])

        print("\n  XCom 저장 완료:")
        print(f"    - nxt_stocks: {len(nxt_stocks)}개")
        print(f"    - krx_only_stocks: {len(krx_only_stocks)}개")

        # ========================================
        # 5. Airflow Variable에 저장 (다른 DAG에서 사용)
        # ========================================
        from airflow.models import Variable
        import json

        Variable.set("nxt_stocks", json.dumps(nxt_stocks, ensure_ascii=False))
        Variable.set("krx_only_stocks", json.dumps(krx_only_stocks, ensure_ascii=False))
        Variable.set("nxt_symbols", json.dumps([s['symbol'] for s in nxt_stocks]))
        Variable.set("krx_only_symbols", json.dumps([s['symbol'] for s in krx_only_stocks]))

        print("\n  Airflow Variable 저장 완료:")
        print(f"    - nxt_stocks: {len(nxt_stocks)}개")
        print(f"    - krx_only_stocks: {len(krx_only_stocks)}개")

        return len(nxt_stocks)
