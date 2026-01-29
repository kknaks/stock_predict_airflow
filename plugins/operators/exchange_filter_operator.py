"""
NXT / KRX 거래소 분류 오퍼레이터

stock_metadata의 is_nxt_tradable, is_nxt_stopped 필드를 기반으로
예상 상승 종목을 NXT / KRX로 분류하여 XCom에 저장합니다.

NXT 조건: is_nxt_tradable=True AND is_nxt_stopped=False
KRX: 그 외 전부
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy import text

from utils.common import get_db_url
from utils.db_writer import StockDataWriter


class ExchangeFilterOperator(BaseOperator):
    """
    예상 상승 종목을 NXT / KRX로 분류

    upstream의 SearchStockDataOperator가 XCom에 저장한 ranking 데이터를 가져와서
    DB의 stock_metadata를 조회하여 NXT/KRX로 분류합니다.

    Args:
        kospi_task_id: KOSPI 예상 상승 조회 task_id
        kosdaq_task_id: KOSDAQ 예상 상승 조회 task_id
        xcom_key: upstream에서 사용한 XCom key (기본: expected_상승_ranking)
        db_conn_id: Airflow DB connection ID
    """

    @apply_defaults
    def __init__(
        self,
        kospi_task_id='get_expected_kospi',
        kosdaq_task_id='get_expected_kosdaq',
        xcom_key='expected_상승_ranking',
        db_conn_id='stock_db',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.kospi_task_id = kospi_task_id
        self.kosdaq_task_id = kosdaq_task_id
        self.xcom_key = xcom_key
        self.db_conn_id = db_conn_id

    def execute(self, context):
        print("=" * 80)
        print("NXT / KRX 거래소 분류")
        print("=" * 80)

        ti = context['task_instance']

        # 1. upstream에서 KOSPI/KOSDAQ 예상 상승 종목 가져오기
        kospi_stocks = ti.xcom_pull(key=self.xcom_key, task_ids=self.kospi_task_id) or []
        kosdaq_stocks = ti.xcom_pull(key=self.xcom_key, task_ids=self.kosdaq_task_id) or []

        all_stocks = kospi_stocks + kosdaq_stocks
        print(f"KOSPI: {len(kospi_stocks)}건, KOSDAQ: {len(kosdaq_stocks)}건, 합계: {len(all_stocks)}건")

        if not all_stocks:
            print("예상 상승 종목이 없습니다.")
            ti.xcom_push(key='nxt_stocks', value=[])
            ti.xcom_push(key='krx_stocks', value=[])
            return 0

        # 2. DB에서 NXT 거래 가능 여부 조회
        symbols = [stock['symbol'] for stock in all_stocks]

        db_url = get_db_url(self.db_conn_id)
        db_writer = StockDataWriter(db_url)

        with db_writer.engine.connect() as conn:
            query = text("""
                SELECT symbol, is_nxt_tradable, is_nxt_stopped
                FROM stock_metadata
                WHERE symbol = ANY(:symbols)
            """)
            result = conn.execute(query, {"symbols": symbols})
            nxt_info = {
                row[0]: {"is_nxt_tradable": row[1], "is_nxt_stopped": row[2]}
                for row in result
            }

        # 3. NXT / KRX 분류
        nxt_stocks = []
        krx_stocks = []

        for stock in all_stocks:
            symbol = stock['symbol']
            info = nxt_info.get(symbol, {})
            is_tradable = info.get('is_nxt_tradable', False)
            is_stopped = info.get('is_nxt_stopped', True)

            if is_tradable and not is_stopped:
                stock['exchange_type'] = 'NXT'
                nxt_stocks.append(stock)
            else:
                stock['exchange_type'] = 'KRX'
                krx_stocks.append(stock)

        print(f"\n[분류 결과]")
        print(f"  NXT: {len(nxt_stocks)}건")
        print(f"  KRX: {len(krx_stocks)}건")

        # 4. XCom에 저장
        ti.xcom_push(key='nxt_stocks', value=nxt_stocks)
        ti.xcom_push(key='krx_stocks', value=krx_stocks)

        return len(all_stocks)
