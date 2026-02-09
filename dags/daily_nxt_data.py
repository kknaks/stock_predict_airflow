"""
NXT 일일 데이터 수집 DAG

매일 장 마감 후 (20:01 KST) NXT 거래 가능 종목 OHLCV 수집
- stock_metadata의 is_nxt_tradable 기준으로 NXT 가능 종목만 수집
- Dynamic Task Mapping으로 종목 수에 따라 자동 배치 생성
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
import math

from plugins.operators.stock_data_operators import (
    SymbolLoaderOperator,
    StockDataOperator
)
from plugins.operators.market_data_operator import (
    MarketOpenCheckOperator,
)


# 배치 설정
SYMBOLS_PER_BATCH = 100  # 배치당 종목 수
MAX_PARALLEL_BATCHES = 4 # 동시 실행 배치 수


def calculate_batch_indices(**context):
    """종목 수에 따라 배치 인덱스 리스트 생성"""
    symbol_list = context['task_instance'].xcom_pull(key='symbol_list')
    total_symbols = len(symbol_list) if symbol_list else 0
    total_batches = math.ceil(total_symbols / SYMBOLS_PER_BATCH)
    batch_indices = list(range(total_batches))
    print(f"총 {total_symbols}개 종목 → {total_batches}개 배치 생성")
    return batch_indices


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='daily_nxt_data',
    default_args=default_args,
    description='NXT 일일 주식 데이터 수집 (NXT 거래 가능 종목) - 동적 배치 처리',
    schedule_interval='1 11 * * 1-5',  # 평일 20:01 KST (11:01 UTC)
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['stock', 'daily', 'nxt'],
    max_active_tasks=MAX_PARALLEL_BATCHES,
) as dag:

    start = EmptyOperator(task_id='start')

    check_market = MarketOpenCheckOperator(
        task_id='check_market_open',
        branch_if_open='load_active_symbols',
        branch_if_closed='skip_collection',
    )

    skip = EmptyOperator(task_id='skip_collection')

    # NXT 거래 가능 종목만 로드
    load_symbols = SymbolLoaderOperator(
        task_id='load_active_symbols',
        load_mode='active',
        nxt_filter='nxt_only',
    )

    calc_batches = PythonOperator(
        task_id='calculate_batches',
        python_callable=calculate_batch_indices,
    )

    process_stocks = StockDataOperator.partial(
        task_id='process_stock_batch',
        data_start_date='{{ data_interval_end | ds }}',
        data_end_date='{{ data_interval_end | ds }}',
        symbols_per_batch=SYMBOLS_PER_BATCH,
        market_code='UN',
    ).expand(symbol_batch_index=calc_batches.output)

    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    start >> check_market
    check_market >> skip >> end
    check_market >> load_symbols >> calc_batches >> process_stocks >> end
