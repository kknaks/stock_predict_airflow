"""
KRX 일일 데이터 수집 DAG

매일 장 마감 후 (15:31 KST) KRX 전용 종목 OHLCV 수집
- stock_metadata의 is_nxt_tradable 기준으로 KRX 전용 종목만 수집
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
    MarketDataOperator
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
    dag_id='daily_krx_data',
    default_args=default_args,
    description='KRX 일일 주식 데이터 수집 (KRX 전용 종목) - 동적 배치 처리',
    schedule_interval='31 6 * * 1-5',  # 평일 15:31 KST (06:31 UTC)
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['stock', 'daily', 'krx'],
    max_active_tasks=MAX_PARALLEL_BATCHES,
) as dag:

    start = EmptyOperator(task_id='start')

    check_market = MarketOpenCheckOperator(
        task_id='check_market_open',
        branch_if_open=['load_active_symbols', 'process_market_indices'],
        branch_if_closed='skip_collection',
    )

    skip = EmptyOperator(task_id='skip_collection')

    # KRX 전용 종목만 로드 (NXT 불가 종목)
    load_symbols = SymbolLoaderOperator(
        task_id='load_active_symbols',
        load_mode='active',
        nxt_filter='krx_only',
    )

    # 시장 지수 수집 (종목 수집과 병렬 실행)
    process_market = MarketDataOperator(
        task_id='process_market_indices',
        data_start_date='{{ data_interval_end | ds }}',
        data_end_date='{{ data_interval_end | ds }}'
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
        market_code='J',
    ).expand(symbol_batch_index=calc_batches.output)

    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    start >> check_market
    check_market >> skip >> end
    check_market >> load_symbols >> calc_batches >> process_stocks >> end
    check_market >> process_market >> end
