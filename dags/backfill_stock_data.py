"""
과거 데이터 백필 DAG

KOSPI/KOSDAQ 전 종목의 과거 OHLCV 데이터 수집 및 기술지표 계산
- Dynamic Task Mapping으로 종목 수에 따라 자동 배치 생성
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.models.param import Param
import math

from plugins.operators.stock_data_operators import (
    SymbolLoaderOperator,
    MarketDataOperator,
    StockDataOperator
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
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=30),  # 배치당 30분
}

with DAG(
    dag_id='backfill_stock_data',
    default_args=default_args,
    description='과거 주식 데이터 백필 (KOSPI/KOSDAQ) - 동적 배치 처리',
    schedule=None,  # Manual trigger only
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['stock', 'backfill', 'data-collection'],
    max_active_runs=1,
    max_active_tasks=MAX_PARALLEL_BATCHES,  # 동시 실행 Task 제한
    params={
        "from_date": Param(
            default="2025-11-01",
            type="string",
            description="백필 시작일 (YYYY-MM-DD)",
        ),
        "to_date": Param(
            type="string",
            description="백필 종료일 (YYYY-MM-DD) - 필수 입력",
        ),
    },
) as dag:

    start = EmptyOperator(task_id='start')

    # 1. KIS 마스터 파일에서 전체 종목 조회 + DB 저장
    load_symbols = SymbolLoaderOperator(
        task_id='load_symbol_list',
        load_mode='all'
    )

    # 2. 시장 지수 수집 (fetch → calc gaps → save)
    process_market = MarketDataOperator(
        task_id='process_market_indices',
        data_start_date='{{ params.from_date }}',
        data_end_date='{{ params.to_date }}'
    )

    # 3. 배치 인덱스 계산 (종목 수 기반)
    calc_batches = PythonOperator(
        task_id='calculate_batches',
        python_callable=calculate_batch_indices,
    )

    # 4. 종목 데이터 배치 처리 (Dynamic Task Mapping)
    process_batches = StockDataOperator.partial(
        task_id='process_stock_batch',
        data_start_date='{{ params.from_date }}',
        data_end_date='{{ params.to_date }}',
        include_historical=False,
        symbols_per_batch=SYMBOLS_PER_BATCH,
    ).expand(symbol_batch_index=calc_batches.output)

    end = EmptyOperator(task_id='end')

    # Task Flow
    start >> load_symbols >> process_market >> calc_batches >> process_batches >> end
