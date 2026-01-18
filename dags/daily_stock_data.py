"""
일일 데이터 수집 DAG

매일 장 마감 후 KOSPI/KOSDAQ 전 종목 데이터 수집 및 기술지표 계산
- Dynamic Task Mapping으로 종목 수에 따라 자동 배치 생성
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
import math

from plugins.operators.stock_data_operators import (
    MarketOpenCheckOperator,
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
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='daily_stock_data',
    default_args=default_args,
    description='일일 주식 데이터 수집 (KOSPI/KOSDAQ) - 동적 배치 처리',
    schedule_interval='0 8 * * 1-5',  # 평일 오후 5시
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['stock', 'daily', 'data-collection'],
    max_active_tasks=MAX_PARALLEL_BATCHES,  # 동시 실행 Task 제한
) as dag:

    start = EmptyOperator(task_id='start')

    # 거래일 체크 (한투 API + Airflow Variable 캐싱)
    # ⚠️ 같은 날 여러 DAG에서 호출해도 1회만 API 호출
    check_market = MarketOpenCheckOperator(
        task_id='check_market_open',
        branch_if_open='load_active_symbols',
        branch_if_closed='skip_collection',
    )

    skip = EmptyOperator(task_id='skip_collection')

    # 1. DB에서 ACTIVE 종목만 조회
    load_symbols = SymbolLoaderOperator(
        task_id='load_active_symbols',
        load_mode='active'
    )

    # 2. 시장 지수 수집 (fetch → calc gaps → save)
    process_market = MarketDataOperator(
        task_id='process_market_indices',
        data_start_date='{{ ds }}',
        data_end_date='{{ ds }}'
    )

    # 3. 배치 인덱스 계산 (종목 수 기반)
    calc_batches = PythonOperator(
        task_id='calculate_batches',
        python_callable=calculate_batch_indices,
    )

    # 4. 종목 데이터 배치 처리 (Dynamic Task Mapping)
    process_stocks = StockDataOperator.partial(
        task_id='process_stock_batch',
        data_start_date='{{ ds }}',
        data_end_date='{{ ds }}',
        include_historical=True,
        historical_days=60,
        symbols_per_batch=SYMBOLS_PER_BATCH,
    ).expand(symbol_batch_index=calc_batches.output)

    # 5. 예측 데이터 채점
    # score_predictions = PythonOperator(
    #     task_id='score_predictions',
    #     python_callable=score_predictions,
    # )

    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    # Task Flow
    start >> check_market
    check_market >> skip >> end
    check_market >> load_symbols >> process_market >> calc_batches >> process_stocks >> end
