"""
종목 메타데이터 일일 업데이트 DAG

매일 장 마감 후 (16:00 KST) 마스터 파일 다운로드 + NXT 거래 가능 여부 갱신
- daily_krx_data, daily_nxt_data 보다 먼저 실행되어야 함
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

from plugins.operators.stock_data_operators import SymbolLoaderOperator
from plugins.operators.market_data_operator import MarketOpenCheckOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_metadata_update',
    default_args=default_args,
    description='종목 메타데이터 일일 갱신 (마스터 파일 + NXT 조회)',
    schedule_interval='0 18 * * 0-4',  # 평일 03:00 KST (전일 18:00 UTC)
    start_date=timezone.datetime(2026, 2, 10),
    catchup=False,
    tags=['stock', 'daily', 'metadata'],
) as dag:

    start = EmptyOperator(task_id='start')

    check_market = MarketOpenCheckOperator(
        task_id='check_market_open',
        branch_if_open='update_metadata',
        branch_if_closed='skip',
    )

    skip = EmptyOperator(task_id='skip')

    # 마스터 파일 다운로드 + NXT 거래 가능 여부 조회 + DB 저장
    update_metadata = SymbolLoaderOperator(
        task_id='update_metadata',
        load_mode='all',
    )

    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    start >> check_market
    check_market >> skip >> end
    check_market >> update_metadata >> end
