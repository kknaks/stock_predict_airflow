"""
WebSocket 시작 DAG

가격 WebSocket 연결 트리거
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

from operators.stock_data_operators import WebSocketStartOperator
from operators.user_strategy_operators import UserStrategyOperator
from operators.kafka_operator import PublishAccountStrategyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='websocket_start',
    default_args=default_args,
    description='가격 WebSocket 연결 시작',
    schedule_interval='30 23 * * 0-4',  # 평일 오전 8시 30분 KST (UTC 23:30 전날, UTC+9 기준)
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['websocket', 'kafka', 'realtime'],
) as dag:

    start = EmptyOperator(task_id='start')

    # 토큰 발급 + Kafka START 명령 발행
    websocket_start = WebSocketStartOperator(
        task_id='websocket_start',
        target='PRICE',
    )

    # 모든 user 전략 조회 (role에 따라 자동 구분)
    user_strategy_create = UserStrategyOperator(
        task_id='user_strategy_create',
        is_mock=False,  # 모든 user 조회 (role에 따라 토큰 발급 여부 결정)
    )

    # Kafka 메시지 발행 (role에 따라 env_dv, is_mock 자동 설정)
    publish_account_strategy = PublishAccountStrategyOperator(
        task_id='publish_account_strategy',
        is_mock=False,  # 모든 user 처리 (role에 따라 자동 구분)
        strategy_xcom_task_id='user_strategy_create',
    )

    end = EmptyOperator(task_id='end')

    start >> websocket_start >> user_strategy_create >> publish_account_strategy >> end
