"""
WebSocket KRX 시작 DAG

KRX 시장 가격 WebSocket 연결 트리거
- KRX 시장 개장: 09:00 KST
- 실행 시각: 08:50 KST (UTC 23:50)
- 전략 발행도 수행 (NXT DAG 실패 시 백업)
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
    dag_id='websocket_krx_start',
    default_args=default_args,
    description='KRX 가격 WebSocket 시작',
    schedule_interval='50 23 * * 0-4',  # 평일 오전 8시 50분 KST (UTC 23:50, KRX 개장 10분 전)
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['websocket', 'kafka', 'realtime', 'krx'],
) as dag:

    start = EmptyOperator(task_id='start')

    # KRX 전용 토큰 발급 + Kafka PRICE START 명령 발행
    websocket_krx_price_start = WebSocketStartOperator(
        task_id='websocket_krx_price_start',
        target='PRICE',
        kis_conn_id='kis_api',
        exchange_type='KRX',
    )

    # 모든 user 전략 조회 (NXT DAG 실패 시 백업)
    user_strategy_create = UserStrategyOperator(
        task_id='user_strategy_create',
        is_mock=False,
    )

    # Kafka 메시지 발행 (WebSocket 서버에서 이미 초기화된 경우 스킵됨)
    publish_account_strategy = PublishAccountStrategyOperator(
        task_id='publish_account_strategy',
        is_mock=False,
        strategy_xcom_task_id='user_strategy_create',
    )

    end = EmptyOperator(task_id='end')

    start >> websocket_krx_price_start >> user_strategy_create >> publish_account_strategy >> end
