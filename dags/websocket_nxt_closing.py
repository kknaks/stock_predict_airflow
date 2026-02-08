"""
WebSocket NXT Closing DAG

NXT 장 마감 시 주문 마감 처리 (CLOSING 명령 발행)
- 미체결 매수 주문 취소
- 미매도 종목 시장가 매도
- 실행 시각: 19:50 KST (UTC 10:50)
"""

from datetime import timedelta, timezone
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone as airflow_timezone

from operators.kafka_operator import KafkaPublishOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='websocket_nxt_closing',
    default_args=default_args,
    description='NXT 장 마감 시 주문 마감 처리 (CLOSING 명령)',
    schedule_interval='50 10 * * 1-5',  # 평일 오후 7시 50분 KST (UTC 10:50)
    start_date=airflow_timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['websocket', 'kafka', 'closing', 'nxt'],
) as dag:

    start = EmptyOperator(task_id='start')

    publish_closing = KafkaPublishOperator(
        task_id='publish_closing',
        kafka_topic='kis_websocket_commands',
        message={
            "command": "CLOSING",
            "timestamp": "{{ ds }}T19:50:00+09:00",
            "target": "ALL",
            "exchange_type": "NXT",
        },
        message_key="closing_nxt_{{ ds }}_{{ ts_nodash }}",
    )

    end = EmptyOperator(task_id='end')

    start >> publish_closing >> end
