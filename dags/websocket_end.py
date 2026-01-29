"""
WebSocket End DAG

장 종료 후 WebSocket 연결 해제 (STOP 명령 발행)
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
    dag_id='websocket_end',
    default_args=default_args,
    description='장 종료 (STOP 명령)',
    schedule_interval='01 11 * * 1-5',  # 평일 오후 8시 01분 KST (UTC 11:01, 장 마감 직후)
    start_date=airflow_timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['websocket', 'kafka', 'stop'],
) as dag:

    start = EmptyOperator(task_id='start')

    # STOP 메시지 발행 (WebSocket 연결 종료)
    publish_closing = KafkaPublishOperator(
        task_id='publish_stop',
        kafka_topic='kis_websocket_commands',
        message={
            "command": "STOP",
            "timestamp": "{{ ds }}T20:00:00+09:00",  # KST 장 마감 시각
            "target": "ALL",  # 모든 전략 대상
        },
        message_key="closing_{{ ds }}_{{ ts_nodash }}",
    )

    end = EmptyOperator(task_id='end')

    start >> publish_closing >> end
