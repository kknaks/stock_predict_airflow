"""
WebSocket Closing DAG

장 마감 시 주문 마감 처리 (CLOSING 명령 발행)
- 미체결 매수 주문 취소
- 미매도 종목 시장가 매도
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
    dag_id='websocket_closing',
    default_args=default_args,
    description='장 마감 시 주문 마감 처리 (CLOSING 명령)',
    schedule_interval='20 6 * * 1-5',  # 평일 오후 7시 50분 KST (UTC 10:50, 장 마감 10분 전)
    start_date=airflow_timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['websocket', 'kafka', 'closing'],
) as dag:

    start = EmptyOperator(task_id='start')

    # CLOSING 메시지 발행 (유저 정보 불필요)
    # 모든 전략에 대해 자동으로 주문 마감 처리됨
    publish_closing = KafkaPublishOperator(
        task_id='publish_closing',
        kafka_topic='kis_websocket_commands',
        message={
            "command": "CLOSING",
            "timestamp": "{{ ds }}T19:50:00+09:00",  # KST 장 마감 10분 전
            "target": "ALL",  # 모든 전략 대상
        },
        message_key="closing_{{ ds }}_{{ ts_nodash }}",
    )

    end = EmptyOperator(task_id='end')

    start >> publish_closing >> end
