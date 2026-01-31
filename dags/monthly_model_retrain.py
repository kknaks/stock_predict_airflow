"""
월말 모델 재학습 명령 발행 DAG

매월 마지막 날 자정(KST)에 model_retrain_command 토픽으로 재학습 신호를 발행합니다.
Retraining 서버에서 수신 후 재학습을 수행합니다.

수동 트리거 시 Trigger w/ config에서 params를 오버라이드할 수 있습니다.
"""

import json
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone as af_timezone

from operators.kafka_operator import KafkaPublishOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='monthly_model_retrain',
    default_args=default_args,
    description='월말 AI 모델 재학습 명령 발행 (model_retrain_command)',
    schedule_interval='0 15 L * *',  # 매월 마지막 날 15:00 UTC = KST 자정
    start_date=af_timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['model', 'retrain', 'kafka', 'monthly'],
    params={
        "trigger_type": "scheduled",
        "data_start_date": None,
        "data_end_date": None,
        "threshold": 0.4,
        "n_estimators": 300,
        "test_size": 0.1,
        "valid_size": 0.2,
        "test_recent": True,
        "min_roc_auc": 0.55,
        "max_roc_auc_drop": 0.02,
        "max_f1_drop": 0.05,
        "min_backtest_return": 0.0,
        "min_sharpe_ratio": 1.0,
        "min_profit_factor": 1.0,
    },
) as dag:

    start = EmptyOperator(task_id='start')

    def build_retrain_message(**context):
        """재학습 명령 메시지 생성 - params에서 config 읽기"""
        params = context['params']
        kst = timezone(timedelta(hours=9))
        timestamp = datetime.now(kst).isoformat()

        message = {
            "timestamp": timestamp,
            "trigger_type": params.get("trigger_type", "scheduled"),
            "config": {
                "data_start_date": params.get("data_start_date"),
                "data_end_date": params.get("data_end_date"),
                "threshold": params.get("threshold", 0.4),
                "n_estimators": params.get("n_estimators", 300),
                "test_size": params.get("test_size", 0.1),
                "valid_size": params.get("valid_size", 0.2),
                "test_recent": params.get("test_recent", True),
                "min_roc_auc": params.get("min_roc_auc", 0.55),
                "max_roc_auc_drop": params.get("max_roc_auc_drop", 0.02),
                "max_f1_drop": params.get("max_f1_drop", 0.05),
                "min_backtest_return": params.get("min_backtest_return", 0.0),
                "min_sharpe_ratio": params.get("min_sharpe_ratio", 1.0),
                "min_profit_factor": params.get("min_profit_factor", 1.0),
            },
        }

        message_json = json.dumps(message, ensure_ascii=False, default=str)
        context['task_instance'].xcom_push(key='retrain_message', value=message_json)
        print(f"Retrain message built: {message_json}")
        return message

    build_message = PythonOperator(
        task_id='build_retrain_message',
        python_callable=build_retrain_message,
    )

    publish_kafka = KafkaPublishOperator(
        task_id='publish_retrain_command',
        kafka_topic='model_retrain_command',
        message="{{ ti.xcom_pull(key='retrain_message', task_ids='build_retrain_message') }}",
        message_key="retrain_{{ execution_date.strftime('%Y%m%d_%H%M%S') }}",
    )

    end = EmptyOperator(task_id='end')

    start >> build_message >> publish_kafka >> end
