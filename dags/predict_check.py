"""
예측 결과 업데이트 DAG

daily_stock_data DAG 완료 후 실행되어
GapPredictions 테이블의 실제 결과를 계산하여 업데이트합니다.

실행 시나리오:
1. daily_stock_data DAG 완료 대기
2. prediction_date가 실행 날짜인 예측들 조회
3. stock_prices에서 실제 종가/고가/저가 조회
4. 실제 수익률, 예측 차이, 방향 정확도 계산
5. GapPredictions 테이블 업데이트
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils import timezone

from plugins.operators.stock_data_operators import UpdatePredictionResultsOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='predict_check',
    default_args=default_args,
    description='예측 결과 업데이트 - daily_stock_data 완료 후 실행',
    schedule_interval='0 8 * * 1-5',  # 평일 오후 7시 (daily_stock_data 이후)
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['stock', 'daily', 'prediction', 'evaluation'],
) as dag:

    start = EmptyOperator(task_id='start')

    # daily_stock_data DAG 완료 대기
    wait_daily_data = ExternalTaskSensor(
        task_id='wait_daily_stock_data',
        external_dag_id='daily_stock_data',
        external_task_id='end',  # daily_stock_data의 마지막 task
        timeout=3600,  # 1시간 타임아웃
        poke_interval=60,  # 1분마다 체크
        mode='reschedule',  # 리소스 절약을 위해 reschedule 모드 사용
    )

    # 예측 결과 업데이트
    update_predictions = UpdatePredictionResultsOperator(
        task_id='update_prediction_results',
        target_date='{{ ds }}',  # execution_date 사용
    )

    end = EmptyOperator(task_id='end')

    # Task Flow
    start >> wait_daily_data >> update_predictions >> end
