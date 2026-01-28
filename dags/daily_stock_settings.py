from datetime import timedelta, timezone as tz
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from operators.stock_data_operators import SearchStockDataOperator
from operators.market_data_operator import (
    MarketOpenCheckOperator,
    IndexCurrentOperator,
)
from operators.gap_up_filter_operator import GapUpFilterOperator
from operators.nxt_filter_operator import NxtFilterOperator
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
    dag_id='daily_stock_settings',
    default_args=default_args,
    description='포트폴리오 추출 (KOSPI/KOSDAQ) - 예상 상승 종목 + NXT/KRX 분류',
    schedule_interval='50 22 * * 0-4',  # UTC 22:50 = KST 07:50, 월~금
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['stock', 'daily', 'settings', 'kafka'],
) as dag:

    # ========================================
    # 1. 시작 및 거래일 체크
    # ========================================
    start = EmptyOperator(task_id='start')

    # 한투 API로 휴장일 체크 (주말 + 공휴일)
    # ⚠️ 이 API는 1일 1회 호출 권장
    check_market = MarketOpenCheckOperator(
        task_id='check_market_open',
        branch_if_open=['get_expected_kospi', 'get_expected_kosdaq'],
        branch_if_closed='skip_collection',
    )

    # ========================================
    # 2. 장전 예상 상승 종목 조회 (KOSPI + KOSDAQ 병렬)
    # ========================================
    # Note: API가 한 번에 ~30건만 반환하므로, KOSPI/KOSDAQ 각각 조회하여 합침
    
    # KOSPI 예상 상승 종목 (~30건)
    get_expected_kospi = SearchStockDataOperator(
        task_id='get_expected_kospi',
        market='J',
        fid_input="0001",  # 0001: 거래소(KOSPI)
        sort_cls='1',      # 0: 상승률 순
        mkop_cls='0',      # 0: 장전 예상
    )

    # KOSDAQ 예상 상승 종목 (~30건)
    get_expected_kosdaq = SearchStockDataOperator(
        task_id='get_expected_kosdaq',
        market='J',
        fid_input="1001",  # 1001: 코스닥
        sort_cls='1',      # 0: 상승률 순
        mkop_cls='0',      # 0: 장전 예상
    )

    # ========================================
    # 3. NXT / KRX 분류
    # ========================================
    classify_nxt = NxtFilterOperator(
        task_id='classify_nxt_krx',
        xcom_keys=['expected_상승_ranking', 'expected_상승_ranking'],
        xcom_task_ids=['get_expected_kospi', 'get_expected_kosdaq'],
    )

    # 휴장일이면 스킵
    skip_collection = EmptyOperator(task_id='skip_collection')

    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    # ========================================
    # Task Dependencies
    # ========================================
    start >> check_market

    # 거래일: 예상 상승 종목 조회 → NXT 분류
    check_market >> [get_expected_kospi, get_expected_kosdaq]
    [get_expected_kospi, get_expected_kosdaq] >> classify_nxt >> end

    # 휴장일: 스킵
    check_market >> skip_collection >> end
