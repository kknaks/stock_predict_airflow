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
from operators.exchange_filter_operator import ExchangeFilterOperator
from operators.gap_up_filter_operator import GapUpFilterOperator
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
    dag_id='daily_krx_predict',
    default_args=default_args,
    description='포트폴리오 추출 (KOSPI/KOSDAQ) - 장전예상→갭상승→카프카',
    schedule_interval='0 0 * * 1-5',  # 평일 오전 9시 (KST, UTC+9 기준)
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['stock', 'daily', 'krx', 'predict','kafka'],
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
    # 3. 시장 지수 현재가 조회
    # ========================================
    # KOSPI, KOSDAQ, KOSPI200 현재지수 조회
    get_market_indices = IndexCurrentOperator(
        task_id='get_market_indices',
    )

    classify_krx = ExchangeFilterOperator(
        task_id='classify_krx',
    )

    # ========================================
    # 4. 갭상승 종목 필터링
    # ========================================
    # 장 시작 후 시가 > 전일종가인 종목만 필터링
    # KOSPI + KOSDAQ 두 결과를 병합하여 처리
    filter_gap_up = GapUpFilterOperator(
        task_id='filter_gap_up',
        gap_threshold=2.0,  # 0% 이상 갭상승 (시가 >= 전일종가)
        xcom_keys=['krx_stocks'],
        xcom_task_ids=['classify_krx'],
        market_code='J',
    )

    # ========================================
    # 5. 카프카 메시지 생성
    # ========================================
    def build_kafka_message(**context):
        """XCom에서 데이터를 가져와서 카프카 메시지 생성"""
        from datetime import datetime
        
        # XCom에서 데이터 가져오기
        gap_up_stocks = context['task_instance'].xcom_pull(
            key='gap_up_stocks',
            task_ids='filter_gap_up'
        ) or []
        market_indices = context['task_instance'].xcom_pull(
            key='market_indices',
            task_ids='get_market_indices'
        ) or {}

        if not gap_up_stocks:
            print("⚠️  갭상승 종목이 없습니다. 빈 메시지를 발행합니다.")
            gap_up_stocks = []

        print(f"✓ 갭상승 종목: {len(gap_up_stocks)}개")
        print(f"✓ 시장 지수: {len(market_indices)}개")

        # 시장 지수 현재가 추출
        kospi_open = market_indices.get('kospi', {}).get('current_value', 0)
        kosdaq_open = market_indices.get('kosdaq', {}).get('current_value', 0)
        kospi200_open = market_indices.get('kospi200', {}).get('current_value', 0)

        # 타임스탬프 (KST)
        kst = tz(timedelta(hours=9))
        timestamp = datetime.now(kst).isoformat()

        # TODO : strategy_id 가져오기

        # 메시지 생성 (개별 종목 정보)
        stock_messages = []
        for stock in gap_up_stocks:
            stock_msg = {
                "strategy_id": 1,
                "stock_code": stock['symbol'],
                "stock_name": stock.get('name', ''),
                "exchange": stock.get('exchange', 'UNKNOWN'),
                "stock_open": float(stock.get('open_price', 0)),
                "gap_rate": float(stock.get('gap_rate', 0)),
                "expected_change_rate": float(stock.get('expected_change_rate', 0)),
                "volume": int(stock.get('volume', 0)),
            }
            stock_messages.append(stock_msg)

        # 배치 메시지 생성 (모든 종목을 하나의 메시지로 묶음)
        batch_message = {
            "timestamp": timestamp,
            "exchange_type": "KRX",
            "kospi_open": float(kospi_open),
            "kosdaq_open": float(kosdaq_open),
            "kospi200_open": float(kospi200_open),
            "stocks": stock_messages,
            "total_count": len(stock_messages)
        }

        # JSON 문자열로 변환하여 XCom에 저장 (Jinja 템플릿으로 전달 시 문자열 변환 방지)
        import json
        batch_message_json = json.dumps(batch_message, ensure_ascii=False, default=str)
        context['task_instance'].xcom_push(key='kafka_message', value=batch_message_json)
        print(f"✓ 카프카 메시지 생성 완료: {len(stock_messages)}개 종목 (JSON 형식)")
        
        return batch_message

    build_message = PythonOperator(
        task_id='build_kafka_message',
        python_callable=build_kafka_message,
    )

    # ========================================
    # 6. 카프카 메시지 발행 (임시 비활성화 - 로그만 출력)
    # ========================================
    def log_kafka_message(**context):
        """카프카 발행 대신 메시지 내용을 로그로 출력"""
        import json

        message_json = context['task_instance'].xcom_pull(
            key='kafka_message', task_ids='build_kafka_message'
        )
        if not message_json:
            print("발행할 메시지가 없습니다.")
            return

        message = json.loads(message_json)
        print(f"[카프카 메시지 미리보기] topic: extract_daily_candidate")
        print(f"총 종목 수: {message.get('total_count', 0)}")
        print(json.dumps(message, ensure_ascii=False, indent=2))

    # publish_kafka = PythonOperator(
    #     task_id='publish_kafka',
    #     python_callable=log_kafka_message,
    # )

    publish_kafka = KafkaPublishOperator(
        task_id='publish_kafka',
        kafka_topic='extract_daily_candidate',
        message="{{ ti.xcom_pull(key='kafka_message', task_ids='build_kafka_message') }}",
        message_key="gap_up_{{ execution_date.strftime('%Y%m%d_%H%M%S') }}",
    )

    # ========================================
    # 스킵 및 종료
    # ========================================
    skip = EmptyOperator(task_id='skip_collection')

    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success'
    )

    # ========================================
    # Task 의존성 정의
    # ========================================
    # 메인 파이프라인
    start >> check_market

    # 거래일인 경우:
    # 1) KOSPI/KOSDAQ 예상종목 병렬 조회 (~30건씩, 총 ~60건)
    # 2) 지수 조회
    # 3) 갭상승 필터 (병합된 데이터 사용)
    # 4) 카프카 메시지 생성
    # 5) 카프카 발행
    check_market >> [get_expected_kospi, get_expected_kosdaq]
    
    [get_expected_kospi, get_expected_kosdaq] >> classify_krx >> get_market_indices >> filter_gap_up >> build_message >> publish_kafka >> end

    # 휴장일인 경우: 스킵
    check_market >> skip >> end
