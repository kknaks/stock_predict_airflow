"""
NXT 갭상승 예측 DAG

08:00 실행 - NXT 거래 종목 대상 갭상승 예측
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils import timezone

from operators.market_data_operator import MarketOpenCheckOperator
from operators.gap_up_filter_operator import GapUpFilterOperator
from operators.kafka_operator import KafkaPublishOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


def get_nxt_stocks_from_variable(**context):
    """
    Airflow Variable에서 NXT 종목 리스트 가져오기
    (daily_stock_settings DAG에서 저장한 데이터)
    """
    from airflow.models import Variable
    import json

    print("=" * 80)
    print("NXT 종목 리스트 조회 (from Variable)")
    print("=" * 80)

    try:
        nxt_stocks_json = Variable.get("nxt_stocks", default_var=None)

        if nxt_stocks_json:
            nxt_stocks = json.loads(nxt_stocks_json)
            print(f"  NXT 종목 수: {len(nxt_stocks)}개")

            for i, stock in enumerate(nxt_stocks[:10], 1):
                print(f"    {i:2d}. {stock.get('name', ''):15s} ({stock.get('symbol')}) "
                      f"| 예상등락률: {stock.get('expected_change_rate', 0):+.2f}%")

            if len(nxt_stocks) > 10:
                print(f"    ... 외 {len(nxt_stocks) - 10}개")

            # XCom에 저장 (후속 Task에서 사용)
            context['task_instance'].xcom_push(key='nxt_stocks', value=nxt_stocks)
            context['task_instance'].xcom_push(key='nxt_symbols', value=[s['symbol'] for s in nxt_stocks])

            return len(nxt_stocks)
        else:
            print("  Variable 'nxt_stocks'가 없습니다.")
            print("  daily_stock_settings DAG를 먼저 실행해주세요.")
            return 0

    except Exception as e:
        print(f"  에러: {str(e)}")
        return 0


def check_nxt_stocks_exist(**context):
    """
    NXT 종목이 있는지 확인하여 분기
    """
    from airflow.models import Variable
    import json

    try:
        nxt_stocks_json = Variable.get("nxt_stocks", default_var=None)
        if nxt_stocks_json:
            nxt_stocks = json.loads(nxt_stocks_json)
            if len(nxt_stocks) > 0:
                return 'get_nxt_stocks'
    except:
        pass

    return 'skip_no_nxt_stocks'


with DAG(
    dag_id='nxt_predict',
    default_args=default_args,
    description='NXT 갭상승 예측 (08:00 실행)',
    schedule_interval='0 23 * * 0-4',  # UTC 23:00 = KST 08:00, 월~금
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['stock', 'nxt', 'predict', 'gap-up'],
) as dag:

    # ========================================
    # 1. 시작
    # ========================================
    start = EmptyOperator(task_id='start')

    # NXT 종목 존재 여부 확인
    check_nxt = BranchPythonOperator(
        task_id='check_nxt_stocks_exist',
        python_callable=check_nxt_stocks_exist,
    )

    # ========================================
    # 2. NXT 종목 리스트 조회 (Variable에서)
    # ========================================
    get_nxt_stocks = PythonOperator(
        task_id='get_nxt_stocks',
        python_callable=get_nxt_stocks_from_variable,
    )

    # ========================================
    # 3. NXT 종목 갭상승 필터링
    # ========================================
    # Note: 08:00에는 시장 지수가 없음 (정규장 09:00 시작)
    # market_code: UN(통합) 사용하여 NXT 시세 조회
    filter_gap_up = GapUpFilterOperator(
        task_id='filter_nxt_gap_up',
        gap_threshold=0.0,  # 0% 이상 갭상승
        xcom_keys=['nxt_stocks'],
        xcom_task_ids=['get_nxt_stocks'],
        market_code='UN',  # 통합 조회 (NXT + KRX)
    )

    # ========================================
    # 4. Kafka 메시지 생성
    # ========================================
    def build_kafka_message(**context):
        """XCom에서 데이터를 가져와서 카프카 메시지 생성"""
        from datetime import datetime, timezone as tz, timedelta
        import json

        # XCom에서 갭상승 종목 가져오기
        gap_up_stocks = context['task_instance'].xcom_pull(
            key='gap_up_stocks',
            task_ids='filter_nxt_gap_up'
        ) or []

        print("=" * 80)
        print("NXT 갭상승 종목 → Kafka 메시지 생성")
        print("=" * 80)

        if not gap_up_stocks:
            print("  갭상승 종목 없음 - 발행 스킵")
            return None

        print(f"  갭상승 종목: {len(gap_up_stocks)}개")

        # 타임스탬프 (KST)
        kst = tz(timedelta(hours=9))
        timestamp = datetime.now(kst).isoformat()

        # 메시지 생성 (개별 종목 정보)
        stock_messages = []
        for i, stock in enumerate(gap_up_stocks, 1):
            stock_msg = {
                "strategy_id": 1,  # TODO: 실제 strategy_id
                "stock_code": stock['symbol'],
                "stock_name": stock.get('name', ''),
                "exchange": stock.get('exchange', 'UNKNOWN'),
                "stock_open": float(stock.get('open_price', 0)),
                "gap_rate": float(stock.get('gap_rate', 0)),
                "expected_change_rate": float(stock.get('expected_change_rate', 0)),
                "volume": int(stock.get('volume', 0)),
                "market_type": "NXT",  # NXT 종목 표시
            }
            stock_messages.append(stock_msg)

            # 상위 20개만 로그 출력
            if i <= 20:
                print(f"    {i:2d}. {stock.get('name', ''):15s} ({stock['symbol']}) "
                      f"| 갭: {stock.get('gap_rate', 0):+.2f}% "
                      f"| 시가: {stock.get('open_price', 0):,}원")

        if len(gap_up_stocks) > 20:
            print(f"    ... 외 {len(gap_up_stocks) - 20}개")

        # 배치 메시지 생성
        # Note: 08:00에는 시장 지수가 없으므로 0으로 설정
        batch_message = {
            "timestamp": timestamp,
            "market_type": "NXT",
            "prediction_phase": "preliminary",  # 1차 예측 (시장 지표 없음)
            "kospi_open": 0.0,   # 08:00에는 시장 지수 없음
            "kosdaq_open": 0.0,
            "kospi200_open": 0.0,
            "stocks": stock_messages,
            "total_count": len(stock_messages)
        }

        # JSON 문자열로 변환하여 XCom에 저장
        batch_message_json = json.dumps(batch_message, ensure_ascii=False, default=str)
        context['task_instance'].xcom_push(key='kafka_message', value=batch_message_json)

        # Variable에 NXT 갭상승 결과 저장 (09:00 krx_predict에서 재사용)
        from airflow.models import Variable
        Variable.set("nxt_gap_up_stocks", json.dumps(gap_up_stocks, ensure_ascii=False, default=str))
        print(f"\n  Variable 'nxt_gap_up_stocks' 저장: {len(gap_up_stocks)}개 종목")

        print(f"  Kafka 메시지 생성 완료: {len(stock_messages)}개 종목")
        return batch_message

    build_message = PythonOperator(
        task_id='build_kafka_message',
        python_callable=build_kafka_message,
    )

    # ========================================
    # 5. Kafka 메시지 로그 출력 (실제 발행은 테스트 후)
    # ========================================
    def log_kafka_message(**context):
        """Kafka 메시지 전체 로그 출력 (발행 전 확인용)"""
        import json

        kafka_message = context['task_instance'].xcom_pull(
            key='kafka_message',
            task_ids='build_kafka_message'
        )

        print("=" * 80)
        print("Kafka 메시지 (발행 예정) - Preliminary Prediction")
        print("=" * 80)

        if kafka_message:
            # JSON 파싱해서 보기 좋게 출력
            msg_dict = json.loads(kafka_message)
            print(f"  Topic: extract_daily_candidate")
            print(f"  Timestamp: {msg_dict.get('timestamp')}")
            print(f"  Market Type: {msg_dict.get('market_type')}")
            print(f"  Prediction Phase: {msg_dict.get('prediction_phase')}")
            print(f"  Total Count: {msg_dict.get('total_count')}")
            print(f"\n  [시장 지수]")
            print(f"    KOSPI:    {msg_dict.get('kospi_open', 0)}")
            print(f"    KOSDAQ:   {msg_dict.get('kosdaq_open', 0)}")
            print(f"    KOSPI200: {msg_dict.get('kospi200_open', 0)}")
            print(f"\n  [종목 리스트]")
            for i, stock in enumerate(msg_dict.get('stocks', []), 1):
                print(f"    {i:2d}. {stock.get('stock_name', ''):15s} ({stock.get('stock_code')}) "
                      f"| 갭: {stock.get('gap_rate', 0):+.2f}% "
                      f"| 시가: {stock.get('stock_open', 0):,.0f}원 "
                      f"| 예상등락률: {stock.get('expected_change_rate', 0):+.2f}%")
            print("\n" + "=" * 80)
            print("Raw JSON Message:")
            print("=" * 80)
            print(json.dumps(msg_dict, indent=2, ensure_ascii=False))
        else:
            print("  메시지 없음")

        return kafka_message

    log_kafka = PythonOperator(
        task_id='log_kafka_message',
        python_callable=log_kafka_message,
    )

    # TODO: 테스트 완료 후 실제 발행 활성화
    # publish_kafka = KafkaPublishOperator(
    #     task_id='publish_kafka',
    #     kafka_topic='extract_daily_candidate',
    #     message="{{ ti.xcom_pull(key='kafka_message', task_ids='build_kafka_message') }}",
    #     message_key="nxt_gap_up_{{ execution_date.strftime('%Y%m%d_%H%M%S') }}",
    # )

    # ========================================
    # Skip 처리
    # ========================================
    skip_no_nxt = EmptyOperator(task_id='skip_no_nxt_stocks')

    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    # ========================================
    # Task Dependencies
    # ========================================
    start >> check_nxt

    # NXT 종목 있음: 갭상승 필터링 → Kafka 메시지 생성 → 로그 출력
    check_nxt >> get_nxt_stocks >> filter_gap_up >> build_message >> log_kafka >> end

    # NXT 종목 없음: 스킵
    check_nxt >> skip_no_nxt >> end
