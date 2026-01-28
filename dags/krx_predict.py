"""
KRX 갭상승 예측 DAG

09:00 실행 - KRX 정규장 시작
- NXT 갭상승 결과 재사용 (08:00에 저장된 Variable)
- KRX만 가능 종목 갭상승 필터링
- 시장 지표 포함하여 전체 재예측 요청
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils import timezone

from operators.market_data_operator import (
    MarketOpenCheckOperator,
    IndexCurrentOperator,
)
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


def get_krx_stocks_from_variable(**context):
    """
    Airflow Variable에서 KRX만 가능 종목 리스트 가져오기
    (daily_stock_settings DAG에서 저장한 데이터)
    """
    from airflow.models import Variable
    import json

    print("=" * 80)
    print("KRX 종목 리스트 조회 (from Variable)")
    print("=" * 80)

    try:
        krx_stocks_json = Variable.get("krx_only_stocks", default_var=None)

        if krx_stocks_json:
            krx_stocks = json.loads(krx_stocks_json)
            print(f"  KRX만 가능 종목 수: {len(krx_stocks)}개")

            for i, stock in enumerate(krx_stocks[:10], 1):
                print(f"    {i:2d}. {stock.get('name', ''):15s} ({stock.get('symbol')}) "
                      f"| 예상등락률: {stock.get('expected_change_rate', 0):+.2f}%")

            if len(krx_stocks) > 10:
                print(f"    ... 외 {len(krx_stocks) - 10}개")

            # XCom에 저장 (후속 Task에서 사용)
            context['task_instance'].xcom_push(key='krx_stocks', value=krx_stocks)

            return len(krx_stocks)
        else:
            print("  Variable 'krx_only_stocks'가 없습니다.")
            print("  daily_stock_settings DAG를 먼저 실행해주세요.")
            context['task_instance'].xcom_push(key='krx_stocks', value=[])
            return 0

    except Exception as e:
        print(f"  에러: {str(e)}")
        context['task_instance'].xcom_push(key='krx_stocks', value=[])
        return 0


def check_krx_stocks_exist(**context):
    """
    KRX 종목 또는 NXT 결과가 있는지 확인하여 분기
    """
    from airflow.models import Variable
    import json

    try:
        # KRX 종목 확인
        krx_stocks_json = Variable.get("krx_only_stocks", default_var=None)
        krx_count = len(json.loads(krx_stocks_json)) if krx_stocks_json else 0

        # NXT 갭상승 결과 확인
        nxt_gap_json = Variable.get("nxt_gap_up_stocks", default_var=None)
        nxt_count = len(json.loads(nxt_gap_json)) if nxt_gap_json else 0

        print(f"  KRX 종목: {krx_count}개, NXT 갭상승: {nxt_count}개")

        if krx_count > 0 or nxt_count > 0:
            return 'get_krx_stocks'
    except:
        pass

    return 'skip_no_stocks'


with DAG(
    dag_id='krx_predict',
    default_args=default_args,
    description='KRX 갭상승 예측 (09:00 실행) - NXT 재사용 + KRX 필터링 + 시장지표',
    schedule_interval='1 0 * * 1-5',  # UTC 00:01 = KST 09:01, 월~금
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=['stock', 'krx', 'predict', 'gap-up'],
) as dag:

    # ========================================
    # 1. 시작
    # ========================================
    start = EmptyOperator(task_id='start')

    # 종목 존재 여부 확인
    check_stocks = BranchPythonOperator(
        task_id='check_stocks_exist',
        python_callable=check_krx_stocks_exist,
    )

    # ========================================
    # 2. KRX 종목 리스트 조회 (Variable에서)
    # ========================================
    get_krx_stocks = PythonOperator(
        task_id='get_krx_stocks',
        python_callable=get_krx_stocks_from_variable,
    )

    # ========================================
    # 3. 시장 지수 현재가 조회
    # ========================================
    get_market_index = IndexCurrentOperator(
        task_id='get_market_index',
    )

    # ========================================
    # 4. KRX 종목만 갭상승 필터링
    # ========================================
    # Note: NXT 종목은 08:00에 이미 필터링 완료 (Variable 재사용)
    filter_krx_gap_up = GapUpFilterOperator(
        task_id='filter_krx_gap_up',
        gap_threshold=0.0,  # 0% 이상 갭상승
        xcom_keys=['krx_stocks'],
        xcom_task_ids=['get_krx_stocks'],
        market_code='J',  # KRX 정규장
    )

    # ========================================
    # 5. NXT + KRX 합쳐서 Kafka 메시지 생성
    # ========================================
    def build_kafka_message(**context):
        """NXT 결과 + KRX 결과 합쳐서 Kafka 메시지 생성 (시장 지표 포함)"""
        from datetime import datetime, timezone as tz, timedelta
        from airflow.models import Variable
        import json

        print("=" * 80)
        print("NXT + KRX 갭상승 종목 → Kafka 메시지 생성 (Final)")
        print("=" * 80)

        # 1. NXT 갭상승 결과 가져오기 (08:00에 저장된 Variable)
        nxt_gap_up_stocks = []
        try:
            nxt_gap_json = Variable.get("nxt_gap_up_stocks", default_var=None)
            if nxt_gap_json:
                nxt_gap_up_stocks = json.loads(nxt_gap_json)
                print(f"  NXT 갭상승 (Variable에서): {len(nxt_gap_up_stocks)}개")
        except Exception as e:
            print(f"  NXT 결과 로드 실패: {e}")

        # 2. KRX 갭상승 결과 가져오기 (방금 필터링한 XCom)
        krx_gap_up_stocks = context['task_instance'].xcom_pull(
            key='gap_up_stocks',
            task_ids='filter_krx_gap_up'
        ) or []
        print(f"  KRX 갭상승 (XCom에서): {len(krx_gap_up_stocks)}개")

        # 3. 시장 지표 가져오기
        market_indices = context['task_instance'].xcom_pull(
            key='market_indices',
            task_ids='get_market_index'
        ) or {}

        kospi_value = market_indices.get('kospi', {}).get('current_value', 0)
        kosdaq_value = market_indices.get('kosdaq', {}).get('current_value', 0)
        kospi200_value = market_indices.get('kospi200', {}).get('current_value', 0)

        print(f"  시장 지표: KOSPI={kospi_value:.2f}, KOSDAQ={kosdaq_value:.2f}, KOSPI200={kospi200_value:.2f}")

        # 4. NXT + KRX 합치기
        all_gap_up_stocks = nxt_gap_up_stocks + krx_gap_up_stocks

        if not all_gap_up_stocks:
            print("  갭상승 종목 없음 - 발행 스킵")
            return None

        print(f"\n  총 갭상승 종목: {len(all_gap_up_stocks)}개 (NXT: {len(nxt_gap_up_stocks)} + KRX: {len(krx_gap_up_stocks)})")

        # NXT 종목 symbol 집합 (market_type 구분용)
        nxt_symbols = {stock['symbol'] for stock in nxt_gap_up_stocks}

        # 타임스탬프 (KST)
        kst = tz(timedelta(hours=9))
        timestamp = datetime.now(kst).isoformat()

        # 메시지 생성
        stock_messages = []
        for i, stock in enumerate(all_gap_up_stocks, 1):
            # NXT vs KRX 구분 (symbol로 비교)
            market_type = "NXT" if stock['symbol'] in nxt_symbols else "KRX"

            stock_msg = {
                "strategy_id": 1,  # TODO: 실제 strategy_id
                "stock_code": stock['symbol'],
                "stock_name": stock.get('name', ''),
                "exchange": stock.get('exchange', 'UNKNOWN'),
                "stock_open": float(stock.get('open_price', 0)),
                "gap_rate": float(stock.get('gap_rate', 0)),
                "expected_change_rate": float(stock.get('expected_change_rate', 0)),
                "volume": int(stock.get('volume', 0)),
                "market_type": market_type,
            }
            stock_messages.append(stock_msg)

            # 상위 20개만 로그 출력
            if i <= 20:
                print(f"    {i:2d}. [{market_type}] {stock.get('name', ''):15s} ({stock['symbol']}) "
                      f"| 갭: {stock.get('gap_rate', 0):+.2f}%")

        if len(all_gap_up_stocks) > 20:
            print(f"    ... 외 {len(all_gap_up_stocks) - 20}개")

        # 배치 메시지 생성 (시장 지표 포함)
        batch_message = {
            "timestamp": timestamp,
            "market_type": "ALL",  # NXT + KRX 전체
            "prediction_phase": "final",  # 최종 예측 (시장 지표 포함)
            "kospi_open": float(kospi_value),
            "kosdaq_open": float(kosdaq_value),
            "kospi200_open": float(kospi200_value),
            "stocks": stock_messages,
            "total_count": len(stock_messages),
            "nxt_count": len(nxt_gap_up_stocks),
            "krx_count": len(krx_gap_up_stocks),
        }

        # JSON 문자열로 변환하여 XCom에 저장
        batch_message_json = json.dumps(batch_message, ensure_ascii=False, default=str)
        context['task_instance'].xcom_push(key='kafka_message', value=batch_message_json)

        print(f"\n  Kafka 메시지 생성 완료: {len(stock_messages)}개 종목 (Final)")
        return batch_message

    build_message = PythonOperator(
        task_id='build_kafka_message',
        python_callable=build_kafka_message,
    )

    # ========================================
    # 6. Kafka 메시지 로그 출력 (실제 발행은 테스트 후)
    # ========================================
    def log_kafka_message(**context):
        """Kafka 메시지 전체 로그 출력 (발행 전 확인용)"""
        import json

        kafka_message = context['task_instance'].xcom_pull(
            key='kafka_message',
            task_ids='build_kafka_message'
        )

        print("=" * 80)
        print("Kafka 메시지 (발행 예정) - Final Prediction")
        print("=" * 80)

        if kafka_message:
            msg_dict = json.loads(kafka_message)
            print(f"  Topic: extract_daily_candidate")
            print(f"  Timestamp: {msg_dict.get('timestamp')}")
            print(f"  Market Type: {msg_dict.get('market_type')}")
            print(f"  Prediction Phase: {msg_dict.get('prediction_phase')}")
            print(f"  Total Count: {msg_dict.get('total_count')} (NXT: {msg_dict.get('nxt_count')}, KRX: {msg_dict.get('krx_count')})")
            print(f"\n  [시장 지표]")
            print(f"    KOSPI:    {msg_dict.get('kospi_open', 0):,.2f}")
            print(f"    KOSDAQ:   {msg_dict.get('kosdaq_open', 0):,.2f}")
            print(f"    KOSPI200: {msg_dict.get('kospi200_open', 0):,.2f}")
            print(f"\n  [종목 리스트]")
            for i, stock in enumerate(msg_dict.get('stocks', []), 1):
                print(f"    {i:2d}. [{stock.get('market_type')}] {stock.get('stock_name', ''):15s} ({stock.get('stock_code')}) "
                      f"| 갭: {stock.get('gap_rate', 0):+.2f}% "
                      f"| 시가: {stock.get('stock_open', 0):,.0f}원")
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
    #     message_key="krx_gap_up_{{ execution_date.strftime('%Y%m%d_%H%M%S') }}",
    # )

    # ========================================
    # Skip 처리
    # ========================================
    skip_no_stocks = EmptyOperator(task_id='skip_no_stocks')

    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    # ========================================
    # Task Dependencies
    # ========================================
    start >> check_stocks

    # 종목 있음: KRX gap filter → 시장지표 조회 → 메시지 생성 → 로그 출력
    check_stocks >> get_krx_stocks >> get_market_index >> filter_krx_gap_up >> build_message >> log_kafka >> end

    # 종목 없음: 스킵
    check_stocks >> skip_no_stocks >> end
