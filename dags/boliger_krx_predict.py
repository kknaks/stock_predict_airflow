"""Boliger KRX 장시작 예측 트리거 DAG.

KRX 장시작(KST 09:00)에 실행하여:
1. 전일 추출된 candidate pickle 로드 (KRX 후보만)
2. 시가 조회 (라이브: KIS API / 매뉴얼: DB)
3. gap_ratio 계산 + min/max 필터
4. Input2 완성 (gap_ratio 채움)
5. 완성 pickle 저장
6. Kafka 트리거 발행 (경량 메시지)

수동 실행 시 params.target_date (YYYY-MM-DD)로 대상일 지정 가능.
"""

import json
import pickle
import time
from datetime import datetime, timedelta, timezone as tz
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from operators.market_data_operator import MarketOpenCheckOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def wait_for_krx_open(**context):
    """KRX 시가 확정 대기 (09:00 트리거 후 1초 대기)."""
    manual_date = context.get("params", {}).get("target_date", "")
    if manual_date:
        print(f"[Manual] target_date={manual_date}, skipping wait.")
        return
    print("Waiting 1s for KRX market open price stabilization...")
    time.sleep(1)
    print("Wait complete.")


def load_and_predict(**context):
    """KRX 후보 pickle 로드 → 시가 조회 → gap 필터 → 완성 pickle + Kafka."""
    import numpy as np

    from utils.boliger_features import complete_input2, DEFAULT_CONFIG

    kst = tz(timedelta(hours=9))
    manual_date = context.get("params", {}).get("target_date", "")
    if manual_date:
        target_date = datetime.strptime(manual_date, "%Y-%m-%d").date()
    else:
        target_date = datetime.now(kst).date()

    today_str = target_date.strftime("%Y%m%d")

    # 1. Candidate pickle 로드
    pickle_path = Path(f"/shared/boliger/candidates_{today_str}.pkl")
    if not pickle_path.exists():
        raise AirflowSkipException(f"Candidate pickle not found: {pickle_path}")

    with open(pickle_path, "rb") as f:
        data = pickle.load(f)

    krx_codes = data.get("krx_codes", [])
    candidates = data.get("candidates", {})
    config = data.get("config", DEFAULT_CONFIG)

    print(f"Loaded {len(krx_codes)} KRX candidates from {pickle_path}")

    if not krx_codes:
        raise AirflowSkipException("No KRX candidates.")

    # 2. 시가 조회
    #    - 오늘 날짜: KIS API 라이브 조회 (매뉴얼이든 스케줄이든)
    #    - 과거 날짜: DB에서 조회
    price_map = {}
    is_today = target_date == datetime.now(kst).date()

    if manual_date and not is_today:
        # 매뉴얼 모드 + 과거 날짜: DB에서 해당일 시가 조회
        from sqlalchemy import create_engine, text as sa_text
        from utils.common import get_db_url

        db_url = get_db_url("stock_db")
        engine = create_engine(db_url)
        print(f"[Manual/Past] Querying open prices from DB for {target_date}...")
        with engine.connect() as conn:
            result = conn.execute(
                sa_text("SELECT symbol, open FROM stock_prices WHERE date = :d"),
                {"d": str(target_date)},
            )
            for row in result:
                sym = str(row[0])
                if sym in candidates:
                    price_map[sym] = float(row[1])
    else:
        # 라이브 모드 (오늘 날짜): KIS API 현재가 조회
        from utils.api_client import KISAPIClient as KISApiClient
        from utils.common import get_kis_credentials

        app_key, app_secret = get_kis_credentials("kis_api")
        api_client = KISApiClient(app_key=app_key, app_secret=app_secret)
        print(f"Querying KRX opening prices for {len(krx_codes)} stocks (live)...")
        price_data = api_client.get_current_price_batch(krx_codes, market_code="J")
        for p in price_data:
            if p and p.get("open_price", 0) > 0:
                price_map[p["symbol"]] = p["open_price"]

    print(f"  Got prices for {len(price_map)}/{len(krx_codes)} stocks")

    # 3. Gap ratio 계산 + 필터
    min_gap = config.get("min_gap_ratio", 0.0001)
    max_gap = config.get("max_gap_ratio", 0.03)

    ready_stocks = []
    for code in krx_codes:
        if code not in candidates or code not in price_map:
            continue

        cand = candidates[code]
        stock_open = float(price_map[code])
        prev_close = float(cand["prev_close"])

        if prev_close <= 0:
            continue

        gap_ratio = (stock_open - prev_close) / prev_close

        # 갭 필터
        if gap_ratio < min_gap or gap_ratio > max_gap:
            continue

        # Input2 완성
        input2 = complete_input2(cand["input2_partial"], gap_ratio)

        ready_stocks.append({
            "stock_code": code,
            "stock_name": cand.get("stock_name", ""),
            "exchange": cand.get("exchange", ""),
            "market_cap": cand.get("market_cap", 0),
            "avg_volume_20d": cand.get("avg_volume_20d"),
            "stock_open": stock_open,
            "gap_ratio": gap_ratio,
            "prev_close": prev_close,
            "input1": cand["input1"],
            "length": cand["length"],
            "input2": input2,
        })

    print(f"\nFiltered: {len(ready_stocks)} stocks passed gap filter [{min_gap}, {max_gap}]")

    if not ready_stocks:
        raise AirflowSkipException(
            f"No stocks passed gap filter [{min_gap}, {max_gap}]."
        )

    # 4. 완성 pickle 저장
    output_dir = Path("/shared/boliger")
    ready_pickle = {
        "date": str(target_date),
        "exchange_type": "KRX",
        "stocks": ready_stocks,
    }

    ready_path = output_dir / f"krx_ready_{today_str}.pkl"
    with open(ready_path, "wb") as f:
        pickle.dump(ready_pickle, f, protocol=pickle.HIGHEST_PROTOCOL)

    file_size_mb = ready_path.stat().st_size / (1024 * 1024)
    print(f"Ready pickle saved: {ready_path} ({file_size_mb:.1f} MB)")

    # 5. Kafka 트리거 메시지 생성
    trigger_message = {
        "exchange_type": "KRX",
        "pickle_path": str(ready_path),
        "stock_count": len(ready_stocks),
        "date": str(target_date),
    }

    trigger_json = json.dumps(trigger_message, ensure_ascii=False, default=str)
    context["task_instance"].xcom_push(key="kafka_message", value=trigger_json)
    context["task_instance"].xcom_push(key="ready_count", value=len(ready_stocks))

    print(f"\nKafka trigger prepared: {len(ready_stocks)} stocks")
    for s in ready_stocks:
        print(f"  {s['stock_code']} ({s['stock_name']}) gap={s['gap_ratio']:.2%}")


with DAG(
    dag_id="boliger_krx_predict",
    default_args=default_args,
    description="Boliger KRX 장시작 예측 트리거",
    schedule_interval="0 0 * * 1-5",  # UTC 00:00 = KST 09:00, 월~금
    params={"target_date": ""},  # YYYY-MM-DD, 빈값이면 오늘(KST)
    start_date=timezone.datetime(2026, 2, 1),
    catchup=False,
    tags=["boliger", "krx", "predict", "kafka"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_market = MarketOpenCheckOperator(
        task_id="check_market_open",
        branch_if_open=["wait_for_open"],
        branch_if_closed="skip",
    )

    wait = PythonOperator(
        task_id="wait_for_open",
        python_callable=wait_for_krx_open,
    )

    predict = PythonOperator(
        task_id="load_and_predict",
        python_callable=load_and_predict,
        execution_timeout=timedelta(minutes=10),
    )

    from operators.kafka_operator import KafkaPublishOperator

    publish = KafkaPublishOperator(
        task_id="publish_kafka",
        kafka_topic="boliger_prediction_trigger",
        message="{{ ti.xcom_pull(key='kafka_message', task_ids='load_and_predict') }}",
        message_key="boliger_krx_{{ execution_date.strftime('%Y%m%d') }}",
    )

    skip = EmptyOperator(task_id="skip")

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    start >> check_market
    check_market >> wait >> predict >> publish >> end
    check_market >> skip >> end
