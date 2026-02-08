"""Boliger 후보 추출 DAG.

매일 장 시작 전(KST 05:00) 실행하여:
1. DB에서 전 종목 OHLCV 조회 (최근 100일, 피처 warmup용)
2. Boliger 피처 계산 (BB, d1/d2, MACD, MA, ATR 등 33개)
3. 횡보 감지 (d2_std < threshold)
4. 후보별 Input1 추출 + scaler 적용
5. 횡보 aggregate 피처 + RF top features 계산
6. NXT/KRX 분류
7. pickle 저장

수동 실행 시 params.target_date (YYYY-MM-DD)로 예측 대상일 지정 가능.
"""

import pickle
from datetime import datetime, timedelta, timezone as tz
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from operators.market_data_operator import MarketOpenCheckOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_candidates(**context):
    """전 종목에서 횡보 후보 추출 + pickle 저장."""
    import numpy as np
    import pandas as pd
    from sqlalchemy import create_engine, text

    from utils.common import get_db_url
    from utils.boliger_features import (
        extract_candidates as run_extraction,
        DEFAULT_CONFIG,
    )

    db_url = get_db_url("stock_db")
    engine = create_engine(db_url)

    # 예측 대상일 결정 (수동 입력 or 오늘)
    kst = tz(timedelta(hours=9))
    manual_date = context.get("params", {}).get("target_date", "")
    if manual_date:
        target_date = datetime.strptime(manual_date, "%Y-%m-%d").date()
    else:
        target_date = datetime.now(kst).date()  # KST 05:00 실행 → 오늘이 예측 대상일

    print(f"Prediction target date: {target_date}")

    # 1. 전 종목 OHLCV 조회 (최근 100일 → warmup 후 70일 유효)
    ohlcv_query = text("""
        SELECT sp.symbol, sp.date, sp.open, sp.high, sp.low, sp.close, sp.volume
        FROM stock_prices sp
        JOIN stock_metadata sm ON sp.symbol = sm.symbol
        WHERE sm.status = 'ACTIVE'
          AND sp.date >= (CURRENT_DATE - INTERVAL '100 days')
        ORDER BY sp.symbol, sp.date
    """)

    print("Querying OHLCV data...")
    with engine.connect() as conn:
        df = pd.read_sql(ohlcv_query, conn)
    print(f"  OHLCV rows: {len(df):,}, stocks: {df['symbol'].nunique()}")

    # 2. 시장 지수 조회 (market_return 계산용)
    market_query = text("""
        SELECT date, kospi_close, kosdaq_close
        FROM market_indices
        WHERE date >= (CURRENT_DATE - INTERVAL '100 days')
        ORDER BY date
    """)

    print("Querying market indices...")
    with engine.connect() as conn:
        market_df = pd.read_sql(market_query, conn)
    print(f"  Market index rows: {len(market_df)}")

    # 3. 종목 메타데이터 조회 (NXT 분류, 종목명)
    meta_query = text("""
        SELECT symbol, name, exchange, market_cap, is_nxt_tradable, is_nxt_stopped
        FROM stock_metadata
        WHERE status = 'ACTIVE'
    """)

    print("Querying stock metadata...")
    with engine.connect() as conn:
        meta_df = pd.read_sql(meta_query, conn)
    print(f"  Active stocks: {len(meta_df)}")

    # NXT 매핑
    meta_dict = {}
    for _, row in meta_df.iterrows():
        is_nxt = bool(row.get("is_nxt_tradable", False)) and not bool(
            row.get("is_nxt_stopped", False)
        )
        meta_dict[str(row["symbol"])] = {
            "stock_name": str(row.get("name", "")),
            "exchange": str(row.get("exchange", "")),
            "market_cap": float(row.get("market_cap", 0) or 0),
            "is_nxt": is_nxt,
        }

    # 4. 종목별 20일 평균 거래량 계산
    avg_vol_map = {}
    for symbol, g in df.groupby("symbol"):
        g_sorted = g.sort_values("date")
        avg_vol = g_sorted["volume"].rolling(20, min_periods=1).mean().iloc[-1]
        avg_vol_map[str(symbol)] = float(avg_vol) if pd.notna(avg_vol) else None

    # 5. 후보 추출
    scaler_path = "/opt/airflow/data/boliger/scaler.npz"
    candidates = run_extraction(df, market_df, scaler_path, cfg=DEFAULT_CONFIG)

    if not candidates:
        print("No candidates found!")
        return

    # 6. 메타데이터 채우기 + NXT/KRX 분류
    nxt_codes = []
    krx_codes = []

    for symbol, cand in candidates.items():
        meta = meta_dict.get(symbol, {})
        cand["stock_name"] = meta.get("stock_name", "")
        cand["exchange"] = meta.get("exchange", "")
        cand["market_cap"] = meta.get("market_cap", 0)
        cand["avg_volume_20d"] = avg_vol_map.get(symbol)
        cand["is_nxt"] = meta.get("is_nxt", False)

        if cand["is_nxt"]:
            nxt_codes.append(symbol)
        else:
            krx_codes.append(symbol)

    print(f"\nCandidate summary:")
    print(f"  Total: {len(candidates)}")
    print(f"  NXT tradable: {len(nxt_codes)}")
    print(f"  KRX only: {len(krx_codes)}")

    # 7. pickle 저장
    output_dir = Path("/shared/boliger")
    output_dir.mkdir(parents=True, exist_ok=True)

    pickle_data = {
        "date": str(target_date),
        "created_at": datetime.now(kst).isoformat(),
        "config": {
            "min_gap_ratio": DEFAULT_CONFIG["min_gap_ratio"],
            "max_gap_ratio": DEFAULT_CONFIG["max_gap_ratio"],
            "min_sideways_days": DEFAULT_CONFIG["min_sideways_days"],
            "threshold_1": DEFAULT_CONFIG["threshold_1"],
            "max_seq_len": DEFAULT_CONFIG["max_seq_len"],
        },
        "candidates": candidates,
        "nxt_codes": nxt_codes,
        "krx_codes": krx_codes,
    }

    pickle_path = output_dir / f"candidates_{target_date.strftime('%Y%m%d')}.pkl"
    with open(pickle_path, "wb") as f:
        pickle.dump(pickle_data, f, protocol=pickle.HIGHEST_PROTOCOL)

    file_size_mb = pickle_path.stat().st_size / (1024 * 1024)
    print(f"\nPickle saved: {pickle_path} ({file_size_mb:.1f} MB)")

    # XCom에 경로 저장
    context["task_instance"].xcom_push(key="pickle_path", value=str(pickle_path))
    context["task_instance"].xcom_push(key="candidate_count", value=len(candidates))


with DAG(
    dag_id="boliger_candidate_extract",
    default_args=default_args,
    description="Boliger 횡보 후보 추출 - 장시작 전 실행",
    schedule_interval="0 20 * * 0-4",  # UTC 20:00 (전날) = KST 05:00, 월~금
    params={"target_date": ""},  # YYYY-MM-DD, 빈값이면 오늘(KST)
    start_date=timezone.datetime(2026, 2, 1),
    catchup=False,
    tags=["boliger", "candidate", "extract"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_market = MarketOpenCheckOperator(
        task_id="check_market_open",
        branch_if_open=["extract_candidates"],
        branch_if_closed="skip",
    )

    extract = PythonOperator(
        task_id="extract_candidates",
        python_callable=extract_candidates,
        execution_timeout=timedelta(minutes=30),
    )

    skip = EmptyOperator(task_id="skip")

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    start >> check_market
    check_market >> extract >> end
    check_market >> skip >> end
