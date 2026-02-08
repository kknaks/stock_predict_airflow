"""Boliger 모델 피처 계산 유틸리티.

preprocess.py (학습)와 동일한 로직으로 피처를 계산한다.
학습-추론 일관성을 위해 함수 시그니처와 계산 로직을 유지한다.

pandarallel 제거 — 단순 groupby.apply 사용 (종목수 ~2000, 충분히 빠름)
"""

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# ============================================================
# 상수 (dataset.py와 동일)
# ============================================================

FEATURE_COLS = [
    "close",
    "bb_upper", "bb_mid", "bb_lower", "bb_width_ratio",
    "bb_position", "d1", "d2",
    "volume", "volume_ratio",
    "macd",
    "ma_5", "ma_20", "ma_50",
    "above_ma5", "above_ma20", "above_ma50", "ma5_ma20_cross",
    "atr_14", "atr_ratio",
    "prev_market_return",
    "prev_return", "prev_range_pct", "prev_upper_shadow", "prev_lower_shadow",
    "day_of_week", "month",
    "is_month_start", "is_month_end", "is_quarter_end",
    "return_5d", "return_20d", "consecutive_up_days",
]

SIDEWAYS_AGG_COLS = [
    "sw_bb_width_slope",
    "sw_volume_slope",
    "sw_rsi_slope",
    "sw_price_range",
    "sw_close_position",
    "sw_d1_slope",
]

RF_TOP_FEATURES = [
    "bb_position",
    "atr_ratio",
    "prev_range_pct",
    "bb_width_ratio",
]

TARGET_COLS = ["day_positive", "day_high_return", "day_low_return"]

# 기본 설정값 (stock.yaml과 동일)
DEFAULT_CONFIG = {
    "bb_period": 20,
    "bb_std": 2,
    "rsi_period": 14,
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_signal": 9,
    "threshold_1": 0.01,
    "min_gap_ratio": 0.0001,
    "max_gap_ratio": 0.03,
    "min_sideways_days": 5,
    "max_seq_len": 50,
    "high_return_threshold": 0.005,
}


# ============================================================
# 전일 패턴 피처
# ============================================================

def compute_prev_pattern(df: pd.DataFrame) -> pd.DataFrame:
    """전일 캔들 패턴 피처 계산."""
    df = df.copy()

    df["prev_open"] = df["open"].shift(1)
    df["prev_high"] = df["high"].shift(1)
    df["prev_low"] = df["low"].shift(1)
    df["prev_close"] = df["close"].shift(1)

    df["prev_return"] = (
        (df["prev_close"] - df["prev_open"]) / df["prev_open"].replace(0, np.nan) * 100
    )
    df["prev_range_pct"] = (
        (df["prev_high"] - df["prev_low"]) / df["prev_close"].replace(0, np.nan) * 100
    )

    prev_body_top = df[["prev_open", "prev_close"]].max(axis=1)
    df["prev_upper_shadow"] = (df["prev_high"] - prev_body_top) / df["prev_close"].replace(
        0, np.nan
    )

    prev_body_bottom = df[["prev_open", "prev_close"]].min(axis=1)
    df["prev_lower_shadow"] = (prev_body_bottom - df["prev_low"]) / df["prev_close"].replace(
        0, np.nan
    )

    return df


# ============================================================
# 이동평균 피처
# ============================================================

def compute_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    """이동평균 및 크로스 피처 계산."""
    df = df.copy()

    df["ma_5"] = df["close"].rolling(5, min_periods=1).mean()
    df["ma_20"] = df["close"].rolling(20, min_periods=1).mean()
    df["ma_50"] = df["close"].rolling(50, min_periods=1).mean()

    prev_close = df["close"].shift(1)
    df["above_ma5"] = (prev_close > df["ma_5"].shift(1)).astype(int)
    df["above_ma20"] = (prev_close > df["ma_20"].shift(1)).astype(int)
    df["above_ma50"] = (prev_close > df["ma_50"].shift(1)).astype(int)

    df["ma5_ma20_cross"] = (df["ma_5"].shift(1) > df["ma_20"].shift(1)).astype(int)

    return df


# ============================================================
# ATR (Average True Range)
# ============================================================

def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """ATR 계산."""
    df = df.copy()

    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift(1))
    low_close = np.abs(df["low"] - df["close"].shift(1))
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

    atr = true_range.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    df["atr_14"] = atr.shift(1)

    prev_close = df["close"].shift(1)
    gap_size = np.abs(df["open"] - prev_close)
    df["atr_ratio"] = gap_size / (df["atr_14"] + 1e-10)

    return df


# ============================================================
# 시간 피처
# ============================================================

def compute_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """시간 관련 피처 계산."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    df["day_of_week"] = df["date"].dt.dayofweek
    df["month"] = df["date"].dt.month

    return df


def compute_time_features_per_stock(df: pd.DataFrame) -> pd.DataFrame:
    """종목별 시간 피처 (월초/월말/분기말)."""
    df = df.copy()

    df["day_of_month"] = df.groupby(df["date"].dt.to_period("M")).cumcount() + 1
    days_in_month = df.groupby(df["date"].dt.to_period("M"))["date"].transform("count")

    df["is_month_start"] = (df["day_of_month"] <= 3).astype(int)
    df["is_month_end"] = (df["day_of_month"] > days_in_month - 3).astype(int)

    df["day_of_quarter"] = df.groupby(df["date"].dt.to_period("Q")).cumcount() + 1
    days_in_quarter = df.groupby(df["date"].dt.to_period("Q"))["date"].transform("count")

    df["is_quarter_end"] = (df["day_of_quarter"] > days_in_quarter - 3).astype(int)

    df.drop(columns=["day_of_month", "day_of_quarter"], inplace=True)

    return df


# ============================================================
# 모멘텀 피처
# ============================================================

def compute_momentum(df: pd.DataFrame) -> pd.DataFrame:
    """모멘텀 및 추세 피처 계산."""
    df = df.copy()

    df["return_5d"] = (
        (df["close"] - df["close"].shift(5)) / df["close"].shift(5).replace(0, np.nan) * 100
    ).shift(1)

    df["return_20d"] = (
        (df["close"] - df["close"].shift(20)) / df["close"].shift(20).replace(0, np.nan) * 100
    ).shift(1)

    daily_return = df["close"].pct_change()
    is_up = (daily_return > 0).astype(int)

    consecutive = is_up * (is_up.groupby((is_up != is_up.shift()).cumsum()).cumcount() + 1)
    df["consecutive_up_days"] = consecutive.shift(1)

    return df


# ============================================================
# 거래량 피처
# ============================================================

def compute_volume_features(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
    """거래량 관련 피처 계산."""
    df = df.copy()

    avg_volume = df["volume"].rolling(period, min_periods=1).mean()

    prev_volume = df["volume"].shift(1)
    df["volume_ratio"] = prev_volume / avg_volume.shift(1).replace(0, np.nan)

    df["volume_norm"] = df["volume"] / avg_volume.replace(0, np.nan)

    return df


# ============================================================
# 볼린저 밴드
# ============================================================

def compute_bollinger(df: pd.DataFrame, period: int, std: int) -> pd.DataFrame:
    """볼린저 밴드 계산 (종목별 호출 전제)."""
    close = df["close"]
    bb_mid = close.rolling(period).mean()
    bb_std = close.rolling(period).std()
    bb_upper = bb_mid + std * bb_std
    bb_lower = bb_mid - std * bb_std
    bb_width = bb_upper - bb_lower

    df = df.copy()
    df["bb_mid"] = bb_mid
    df["bb_upper"] = bb_upper
    df["bb_lower"] = bb_lower
    df["bb_width"] = bb_width
    df["bb_width_ratio"] = bb_width / close.replace(0, np.nan)
    df["bb_position"] = (close - bb_lower) / bb_width.replace(0, np.nan)
    return df


def compute_bollinger_derivatives(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
    """누적면적(20일합), 1차미분, 2차미분."""
    df = df.copy()
    df["cum_area"] = df["bb_width_ratio"].rolling(period).sum()
    df["d1"] = df["cum_area"].diff()
    df["d2"] = df["d1"].diff()
    return df


# ============================================================
# RSI / MACD
# ============================================================

def compute_rsi(series: pd.Series, period: int) -> pd.Series:
    """RSI 계산."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(span=period, min_periods=period).mean()
    avg_loss = loss.ewm(span=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_macd(
    series: pd.Series, fast: int, slow: int, signal: int
) -> pd.DataFrame:
    """MACD 계산."""
    ema_fast = series.ewm(span=fast, min_periods=fast).mean()
    ema_slow = series.ewm(span=slow, min_periods=slow).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, min_periods=signal).mean()
    return pd.DataFrame({"macd": macd_line, "macd_signal": signal_line})


# ============================================================
# 종목별 피처 통합
# ============================================================

def compute_features(df: pd.DataFrame, cfg: Optional[dict] = None) -> pd.DataFrame:
    """종목별 피처 엔지니어링 통합.

    Args:
        df: OHLCV DataFrame (symbol, date, open, high, low, close, volume, market_return).
        cfg: 설정 dict. None이면 DEFAULT_CONFIG 사용.
    """
    if cfg is None:
        cfg = DEFAULT_CONFIG

    def _per_stock(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date")

        g = compute_bollinger(g, cfg["bb_period"], cfg["bb_std"])
        g = compute_bollinger_derivatives(g, cfg["bb_period"])
        g["rsi"] = compute_rsi(g["close"], cfg["rsi_period"])
        macd_df = compute_macd(g["close"], cfg["macd_fast"], cfg["macd_slow"], cfg["macd_signal"])
        g["macd"] = macd_df["macd"].values
        g["macd_signal"] = macd_df["macd_signal"].values

        g = compute_prev_pattern(g)
        g = compute_moving_averages(g)
        g = compute_atr(g, period=14)
        g = compute_time_features_per_stock(g)
        g = compute_momentum(g)
        g = compute_volume_features(g, period=cfg["bb_period"])

        g["prev_market_return"] = g["market_return"].shift(1)

        return g

    df = compute_time_features(df)

    return df.groupby("symbol", group_keys=False).apply(_per_stock)


# ============================================================
# 변곡점 탐지
# ============================================================

def detect_inflection_points(df: pd.DataFrame, cfg: Optional[dict] = None) -> pd.DataFrame:
    """변곡점 탐지: 횡보→확산 전환 + 갭 상승 필터.

    Args:
        df: 피처가 계산된 DataFrame.
        cfg: 설정 dict.
    """
    if cfg is None:
        cfg = DEFAULT_CONFIG

    min_gap = cfg.get("min_gap_ratio", 0.0001)
    max_gap = cfg.get("max_gap_ratio", 0.03)
    min_sideways = cfg.get("min_sideways_days", 5)
    threshold = cfg.get("threshold_1", 0.01)

    def _per_stock(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        d2 = g["d2"]
        d2_std = d2.rolling(20).std()

        prev_close = g["close"].shift(1)
        g["gap_ratio"] = (g["open"] - prev_close) / prev_close.replace(0, np.nan)

        d2_std_prev = d2_std.shift(1)
        is_sideways = d2_std_prev < threshold
        not_sideways = ~is_sideways
        group_id = not_sideways.cumsum()
        sideways_days = is_sideways.groupby(group_id).cumsum().astype(int)
        g["sideways_days"] = sideways_days

        g["is_inflection"] = (
            (d2_std_prev < threshold)
            & (g["gap_ratio"] >= min_gap)
            & (g["gap_ratio"] <= max_gap)
            & (sideways_days >= min_sideways)
        )

        return g

    return df.groupby("symbol", group_keys=False).apply(_per_stock)


# ============================================================
# 횡보 구간 aggregate 피처
# ============================================================

def compute_sideways_features(df: pd.DataFrame, cfg: Optional[dict] = None) -> pd.DataFrame:
    """횡보 구간 aggregate 피처 계산.

    각 변곡점(is_inflection=True) 행에 대해,
    직전 sideways_days 기간의 피처 통계를 계산한다.
    """
    if cfg is None:
        cfg = DEFAULT_CONFIG

    max_seq_len = cfg.get("max_seq_len", 50)

    sw_cols = [
        "sw_bb_width_slope",
        "sw_volume_slope",
        "sw_rsi_slope",
        "sw_price_range",
        "sw_close_position",
        "sw_d1_slope",
    ]
    for col in sw_cols:
        df[col] = np.nan

    def _per_stock(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        inflection_mask = g["is_inflection"]
        if inflection_mask.sum() == 0:
            return g

        bb_width_arr = g["bb_width_ratio"].values
        volume_arr = g["volume"].values
        rsi_arr = g["rsi"].values
        close_arr = g["close"].values
        d1_arr = g["d1"].values
        sideways_arr = g["sideways_days"].values

        inflection_indices = g.index[inflection_mask].tolist()

        for idx_label in inflection_indices:
            iloc_pos = g.index.get_loc(idx_label)
            raw_sw_days = int(sideways_arr[iloc_pos])
            sw_days = min(raw_sw_days, max_seq_len)

            if iloc_pos < sw_days or sw_days < 3:
                continue

            start = iloc_pos - sw_days
            end = iloc_pos

            sw_close = close_arr[start:end]
            sw_bb_width = bb_width_arr[start:end]
            sw_volume = volume_arr[start:end]
            sw_rsi = rsi_arr[start:end]
            sw_d1 = d1_arr[start:end]

            if (
                np.isnan(sw_close).any()
                or np.isnan(sw_bb_width).any()
                or np.isnan(sw_rsi).any()
                or np.isnan(sw_d1).any()
            ):
                continue

            n = len(sw_close)
            x = np.arange(n, dtype=np.float64)

            try:
                slope_bb = np.polyfit(x, sw_bb_width, 1)[0]
            except (np.linalg.LinAlgError, ValueError):
                slope_bb = 0.0

            vol_mean = sw_volume.mean()
            if vol_mean > 0 and not np.isnan(sw_volume).any():
                try:
                    slope_vol = np.polyfit(x, sw_volume / vol_mean, 1)[0]
                except (np.linalg.LinAlgError, ValueError):
                    slope_vol = 0.0
            else:
                slope_vol = 0.0

            try:
                slope_rsi = np.polyfit(x, sw_rsi, 1)[0]
            except (np.linalg.LinAlgError, ValueError):
                slope_rsi = 0.0

            close_mean = sw_close.mean()
            close_min = sw_close.min()
            close_max = sw_close.max()
            if close_mean > 0:
                price_range = (close_max - close_min) / close_mean
            else:
                price_range = 0.0

            range_diff = close_max - close_min
            if range_diff > 0:
                close_position = (sw_close[-1] - close_min) / range_diff
            else:
                close_position = 0.5

            try:
                slope_d1 = np.polyfit(x, sw_d1, 1)[0]
            except (np.linalg.LinAlgError, ValueError):
                slope_d1 = 0.0

            g.at[idx_label, "sw_bb_width_slope"] = slope_bb
            g.at[idx_label, "sw_volume_slope"] = slope_vol
            g.at[idx_label, "sw_rsi_slope"] = slope_rsi
            g.at[idx_label, "sw_price_range"] = price_range
            g.at[idx_label, "sw_close_position"] = close_position
            g.at[idx_label, "sw_d1_slope"] = slope_d1

        return g

    return df.groupby("symbol", group_keys=False).apply(_per_stock)


# ============================================================
# 스케일러 로드
# ============================================================

def load_scaler(scaler_path: str) -> tuple[np.ndarray, np.ndarray]:
    """저장된 scaler 로드.

    Args:
        scaler_path: scaler.npz 파일 경로.

    Returns:
        (mean, std): 각 피처의 평균/표준편차.
    """
    path = Path(scaler_path)
    if not path.exists():
        raise FileNotFoundError(f"Scaler not found: {scaler_path}")

    loaded = np.load(scaler_path, allow_pickle=True)
    mean = loaded["mean"]
    std = loaded["std"]

    return mean, std


# ============================================================
# Input1 추출 (시계열 피처)
# ============================================================

def extract_input1(
    stock_df: pd.DataFrame,
    inflection_idx: int,
    seq_len: int,
    global_mean: np.ndarray,
    global_std: np.ndarray,
) -> Optional[np.ndarray]:
    """변곡점 기준 Input1(시계열 피처) 추출 + 스케일링.

    Args:
        stock_df: 종목별 DataFrame (date 정렬, reset_index 상태).
        inflection_idx: 변곡점의 iloc 위치.
        seq_len: 추출할 시퀀스 길이 (sideways_days, max_seq_len 중 작은 값).
        global_mean: 전역 스케일러 평균 (33,).
        global_std: 전역 스케일러 표준편차 (33,).

    Returns:
        (seq_len, 33) scaled numpy array, or None if invalid.
    """
    if inflection_idx < seq_len or seq_len == 0:
        return None

    feature_arr = stock_df[FEATURE_COLS].values
    window = feature_arr[inflection_idx - seq_len: inflection_idx]

    if np.isnan(window).any():
        return None

    scaled = (window - global_mean) / global_std
    if not np.isfinite(scaled).all():
        return None

    return scaled


# ============================================================
# Input2 partial 구성 (gap_ratio 제외)
# ============================================================

def build_input2_partial(
    stock_df: pd.DataFrame,
    inflection_idx: int,
    global_mean: np.ndarray,
    global_std: np.ndarray,
) -> Optional[np.ndarray]:
    """Input2 부분 구성 (gap_ratio=0 placeholder).

    Input2 구조 (12,):
    [0] gap_ratio (placeholder 0.0 - 장시작 시 채움)
    [1] placeholder (0.0)
    [2:8] sw_* aggregate 피처 6개
    [8:12] RF top features 4개 (전역 스케일링)

    Args:
        stock_df: 종목별 DataFrame (date 정렬, reset_index 상태).
        inflection_idx: 변곡점의 iloc 위치.
        global_mean: 전역 스케일러 평균 (33,).
        global_std: 전역 스케일러 표준편차 (33,).

    Returns:
        (12,) numpy array, or None if invalid.
    """
    feature_arr = stock_df[FEATURE_COLS].values

    # 횡보 aggregate 피처
    has_sw_cols = all(col in stock_df.columns for col in SIDEWAYS_AGG_COLS)
    if has_sw_cols:
        sw_agg = stock_df[SIDEWAYS_AGG_COLS].iloc[inflection_idx].values
        if np.isnan(sw_agg).any():
            return None
    else:
        sw_agg = np.array([0.0, 0.0, 0.0, 0.0, 0.5, 0.0])

    # RF top features: 변곡점 직전(idx-1)의 원본 값 → 전역 스케일링
    if inflection_idx < 1:
        return None

    rf_feature_indices = [FEATURE_COLS.index(f) for f in RF_TOP_FEATURES]
    rf_features = feature_arr[inflection_idx - 1, rf_feature_indices]
    if np.isnan(rf_features).any():
        return None

    rf_means = global_mean[rf_feature_indices]
    rf_stds = global_std[rf_feature_indices]
    rf_features_scaled = (rf_features - rf_means) / rf_stds
    if not np.isfinite(rf_features_scaled).all():
        return None

    input2 = np.array([
        0.0,                       # gap_ratio placeholder
        0.0,                       # placeholder
        sw_agg[0],                 # sw_bb_width_slope
        sw_agg[1],                 # sw_volume_slope
        sw_agg[2],                 # sw_rsi_slope
        sw_agg[3],                 # sw_price_range
        sw_agg[4],                 # sw_close_position
        sw_agg[5],                 # sw_d1_slope
        rf_features_scaled[0],     # bb_position
        rf_features_scaled[1],     # atr_ratio
        rf_features_scaled[2],     # prev_range_pct
        rf_features_scaled[3],     # bb_width_ratio
    ])

    return input2


# ============================================================
# Input2 완성 (gap_ratio 채움)
# ============================================================

def complete_input2(input2_partial: np.ndarray, gap_ratio: float) -> np.ndarray:
    """Input2에 gap_ratio를 채워 완성.

    Args:
        input2_partial: (12,) placeholder 상태의 input2.
        gap_ratio: (open - prev_close) / prev_close.

    Returns:
        (12,) 완성된 input2.
    """
    input2 = input2_partial.copy()
    input2[0] = np.clip(gap_ratio, -0.3, 0.3)
    return input2


# ============================================================
# Flat features (Tree 모델용)
# ============================================================

def extract_flat_features(
    stock_df: pd.DataFrame,
    inflection_idx: int,
    global_mean: np.ndarray,
    global_std: np.ndarray,
) -> Optional[np.ndarray]:
    """변곡점 당일 피처 추출 (Tree 모델용, 스케일링 적용).

    Args:
        stock_df: 종목별 DataFrame.
        inflection_idx: 변곡점의 iloc 위치.
        global_mean: 전역 스케일러 평균 (33,).
        global_std: 전역 스케일러 표준편차 (33,).

    Returns:
        (33,) scaled numpy array, or None if invalid.
    """
    feature_arr = stock_df[FEATURE_COLS].values
    inflection_features = feature_arr[inflection_idx]

    if np.isnan(inflection_features).any():
        inflection_features = np.nan_to_num(inflection_features, nan=0.0)

    scaled = (inflection_features - global_mean) / global_std
    if not np.isfinite(scaled).all():
        scaled = np.nan_to_num(scaled, nan=0.0, posinf=0.0, neginf=0.0)

    return scaled


# ============================================================
# 전체 파이프라인: 후보 추출
# ============================================================

def extract_candidates(
    df: pd.DataFrame,
    market_df: pd.DataFrame,
    scaler_path: str,
    cfg: Optional[dict] = None,
) -> dict:
    """전체 후보 추출 파이프라인.

    Args:
        df: 전 종목 OHLCV DataFrame (symbol, date, open, high, low, close, volume).
        market_df: 시장 지수 DataFrame (date, kospi_close, kosdaq_close).
        scaler_path: scaler.npz 파일 경로.
        cfg: 설정 dict.

    Returns:
        후보 dict (pickle 저장용).
    """
    if cfg is None:
        cfg = DEFAULT_CONFIG

    max_seq_len = cfg.get("max_seq_len", 50)

    # 1. 시장 수익률 계산
    market_df = market_df.copy()
    market_df["date"] = pd.to_datetime(market_df["date"])
    market_df = market_df.sort_values("date")
    # KOSPI + KOSDAQ 합성 지수 수익률
    market_df["market_return"] = market_df["kospi_close"].pct_change() * 100

    # 2. 시장 수익률을 종목 데이터에 병합
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.merge(market_df[["date", "market_return"]], on="date", how="left")
    df["market_return"] = df["market_return"].fillna(0.0)

    print(f"[1/5] Feature computation for {df['symbol'].nunique()} stocks...")
    df = compute_features(df, cfg)

    # 3. 워밍업 NaN 제거
    warmup_cols = [
        "bb_mid", "bb_upper", "bb_lower", "cum_area", "d2",
        "rsi", "macd", "macd_signal",
        "ma_50", "atr_14",
        "prev_return", "prev_range_pct",
        "return_5d", "return_20d",
        "volume",
        "prev_market_return",
    ]
    df.dropna(subset=warmup_cols, inplace=True)

    # 4. 변곡점 탐지 — 여기서는 "오늘" 기준이 아니라 모든 날짜에서 횡보 판단
    # 실제로 필요한 건 "내일" 기준으로 횡보 중인 종목
    # → 마지막 날짜 기준 d2_std < threshold인 종목만 추출
    print(f"[2/5] Detecting sideways patterns...")
    df = detect_inflection_points(df, cfg)

    # 5. 횡보 피처 계산
    print(f"[3/5] Computing sideways aggregate features...")
    df = compute_sideways_features(df, cfg)

    # 6. 스케일러 로드
    print(f"[4/5] Loading scaler...")
    global_mean, global_std = load_scaler(scaler_path)

    # 7. 후보 추출: 마지막 날짜 기준으로 횡보 중인 종목
    print(f"[5/5] Extracting candidates...")
    last_date = df["date"].max()
    threshold = cfg.get("threshold_1", 0.01)
    min_sideways = cfg.get("min_sideways_days", 5)

    candidates = {}
    for symbol, stock_df in df.groupby("symbol"):
        stock_df = stock_df.sort_values("date").reset_index(drop=True)

        if len(stock_df) == 0:
            continue

        # 마지막 행 기준으로 현재 횡보 중인지 확인
        last_idx = len(stock_df) - 1
        last_row = stock_df.iloc[last_idx]

        # d2_std 확인 (현재 횡보 중인지)
        d2 = stock_df["d2"]
        d2_std = d2.rolling(20).std()

        if last_idx < 20:
            continue

        current_d2_std = d2_std.iloc[last_idx]
        if pd.isna(current_d2_std) or current_d2_std >= threshold:
            continue

        # 현재 횡보 일수 계산
        current_sideways = int(last_row.get("sideways_days", 0))
        if current_sideways < min_sideways:
            continue

        # 시퀀스 길이 결정
        seq_len = min(current_sideways, max_seq_len)

        # Input1 추출 (현재 횡보 기간)
        # 변곡점은 아직 발생하지 않았으므로, last_idx + 1 위치에 내일 시가가 올 것
        # → last_idx까지의 데이터를 시퀀스로 사용 (내일 시가 갭은 장시작 시 결정)
        if last_idx < seq_len:
            continue

        # 시퀀스 데이터 추출 (last_idx - seq_len + 1 ~ last_idx 포함)
        # 학습 시: inflection_idx - seq_len : inflection_idx (전일까지)
        # 추론 시: last_idx - seq_len + 1 : last_idx + 1 (오늘까지)
        feature_arr = stock_df[FEATURE_COLS].values
        window = feature_arr[last_idx - seq_len + 1: last_idx + 1]

        if np.isnan(window).any():
            continue

        input1 = (window - global_mean) / global_std
        if not np.isfinite(input1).all():
            continue

        # 횡보 aggregate 피처 (현재 횡보 기간 기준)
        sw_features = _compute_current_sideways_features(
            stock_df, last_idx, seq_len, max_seq_len
        )
        if sw_features is None:
            continue

        # RF top features (마지막 행 기준)
        rf_feature_indices = [FEATURE_COLS.index(f) for f in RF_TOP_FEATURES]
        rf_features = feature_arr[last_idx, rf_feature_indices]
        if np.isnan(rf_features).any():
            continue

        rf_means = global_mean[rf_feature_indices]
        rf_stds = global_std[rf_feature_indices]
        rf_features_scaled = (rf_features - rf_means) / rf_stds
        if not np.isfinite(rf_features_scaled).all():
            continue

        # Input2 partial (gap_ratio = 0 placeholder)
        input2_partial = np.array([
            0.0,                       # gap_ratio placeholder
            0.0,                       # placeholder
            sw_features[0],            # sw_bb_width_slope
            sw_features[1],            # sw_volume_slope
            sw_features[2],            # sw_rsi_slope
            sw_features[3],            # sw_price_range
            sw_features[4],            # sw_close_position
            sw_features[5],            # sw_d1_slope
            rf_features_scaled[0],     # bb_position
            rf_features_scaled[1],     # atr_ratio
            rf_features_scaled[2],     # prev_range_pct
            rf_features_scaled[3],     # bb_width_ratio
        ])

        prev_close = float(stock_df["close"].iloc[last_idx])

        candidates[str(symbol)] = {
            "stock_name": "",  # DAG에서 stock_metadata 조인으로 채움
            "exchange": "",    # DAG에서 채움
            "is_nxt": False,   # DAG에서 채움
            "prev_close": prev_close,
            "sideways_days": current_sideways,
            "input1": input1,
            "length": seq_len,
            "input2_partial": input2_partial,
        }

    print(f"  Candidates found: {len(candidates)}")
    return candidates


def _compute_current_sideways_features(
    stock_df: pd.DataFrame,
    last_idx: int,
    seq_len: int,
    max_seq_len: int,
) -> Optional[np.ndarray]:
    """현재 횡보 기간의 aggregate 피처 계산.

    학습 시 compute_sideways_features()와 동일한 로직.
    """
    sw_days = min(seq_len, max_seq_len)

    if last_idx < sw_days or sw_days < 3:
        return None

    # 횡보 구간: last_idx - sw_days + 1 ~ last_idx (포함)
    start = last_idx - sw_days + 1
    end = last_idx + 1

    bb_width_arr = stock_df["bb_width_ratio"].values[start:end]
    volume_arr = stock_df["volume"].values[start:end]
    rsi_arr = stock_df["rsi"].values[start:end]
    close_arr = stock_df["close"].values[start:end]
    d1_arr = stock_df["d1"].values[start:end]

    if (
        np.isnan(bb_width_arr).any()
        or np.isnan(rsi_arr).any()
        or np.isnan(d1_arr).any()
        or np.isnan(close_arr).any()
    ):
        return None

    n = len(close_arr)
    x = np.arange(n, dtype=np.float64)

    try:
        slope_bb = np.polyfit(x, bb_width_arr, 1)[0]
    except (np.linalg.LinAlgError, ValueError):
        slope_bb = 0.0

    vol_mean = volume_arr.mean()
    if vol_mean > 0 and not np.isnan(volume_arr).any():
        try:
            slope_vol = np.polyfit(x, volume_arr / vol_mean, 1)[0]
        except (np.linalg.LinAlgError, ValueError):
            slope_vol = 0.0
    else:
        slope_vol = 0.0

    try:
        slope_rsi = np.polyfit(x, rsi_arr, 1)[0]
    except (np.linalg.LinAlgError, ValueError):
        slope_rsi = 0.0

    close_mean = close_arr.mean()
    close_min = close_arr.min()
    close_max = close_arr.max()
    price_range = (close_max - close_min) / close_mean if close_mean > 0 else 0.0

    range_diff = close_max - close_min
    close_position = (close_arr[-1] - close_min) / range_diff if range_diff > 0 else 0.5

    try:
        slope_d1 = np.polyfit(x, d1_arr, 1)[0]
    except (np.linalg.LinAlgError, ValueError):
        slope_d1 = 0.0

    return np.array([
        slope_bb,
        slope_vol,
        slope_rsi,
        price_range,
        close_position,
        slope_d1,
    ])
