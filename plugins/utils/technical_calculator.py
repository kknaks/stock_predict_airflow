"""
기술지표 계산 모듈

독립적으로 기술지표를 계산합니다.
"""

import pandas as pd
import numpy as np
from typing import Optional


class TechnicalCalculator:
    """
    기술지표 계산 클래스
    """

    @staticmethod
    def calculate_all_features(
        stock_df: pd.DataFrame,
        market_df: Optional[pd.DataFrame] = None,
        db_url: Optional[str] = None
    ) -> pd.DataFrame:
        """
        모든 기술지표 계산

        Args:
            stock_df: 종목 OHLCV DataFrame
                      필수 컬럼: symbol, date, open, high, low, close, volume
            market_df: 시장 지수 DataFrame (optional)
                      필수 컬럼: date, kospi_open, kospi_close, kosdaq_open, kosdaq_close
            db_url: DB URL (optional, gap 계산 시 전일 데이터 조회용)

        Returns:
            기술지표가 추가된 DataFrame
        """
        # 0. 날짜 타입 통일 (문자열/date/datetime 혼재 방지)
        stock_df['date'] = pd.to_datetime(stock_df['date'])
        if market_df is not None and not market_df.empty:
            market_df['date'] = pd.to_datetime(market_df['date'])

        # 1. 가격 기반 Features
        stock_df = TechnicalCalculator._add_price_features(stock_df)

        # 2. 기술지표 Features (종목별로 계산)
        result_list = []
        for symbol, group in stock_df.groupby('symbol'):
            group = group.sort_values('date').copy()
            group = TechnicalCalculator._add_all_technical_features(group)
            result_list.append(group)

        stock_df = pd.concat(result_list, ignore_index=True)

        # 3. 시장 컨텍스트 Features (있는 경우)
        if market_df is not None and not market_df.empty:
            # 시장 지수 갭 계산 (DB에서 전일 데이터 조회)
            market_df = TechnicalCalculator._calculate_market_gaps(market_df, db_url=db_url)

            # 병합
            stock_df = TechnicalCalculator._merge_market_features(stock_df, market_df)

        return stock_df

    @staticmethod
    def _add_price_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        가격 기반 Feature 계산
        """
        df = df.sort_values(['symbol', 'date']).copy()

        # 전일 종가
        df['prev_close'] = df.groupby('symbol')['close'].shift(1)

        # 갭 비율 (%)
        df['gap_pct'] = (df['open'] - df['prev_close']) / df['prev_close'] * 100

        # 전일 수익률 (%)
        df['prev_return'] = (
            (df['prev_close'] - df.groupby('symbol')['close'].shift(2)) /
            df.groupby('symbol')['close'].shift(2) * 100
        )

        # 전일 고저가 범위 (%)
        prev_high = df.groupby('symbol')['high'].shift(1)
        prev_low = df.groupby('symbol')['low'].shift(1)
        df['prev_range_pct'] = (prev_high - prev_low) / df['prev_close'] * 100

        # 전일 윗꼬리
        prev_open = df.groupby('symbol')['open'].shift(1)
        prev_close_shift = df.groupby('symbol')['close'].shift(1)
        df['prev_upper_shadow'] = (
            (prev_high - pd.concat([prev_open, prev_close_shift], axis=1).max(axis=1)) /
            df['prev_close']
        )

        # 전일 아래꼬리
        df['prev_lower_shadow'] = (
            (pd.concat([prev_open, prev_close_shift], axis=1).min(axis=1) - prev_low) /
            df['prev_close']
        )

        # 거래량 비율 (전일 거래량 / 20일 평균)
        df['avg_volume_20d'] = (
            df.groupby('symbol')['volume']
            .rolling(20, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
        df['volume_ratio'] = df.groupby('symbol')['volume'].shift(1) / df['avg_volume_20d']

        # 임시 컬럼 제거
        df = df.drop(columns=['avg_volume_20d'], errors='ignore')

        return df

    @staticmethod
    def _add_all_technical_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        모든 기술적 지표를 한 번에 추가 (단일 종목용)
        """
        df = TechnicalCalculator._calculate_moving_averages(df)
        df = TechnicalCalculator._calculate_rsi(df)
        df = TechnicalCalculator._calculate_atr(df)
        df = TechnicalCalculator._calculate_bollinger_bands(df)
        df = TechnicalCalculator._calculate_momentum(df)

        return df

    @staticmethod
    def _calculate_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
        """
        이동평균 Features 계산
        """
        # 5일, 20일, 50일 이동평균 (종가 기준)
        df['ma_5'] = df['close'].rolling(window=5, min_periods=1).mean()
        df['ma_20'] = df['close'].rolling(window=20, min_periods=1).mean()
        df['ma_50'] = df['close'].rolling(window=50, min_periods=1).mean()

        # 전일 종가가 이동평균선 위에 있는지 (Look-ahead bias 방지)
        df['above_ma5'] = (df['prev_close'] > df['ma_5'].shift(1)).astype(int)
        df['above_ma20'] = (df['prev_close'] > df['ma_20'].shift(1)).astype(int)
        df['above_ma50'] = (df['prev_close'] > df['ma_50'].shift(1)).astype(int)

        # 5일선이 20일선 위에 있는지 (골든크로스/데드크로스)
        df['ma5_ma20_cross'] = (df['ma_5'].shift(1) > df['ma_20'].shift(1)).astype(int)

        return df

    @staticmethod
    def _calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        RSI (Relative Strength Index) 계산
        """
        # 일간 가격 변화
        delta = df['close'].diff()

        # 상승/하락 분리
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        # 평균 상승/하락폭 (Wilder's smoothing)
        avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

        # RS와 RSI 계산
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        # Look-ahead bias 방지: 전일 RSI 사용
        df['rsi_14'] = rsi.shift(1)

        # RSI 카테고리
        df['rsi_category'] = pd.cut(
            df['rsi_14'],
            bins=[0, 30, 70, 100],
            labels=['oversold', 'neutral', 'overbought'],
            include_lowest=True
        )

        return df

    @staticmethod
    def _calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        ATR (Average True Range) 계산
        """
        # True Range 계산
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift(1))
        low_close = np.abs(df['low'] - df['close'].shift(1))

        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

        # ATR 계산 (Wilder's smoothing)
        atr = true_range.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

        # Look-ahead bias 방지: 전일 ATR 사용
        df['atr_14'] = atr.shift(1)

        # 갭 크기 대비 ATR 비율
        if 'gap_pct' in df.columns and 'prev_close' in df.columns:
            gap_size = np.abs(df['open'] - df['prev_close'])
            df['atr_ratio'] = gap_size / (df['atr_14'] + 1e-10)
        else:
            df['atr_ratio'] = np.nan

        return df

    @staticmethod
    def _calculate_bollinger_bands(df: pd.DataFrame, period: int = 20, std: float = 2.0) -> pd.DataFrame:
        """
        볼린저 밴드 계산
        """
        # 중심선 (20일 이동평균)
        sma = df['close'].rolling(window=period, min_periods=period).mean()

        # 표준편차
        rolling_std = df['close'].rolling(window=period, min_periods=period).std()

        # 상단/하단 밴드
        upper_band = sma + (rolling_std * std)
        lower_band = sma - (rolling_std * std)

        # 볼린저 밴드 값 저장 (당일 값)
        df['bollinger_upper'] = upper_band
        df['bollinger_lower'] = lower_band
        df['bollinger_middle'] = sma

        # 볼린저 밴드 위치 계산 (0~1, 0.5가 중심)
        # Look-ahead bias 방지: 전일 종가와 전일 볼린저 밴드 값 사용
        prev_bollinger_upper = df['bollinger_upper'].shift(1)
        prev_bollinger_lower = df['bollinger_lower'].shift(1)
        
        df['bollinger_position'] = (
            (df['prev_close'] - prev_bollinger_lower) /
            (prev_bollinger_upper - prev_bollinger_lower + 1e-10)
        )

        # 밴드 범위 밖인 경우 클리핑
        df['bollinger_position'] = df['bollinger_position'].clip(0, 1)

        return df

    @staticmethod
    def _calculate_momentum(df: pd.DataFrame) -> pd.DataFrame:
        """
        추세 및 모멘텀 Features 계산
        """
        # 5일 수익률
        df['return_5d'] = ((df['close'] - df['close'].shift(5)) / df['close'].shift(5) * 100)

        # 20일 수익률
        df['return_20d'] = ((df['close'] - df['close'].shift(20)) / df['close'].shift(20) * 100)

        # 연속 상승일 수
        daily_return = df['close'].pct_change()
        is_up = (daily_return > 0).astype(int)

        # 연속 상승일 카운트
        consecutive = (is_up * (is_up.groupby((is_up != is_up.shift()).cumsum()).cumcount() + 1))

        # Look-ahead bias 방지
        df['consecutive_up_days'] = consecutive.shift(1)
        df['return_5d'] = df['return_5d'].shift(1)
        df['return_20d'] = df['return_20d'].shift(1)

        return df

    @staticmethod
    def _calculate_market_gaps(market_df: pd.DataFrame, db_url: Optional[str] = None) -> pd.DataFrame:
        """
        시장 지수 갭 계산

        Args:
            market_df: 시장 지수 DataFrame
            db_url: DB URL (optional, 전일 종가 조회용)
        """
        market_df = market_df.sort_values('date').copy()

        # KOSPI 갭
        if 'kospi_open' in market_df.columns and 'kospi_close' in market_df.columns:
            market_df['kospi_prev_close'] = market_df['kospi_close'].shift(1)

            # 전일 종가가 없으면 (첫 행) DB에서 조회
            if market_df['kospi_prev_close'].isna().any() and db_url:
                market_df = TechnicalCalculator._fill_prev_close_from_db(
                    market_df, db_url, 'kospi_close', 'kospi_prev_close'
                )

            market_df['kospi_gap_pct'] = (
                (market_df['kospi_open'] - market_df['kospi_prev_close']) /
                market_df['kospi_prev_close'] * 100
            )

        # KOSDAQ 갭
        if 'kosdaq_open' in market_df.columns and 'kosdaq_close' in market_df.columns:
            market_df['kosdaq_prev_close'] = market_df['kosdaq_close'].shift(1)

            # 전일 종가가 없으면 (첫 행) DB에서 조회
            if market_df['kosdaq_prev_close'].isna().any() and db_url:
                market_df = TechnicalCalculator._fill_prev_close_from_db(
                    market_df, db_url, 'kosdaq_close', 'kosdaq_prev_close'
                )

            market_df['kosdaq_gap_pct'] = (
                (market_df['kosdaq_open'] - market_df['kosdaq_prev_close']) /
                market_df['kosdaq_prev_close'] * 100
            )

        # KOSPI200 갭
        if 'kospi200_open' in market_df.columns and 'kospi200_close' in market_df.columns:
            market_df['kospi200_prev_close'] = market_df['kospi200_close'].shift(1)

            # 전일 종가가 없으면 (첫 행) DB에서 조회
            if market_df['kospi200_prev_close'].isna().any() and db_url:
                market_df = TechnicalCalculator._fill_prev_close_from_db(
                    market_df, db_url, 'kospi200_close', 'kospi200_prev_close'
                )

            market_df['kospi200_gap_pct'] = (
                (market_df['kospi200_open'] - market_df['kospi200_prev_close']) /
                market_df['kospi200_prev_close'] * 100
            )

        # 임시 컬럼 제거
        market_df = market_df.drop(
            columns=['kospi_prev_close', 'kosdaq_prev_close', 'kospi200_prev_close'],
            errors='ignore'
        )

        return market_df

    @staticmethod
    def _fill_prev_close_from_db(
        market_df: pd.DataFrame,
        db_url: str,
        close_col: str,
        prev_close_col: str
    ) -> pd.DataFrame:
        """
        DB에서 전일 종가를 조회하여 채우기

        Args:
            market_df: 시장 지수 DataFrame
            db_url: DB URL
            close_col: 종가 컬럼명 (예: 'kospi_close')
            prev_close_col: 전일 종가 컬럼명 (예: 'kospi_prev_close')
        """
        from sqlalchemy import create_engine, text

        # NaN인 행만 처리
        missing_rows = market_df[market_df[prev_close_col].isna()].copy()

        if missing_rows.empty:
            return market_df

        engine = create_engine(db_url)

        for idx in missing_rows.index:
            target_date = market_df.loc[idx, 'date']

            # DB에서 해당 날짜 이전의 가장 최근 데이터 조회
            query = text(f"""
                SELECT {close_col}
                FROM market_indices
                WHERE date < :target_date
                ORDER BY date DESC
                LIMIT 1
            """)

            with engine.connect() as conn:
                result = conn.execute(query, {"target_date": target_date})
                row = result.fetchone()

                if row:
                    prev_close = row[0]
                    market_df.loc[idx, prev_close_col] = prev_close
                    # 날짜 타입에 따라 적절히 처리
                    date_str = target_date.date() if hasattr(target_date, 'date') else target_date
                    print(f"  ✓ DB에서 {close_col} 전일 종가 조회: {prev_close} (날짜: {date_str})")

        engine.dispose()
        return market_df

    @staticmethod
    def _merge_market_features(
        stock_df: pd.DataFrame,
        market_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        종목 데이터에 시장 지수 Features 병합
        """
        if market_df.empty:
            stock_df['market_gap_diff'] = np.nan
            return stock_df

        # 날짜를 datetime으로 통일
        stock_df['date'] = pd.to_datetime(stock_df['date'])
        market_df['date'] = pd.to_datetime(market_df['date'])

        # *_gap_pct 컬럼들 찾기
        gap_cols = [c for c in market_df.columns if c.endswith('_gap_pct')]

        # 병합할 컬럼 선택
        merge_cols = ['date'] + gap_cols
        available_cols = [c for c in merge_cols if c in market_df.columns]

        # 병합 (left join)
        result = stock_df.merge(
            market_df[available_cols],
            on='date',
            how='left'
        )

        # KOSPI 대비 상대 갭 계산
        if 'gap_pct' in result.columns and 'kospi_gap_pct' in result.columns:
            result['market_gap_diff'] = result['gap_pct'] - result['kospi_gap_pct']
        else:
            result['market_gap_diff'] = np.nan

        return result

    @staticmethod
    def validate_features(df: pd.DataFrame) -> dict:
        """
        계산된 기술지표 검증
        """
        validation = {
            'total_rows': len(df),
            'null_counts': df.isnull().sum().to_dict(),
            'feature_ranges': {},
            'warnings': []
        }

        # RSI 범위 확인 (0~100)
        if 'rsi_14' in df.columns:
            rsi_min = df['rsi_14'].min()
            rsi_max = df['rsi_14'].max()
            validation['feature_ranges']['rsi_14'] = (rsi_min, rsi_max)

            if rsi_min < 0 or rsi_max > 100:
                validation['warnings'].append(
                    f"RSI 범위 오류: {rsi_min:.2f} ~ {rsi_max:.2f} (정상: 0~100)"
                )

        # Bollinger Position 범위 확인 (0~1)
        if 'bollinger_position' in df.columns:
            bb_min = df['bollinger_position'].min()
            bb_max = df['bollinger_position'].max()
            validation['feature_ranges']['bollinger_position'] = (bb_min, bb_max)

            if bb_min < 0 or bb_max > 1:
                validation['warnings'].append(
                    f"Bollinger Position 범위 오류: {bb_min:.4f} ~ {bb_max:.4f}"
                )

        # Volume Ratio 확인 (양수)
        if 'volume_ratio' in df.columns:
            vol_min = df['volume_ratio'].min()
            validation['feature_ranges']['volume_ratio'] = (vol_min, df['volume_ratio'].max())

            if vol_min < 0:
                validation['warnings'].append(f"Volume Ratio 음수 발견: {vol_min:.4f}")

        # NULL 비율 확인
        for col, null_count in validation['null_counts'].items():
            null_ratio = null_count / len(df) * 100
            if null_ratio > 50:
                validation['warnings'].append(
                    f"{col}: NULL 비율 {null_ratio:.1f}% (> 50%)"
                )

        return validation
