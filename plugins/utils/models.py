"""
Airflow용 SQLAlchemy 1.4 호환 모델

Airflow 2.8.x는 SQLAlchemy 1.4.x를 사용하므로
SQLAlchemy 2.0 스타일 (mapped_column, DeclarativeBase)을 사용할 수 없습니다.

이 모델은 stock_predict_database의 스키마와 동일하게 유지해야 합니다.
"""

from datetime import datetime, date
import enum
from sqlalchemy import (
    Column, Integer, BigInteger, String, Float, Date, DateTime,
    Enum, UniqueConstraint, Index, Text, Boolean, ForeignKey, Numeric
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

Base = declarative_base()


# =====================================================
# Enum 정의
# =====================================================
class Exchange(str, enum.Enum):
    """거래소"""
    KOSPI = "KOSPI"
    KOSDAQ = "KOSDAQ"


class StockStatus(str, enum.Enum):
    """종목 상태"""
    ACTIVE = "ACTIVE"
    DELISTED = "DELISTED"
    SUSPENDED = "SUSPENDED"
    WARNING = "WARNING"


class StrategyStatus(str, enum.Enum):
    """전략 상태"""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    PAUSED = "PAUSED"


class WeightType(str, enum.Enum):
    """전략 가중치 타입"""
    EQUAL = "equal"
    MARKETCAP = "marketcap"
    VOLUME = "volume"
    PRICE = "price"


class UserRole(str, enum.Enum):
    """사용자 역할"""
    MASTER = "master"
    USER = "user"
    MOCK = "mock"


class AccountType(str, enum.Enum):
    """계좌 유형"""
    REAL = "real"
    PAPER = "paper"
    MOCK = "mock"


class SignalType(str, enum.Enum):
    """매매 신호"""
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"


class ConfidenceLevel(str, enum.Enum):
    """신뢰도"""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class OrderStatus(str, enum.Enum):
    """주문 상태"""
    ORDERED = "ordered"
    PARTIALLY_EXECUTED = "partially_executed"
    EXECUTED = "executed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class OrderType(str, enum.Enum):
    """주문 유형"""
    BUY = "BUY"
    SELL = "SELL"


class TimestampMixin:
    """생성/수정 시간 자동 관리 Mixin"""
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)


# =====================================================
# Stock 관련 테이블
# =====================================================
class StockMetadata(Base, TimestampMixin):
    """
    종목 메타 정보 테이블

    stock_predict_database/database/stocks.py와 동기화 유지 필요!
    - symbol이 Primary Key
    """
    __tablename__ = 'stock_metadata'

    # Primary Key - 종목 코드
    symbol = Column(String(20), primary_key=True)
    name = Column(String(100), nullable=False)
    exchange = Column(Enum(Exchange), nullable=False)
    sector = Column(String(100), nullable=True)
    industry = Column(String(100), nullable=True)
    market_cap = Column(Numeric(20, 2), nullable=True)
    listing_date = Column(Date, nullable=True)
    status = Column(Enum(StockStatus), default=StockStatus.ACTIVE, nullable=False)
    delist_date = Column(Date, nullable=True)

    # Relationship: 1:N (StockMetadata : StockPrices)
    prices = relationship("StockPrices", back_populates="stock", cascade="all, delete-orphan")


class StockPrices(Base, TimestampMixin):
    """
    주식 가격 + 기술지표 테이블

    stock_predict_database/database/stocks.py와 동기화 유지 필요!
    """
    __tablename__ = 'stock_prices'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(
        String(20),
        ForeignKey("stock_metadata.symbol", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    date = Column(Date, nullable=False, index=True)

    # Relationship: N:1 (StockPrices : StockMetadata)
    stock = relationship("StockMetadata", back_populates="prices")

    # =====================================================
    # OHLCV (원본 데이터)
    # =====================================================
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)

    # =====================================================
    # 가격 기반 Features (미리 계산)
    # =====================================================
    prev_close = Column(Float, nullable=True)           # 전일 종가
    gap_pct = Column(Float, nullable=True)              # 갭 비율 (%)
    prev_return = Column(Float, nullable=True)          # 전일 수익률 (%)
    prev_range_pct = Column(Float, nullable=True)       # 전일 고저가 범위 (%)
    prev_upper_shadow = Column(Float, nullable=True)    # 전일 윗꼬리
    prev_lower_shadow = Column(Float, nullable=True)    # 전일 아래꼬리
    volume_ratio = Column(Float, nullable=True)         # 거래량 비율 (전일/20일평균)

    # =====================================================
    # 기술적 지표 Features (미리 계산)
    # =====================================================
    rsi_14 = Column(Float, nullable=True)               # RSI 14일
    atr_14 = Column(Float, nullable=True)               # ATR 14일
    atr_ratio = Column(Float, nullable=True)            # ATR 비율 (갭/ATR)

    # 볼린저밴드
    bollinger_position = Column(Float, nullable=True)   # 볼린저밴드 위치 (0~1)
    bollinger_upper = Column(Float, nullable=True)
    bollinger_middle = Column(Float, nullable=True)
    bollinger_lower = Column(Float, nullable=True)

    # 이동평균선
    ma_5 = Column(Float, nullable=True)
    ma_20 = Column(Float, nullable=True)
    ma_50 = Column(Float, nullable=True)
    above_ma5 = Column(Float, nullable=True)            # 이평선 위/아래 (0 or 1)
    above_ma20 = Column(Float, nullable=True)
    above_ma50 = Column(Float, nullable=True)
    ma5_ma20_cross = Column(Float, nullable=True)       # 골든/데드크로스

    # 수익률
    return_5d = Column(Float, nullable=True)            # 5일 수익률 (%)
    return_20d = Column(Float, nullable=True)           # 20일 수익률 (%)
    consecutive_up_days = Column(Float, nullable=True)  # 연속 상승일

    # =====================================================
    # 시장 컨텍스트 Features
    # =====================================================
    market_gap_diff = Column(Float, nullable=True)      # 시장 대비 갭 차이

    __table_args__ = (
        UniqueConstraint('symbol', 'date', name='uq_stock_prices_symbol_date'),
        Index('idx_stock_prices_symbol_date', 'symbol', 'date'),
        Index('idx_stock_prices_date', 'date'),
    )


class MarketIndices(Base, TimestampMixin):
    """
    시장 지수 테이블 (KOSPI/KOSDAQ)

    stock_predict_database/database/stocks.py와 동기화 유지 필요!
    - date가 Primary Key
    """
    __tablename__ = 'market_indices'

    # Primary Key - 날짜
    date = Column(Date, primary_key=True)

    # KOSPI
    kospi_open = Column(Float, nullable=True)
    kospi_high = Column(Float, nullable=True)
    kospi_low = Column(Float, nullable=True)
    kospi_close = Column(Float, nullable=True)
    kospi_volume = Column(BigInteger, nullable=True)
    kospi_gap_pct = Column(Float, nullable=True)

    # KOSDAQ
    kosdaq_open = Column(Float, nullable=True)
    kosdaq_high = Column(Float, nullable=True)
    kosdaq_low = Column(Float, nullable=True)
    kosdaq_close = Column(Float, nullable=True)
    kosdaq_volume = Column(BigInteger, nullable=True)
    kosdaq_gap_pct = Column(Float, nullable=True)

    # KOSPI200 (선택)
    kospi200_open = Column(Float, nullable=True)
    kospi200_high = Column(Float, nullable=True)
    kospi200_low = Column(Float, nullable=True)
    kospi200_close = Column(Float, nullable=True)
    kospi200_volume = Column(BigInteger, nullable=True)
    kospi200_gap_pct = Column(Float, nullable=True)

    __table_args__ = (
        Index('idx_market_indices_date', 'date'),
    )


# =====================================================
# 사용자 & 계좌 테이블
# stock_predict_database/database/users.py와 동기화 유지 필요!
# =====================================================
class Users(Base, TimestampMixin):
    """사용자 테이블"""
    __tablename__ = "users"

    uid = Column(BigInteger, primary_key=True, autoincrement=True)
    nickname = Column(String(50), nullable=False, unique=True)
    password_hash = Column(String(64), nullable=False)
    role = Column(Enum(UserRole), nullable=False, default=UserRole.USER)
    refresh_token = Column(Text, nullable=True)
    access_token = Column(Text, nullable=True)

    # 관계: 유저 1명 → 계좌 여러개
    accounts = relationship("Accounts", back_populates="user", cascade="all, delete-orphan")


class Accounts(Base, TimestampMixin):
    """주식 계좌 테이블"""
    __tablename__ = "stock_accounts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_uid = Column(BigInteger, ForeignKey("users.uid", ondelete="CASCADE"), nullable=False)

    # 계좌 기본 정보
    account_name = Column(String(50), nullable=True, default="test")
    account_type = Column(Enum(AccountType), nullable=False, default=AccountType.REAL)
    hts_id = Column(String(50), nullable=True, unique=True)
    account_number = Column(String(50), nullable=False, unique=True)
    account_balance = Column(Numeric(20, 2), nullable=False, default=0)

    is_deleted = Column(Boolean, nullable=False, default=False)

    # 한국투자증권 API 키
    app_key = Column(String(100), nullable=False)
    app_secret = Column(String(200), nullable=False)
    kis_access_token = Column(Text, nullable=True)
    kis_token_expired_at = Column(DateTime(timezone=True), nullable=True)

    # 관계
    user = relationship("Users", back_populates="accounts")
    user_strategies = relationship("UserStrategy", back_populates="account", cascade="all, delete-orphan")


# =====================================================
# 전략 테이블
# stock_predict_database/database/strategy.py와 동기화 유지 필요!
# =====================================================
class StrategyInfo(Base, TimestampMixin):
    """전략 정보 테이블"""
    __tablename__ = "strategy_info"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)

    # 관계
    user_strategies = relationship("UserStrategy", back_populates="strategy_info")
    gap_predictions = relationship("GapPredictions", back_populates="strategy_info")


class StrategyWeightType(Base, TimestampMixin):
    """전략 가중치 타입 테이블"""
    __tablename__ = "strategy_weight"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    weight_type = Column(Enum(WeightType), nullable=False)
    description = Column(Text, nullable=True)

    # 관계
    user_strategies = relationship("UserStrategy", back_populates="strategy_weight_type")


class GapPredictions(Base, TimestampMixin):
    """
    갭 예측 결과 테이블

    stock_predict_database/database/strategy.py와 동기화 유지 필요!
    """
    __tablename__ = 'predictions'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # 원본 정보
    timestamp = Column(Date, nullable=False)
    stock_code = Column(String(20), nullable=False, index=True)
    stock_name = Column(String(100), nullable=False)
    exchange = Column(Enum(Exchange, create_type=False), nullable=True)
    prediction_date = Column(Date, nullable=False, index=True)

    # 전략 정보 (어떤 전략으로 예측했는지)
    strategy_id = Column(
        BigInteger,
        ForeignKey("strategy_info.id", ondelete="SET NULL"),
        nullable=True
    )

    # Relationship: N:1 (GapPredictions : StrategyInfo)
    strategy_info = relationship("StrategyInfo", back_populates="gap_predictions")

    # 입력 데이터
    gap_rate = Column(Float, nullable=False)
    stock_open = Column(Float, nullable=False)

    # 예측 결과
    prob_up = Column(Float, nullable=False)
    prob_down = Column(Float, nullable=False)
    predicted_direction = Column(Integer, nullable=False)
    expected_return = Column(Float, nullable=False)
    return_if_up = Column(Float, nullable=False)
    return_if_down = Column(Float, nullable=False)

    # 고가 예측 (선택)
    max_return_if_up = Column(Float, nullable=True)
    take_profit_target = Column(Float, nullable=True)

    # 매매 신호
    signal = Column(Enum(SignalType), nullable=False, default=SignalType.HOLD)

    # 메타 정보
    model_version = Column(String(20), nullable=False, default='v1.0')
    confidence = Column(Enum(ConfidenceLevel), nullable=True)

    # 실제 결과 (Airflow에서 장 마감 후 업데이트)
    actual_close = Column(Float, nullable=True)
    actual_high = Column(Float, nullable=True)
    actual_low = Column(Float, nullable=True)

    # 예측 vs 실제 비교
    actual_return = Column(Float, nullable=True)
    return_diff = Column(Float, nullable=True)
    actual_max_return = Column(Float, nullable=True)
    max_return_diff = Column(Float, nullable=True)
    direction_correct = Column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint('stock_code', 'prediction_date', name='uq_gap_predictions_stock_date'),
        Index('idx_gap_predictions_stock_code', 'stock_code'),
        Index('idx_gap_predictions_date', 'prediction_date'),
        Index('idx_gap_predictions_signal', 'signal'),
    )


class UserStrategy(Base, TimestampMixin):
    """
    사용자 전략 테이블

    주의: user_id가 아닌 account_id를 FK로 사용 (스키마 변경됨)
    """
    __tablename__ = "user_strategy"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # FK가 user_id → account_id로 변경됨!
    account_id = Column(
        BigInteger,
        ForeignKey("stock_accounts.id", ondelete="CASCADE"),
        nullable=False
    )
    strategy_id = Column(BigInteger, ForeignKey("strategy_info.id"), nullable=False)

    investment_weight = Column(Float, nullable=True, default=0.9)
    ls_ratio = Column(Float, nullable=False, default=0.0)
    tp_ratio = Column(Float, nullable=False, default=0.0)
    is_auto = Column(Boolean, nullable=True, default=False)
    weight_type_id = Column(BigInteger, ForeignKey("strategy_weight.id"), nullable=True)
    status = Column(Enum(StrategyStatus), nullable=True, default=StrategyStatus.ACTIVE)
    is_deleted = Column(Boolean, nullable=True, default=False)

    # 관계
    strategy_info = relationship("StrategyInfo", back_populates="user_strategies", uselist=False)
    account = relationship("Accounts", back_populates="user_strategies")
    strategy_weight_type = relationship("StrategyWeightType", back_populates="user_strategies", uselist=False)
    daily_strategies = relationship("DailyStrategy", back_populates="user_strategy", cascade="all, delete-orphan")


class DailyStrategy(Base, TimestampMixin):
    """일별 전략 실행 기록"""
    __tablename__ = "daily_strategy"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_strategy_id = Column(BigInteger, ForeignKey("user_strategy.id"), nullable=False)

    buy_amount = Column(Float, nullable=True)
    sell_amount = Column(Float, nullable=True)
    total_profit_rate = Column(Float, nullable=True)
    total_profit_amount = Column(Float, nullable=True)
    timestamp = Column(DateTime(timezone=True), nullable=False)

    # 관계
    user_strategy = relationship("UserStrategy", back_populates="daily_strategies")
    stocks = relationship("DailyStrategyStock", back_populates="daily_strategy", cascade="all, delete-orphan")


class DailyStrategyStock(Base, TimestampMixin):
    """일별 전략별 종목 데이터"""
    __tablename__ = "daily_strategy_stock"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    daily_strategy_id = Column(BigInteger, ForeignKey("daily_strategy.id", ondelete="CASCADE"), nullable=False)

    stock_code = Column(String(20), nullable=False)
    stock_name = Column(String(100), nullable=False)
    exchange = Column(String(20), nullable=True)
    stock_open = Column(Float, nullable=False)

    # 목표 정보
    target_price = Column(Float, nullable=True)
    target_quantity = Column(Integer, nullable=True)
    target_sell_price = Column(Float, nullable=True)
    stop_loss_price = Column(Float, nullable=True)

    # 실제 거래 정보 (장 마감 후 업데이트)
    buy_price = Column(Float, nullable=True)
    buy_quantity = Column(Float, nullable=True)
    sell_price = Column(Float, nullable=True)
    sell_quantity = Column(Float, nullable=True)
    profit_rate = Column(Float, nullable=True)

    # 관계
    daily_strategy = relationship("DailyStrategy", back_populates="stocks")
    orders = relationship("Order", back_populates="daily_strategy_stock", cascade="all, delete-orphan")


class Order(Base, TimestampMixin):
    """주문 내역 테이블"""
    __tablename__ = "order"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    daily_strategy_stock_id = Column(
        BigInteger,
        ForeignKey("daily_strategy_stock.id", ondelete="CASCADE"),
        nullable=False
    )

    order_no = Column(String(50), nullable=False, unique=True, index=True)
    order_type = Column(Enum(OrderType), nullable=False)
    order_quantity = Column(Integer, nullable=False)
    order_price = Column(Float, nullable=False)
    order_dvsn = Column(String(10), nullable=False)  # 00: 지정가, 01: 시장가 등
    account_no = Column(String(50), nullable=False)
    is_mock = Column(Boolean, nullable=False, default=False)
    status = Column(Enum(OrderStatus), nullable=False, default=OrderStatus.ORDERED)

    # 누적 체결 정보
    total_executed_quantity = Column(Integer, nullable=False, default=0)
    total_executed_price = Column(Float, nullable=False, default=0.0)
    remaining_quantity = Column(Integer, nullable=False)
    is_fully_executed = Column(Boolean, nullable=False, default=False)

    # 주문 시각
    ordered_at = Column(DateTime(timezone=True), nullable=False)

    # 관계
    daily_strategy_stock = relationship("DailyStrategyStock", back_populates="orders")
    executions = relationship("OrderExecution", back_populates="order", cascade="all, delete-orphan")


class OrderExecution(Base, TimestampMixin):
    """체결통보 내역 테이블 (건별)"""
    __tablename__ = "order_execution"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    order_id = Column(BigInteger, ForeignKey("order.id", ondelete="CASCADE"), nullable=False, index=True)

    execution_sequence = Column(Integer, nullable=False)
    executed_quantity = Column(Integer, nullable=False)
    executed_price = Column(Float, nullable=False)

    # 체결 시점의 누적 정보
    total_executed_quantity_after = Column(Integer, nullable=False)
    total_executed_price_after = Column(Float, nullable=False)
    remaining_quantity_after = Column(Integer, nullable=False)
    is_fully_executed_after = Column(Boolean, nullable=False, default=False)

    # 체결 시각
    executed_at = Column(DateTime(timezone=True), nullable=False)

    # 관계
    order = relationship("Order", back_populates="executions")


class HourCandleData(Base, TimestampMixin):
    """1시간봉 캔들 데이터"""
    __tablename__ = "hour_candle_data"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False)
    candle_date = Column(Date, nullable=False)
    hour = Column(Integer, nullable=False)  # 9, 10, 11, 12, 13, 14, 15

    # OHLCV
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False, default=0)
    trade_count = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint('stock_code', 'candle_date', 'hour', name='uq_hour_candle_stock_date_hour'),
        Index('idx_hour_candle_stock_code', 'stock_code'),
        Index('idx_hour_candle_date', 'candle_date'),
        Index('idx_hour_candle_stock_date', 'stock_code', 'candle_date'),
    )
