"""
DB 저장 유틸리티

StockPrices, MarketIndices, StockMetadata 테이블 UPSERT
"""

import pandas as pd
from typing import List, Dict, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

# Airflow용 SQLAlchemy 1.4 호환 모델
from utils.models import (
    Base, StockPrices, MarketIndices, StockMetadata, GapPredictions,
    Users, Accounts, UserStrategy, StrategyInfo, StrategyStatus,
    DailyStrategy, DailyStrategyStock, Order, OrderExecution, HourCandleData,
    AccountType
)


class StockDataWriter:
    """
    주식 데이터 DB 저장 유틸리티

    UPSERT (INSERT ON CONFLICT DO UPDATE) 지원
    """

    def __init__(self, db_url: str):
        """
        Args:
            db_url: PostgreSQL 연결 문자열
                   예: postgresql+psycopg2://user:pass@localhost:5432/stock_db
        """
        self.engine = create_engine(db_url, pool_pre_ping=True)
        self.Session = sessionmaker(bind=self.engine)

    def upsert_stock_metadata(self, symbols_data: List[Dict]) -> int:
        """
        StockMetadata 테이블 UPSERT

        Args:
            symbols_data: 종목 메타 정보 리스트
                [
                    {
                        "symbol": "005930",
                        "name": "삼성전자",
                        "exchange": "KOSPI",
                        "sector": "전기전자",
                        "industry": "반도체",
                        "market_cap": 500000000000,
                        "listing_date": "1975-06-11",
                        "status": "ACTIVE"
                    },
                    ...
                ]

        Returns:
            저장된 행 수
        """
        if not symbols_data:
            return 0

        with self.engine.begin() as conn:
            stmt = insert(StockMetadata).values(symbols_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol'],  # symbol이 PK
                set_={
                    'name': stmt.excluded.name,
                    'exchange': stmt.excluded.exchange,
                    'sector': stmt.excluded.sector,
                    'industry': stmt.excluded.industry,
                    'market_cap': stmt.excluded.market_cap,
                    'listing_date': stmt.excluded.listing_date,
                    'status': stmt.excluded.status,
                    'delist_date': stmt.excluded.delist_date,
                    'updated_at': text('CURRENT_TIMESTAMP'),
                }
            )
            result = conn.execute(stmt)
            return result.rowcount

    def upsert_stock_prices(self, prices_df: pd.DataFrame) -> int:
        """
        StockPrices 테이블 UPSERT

        Args:
            prices_df: 가격 + 기술지표 DataFrame
                      필수 컬럼: symbol, date, open, high, low, close, volume
                      기술지표 컬럼: rsi_14, atr_14, ... (25개)

        Returns:
            저장된 행 수
        """
        if prices_df.empty:
            return 0

        # DB 테이블에 존재하는 컬럼만 추출
        db_columns = {c.name for c in StockPrices.__table__.columns}
        excluded_from_insert = {'id', 'created_at', 'updated_at'}
        excluded_from_update = {'id', 'symbol', 'date', 'created_at', 'updated_at'}

        # DataFrame에서 DB 컬럼만 선택
        valid_columns = [col for col in prices_df.columns if col in db_columns - excluded_from_insert]
        filtered_df = prices_df[valid_columns].copy()

        # Integer 범위 초과 값 체크 및 클리핑
        INT_MAX = 2147483647
        INT_MIN = -2147483648
        BIGINT_MAX = 9223372036854775807

        for col in filtered_df.columns:
            if filtered_df[col].dtype in ['int64', 'float64']:
                # 극단적으로 큰 값이 있는지 확인
                col_max = filtered_df[col].max()
                col_min = filtered_df[col].min()
                if pd.notna(col_max) and (col_max > BIGINT_MAX or col_min < -BIGINT_MAX):
                    print(f"  ⚠️ 컬럼 '{col}'에 범위 초과 값 발견: max={col_max}, min={col_min}")
                    # NaN으로 변환
                    filtered_df.loc[filtered_df[col] > BIGINT_MAX, col] = None
                    filtered_df.loc[filtered_df[col] < -BIGINT_MAX, col] = None

        # DataFrame을 딕셔너리 리스트로 변환
        records = filtered_df.to_dict('records')

        # date를 문자열로 변환 (pd.Timestamp → str)
        for record in records:
            if 'date' in record and hasattr(record['date'], 'strftime'):
                record['date'] = record['date'].strftime('%Y-%m-%d')

        try:
            with self.engine.begin() as conn:
                stmt = insert(StockPrices).values(records)

                # DB 테이블에 존재하는 컬럼만 업데이트
                update_dict = {
                    col: getattr(stmt.excluded, col)
                    for col in valid_columns
                    if col not in excluded_from_update
                }
                update_dict['updated_at'] = text('CURRENT_TIMESTAMP')

                stmt = stmt.on_conflict_do_update(
                    constraint='uq_stock_prices_symbol_date',
                    set_=update_dict
                )

                result = conn.execute(stmt)
                return result.rowcount
        except Exception as e:
            # 에러 발생 시 문제 데이터 로깅
            print(f"  ❌ DB Insert 에러: {e}")
            print(f"  📊 데이터 통계:")
            for col in filtered_df.columns:
                if filtered_df[col].dtype in ['int64', 'float64']:
                    print(f"    - {col}: min={filtered_df[col].min()}, max={filtered_df[col].max()}")
            # 첫 번째 레코드 샘플 출력
            if records:
                print(f"  📝 첫 번째 레코드 샘플: {records[0]}")
            raise

    def upsert_market_indices(self, indices_df: pd.DataFrame) -> int:
        """
        MarketIndices 테이블 UPSERT

        Args:
            indices_df: 시장 지수 DataFrame
                       필수 컬럼: date, kospi_open, kospi_close, kosdaq_open, kosdaq_close
                       계산 컬럼: kospi_gap_pct, kosdaq_gap_pct

        Returns:
            저장된 행 수

        Note:
            - date가 Primary Key (id 컬럼 없음)
        """
        if indices_df.empty:
            return 0

        # DB 테이블에 존재하는 컬럼만 추출
        db_columns = {c.name for c in MarketIndices.__table__.columns}
        excluded_from_insert = {'created_at', 'updated_at'}
        excluded_from_update = {'date', 'created_at', 'updated_at'}

        # DataFrame에서 DB 컬럼만 선택
        valid_columns = [col for col in indices_df.columns if col in db_columns - excluded_from_insert]
        filtered_df = indices_df[valid_columns].copy()

        # DataFrame을 딕셔너리 리스트로 변환
        records = filtered_df.to_dict('records')

        # date를 문자열로 변환
        for record in records:
            if 'date' in record and hasattr(record['date'], 'strftime'):
                record['date'] = record['date'].strftime('%Y-%m-%d')

        with self.engine.begin() as conn:
            stmt = insert(MarketIndices).values(records)

            # 모든 컬럼 업데이트 (date, created_at 제외)
            update_dict = {
                col: getattr(stmt.excluded, col)
                for col in valid_columns
                if col not in excluded_from_update
            }
            update_dict['updated_at'] = text('CURRENT_TIMESTAMP')

            stmt = stmt.on_conflict_do_update(
                index_elements=['date'],  # date가 PK
                set_=update_dict
            )

            result = conn.execute(stmt)
            return result.rowcount

    def get_latest_date(self, table: str = 'stock_prices') -> Optional[str]:
        """
        테이블에서 가장 최근 날짜 조회

        Args:
            table: 테이블명 ('stock_prices', 'market_indices')

        Returns:
            최근 날짜 (YYYY-MM-DD) 또는 None
        """
        with self.engine.connect() as conn:
            if table == 'stock_prices':
                query = text("SELECT MAX(date) FROM stock_prices")
            elif table == 'market_indices':
                query = text("SELECT MAX(date) FROM market_indices")
            else:
                raise ValueError(f"Unknown table: {table}")

            result = conn.execute(query).scalar()
            return result.strftime('%Y-%m-%d') if result else None

    def get_symbol_count(self) -> int:
        """
        저장된 종목 수 조회

        Returns:
            종목 수
        """
        with self.engine.connect() as conn:
            query = text("SELECT COUNT(*) FROM stock_metadata WHERE status = 'ACTIVE'")
            return conn.execute(query).scalar() or 0

    def get_date_range(self, symbol: str) -> Optional[tuple]:
        """
        특정 종목의 데이터 날짜 범위 조회

        Args:
            symbol: 종목 코드

        Returns:
            (시작일, 종료일) 튜플 또는 None
        """
        with self.engine.connect() as conn:
            query = text("""
                SELECT MIN(date), MAX(date)
                FROM stock_prices
                WHERE symbol = :symbol
            """)
            result = conn.execute(query, {"symbol": symbol}).fetchone()

            if result and result[0]:
                return (
                    result[0].strftime('%Y-%m-%d'),
                    result[1].strftime('%Y-%m-%d')
                )
            return None

    def vacuum_analyze(self):
        """
        VACUUM ANALYZE 실행 (대량 INSERT 후 성능 최적화)
        """
        with self.engine.connect() as conn:
            # autocommit 모드로 실행
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text("VACUUM ANALYZE stock_prices"))
            conn.execute(text("VACUUM ANALYZE market_indices"))
            conn.execute(text("VACUUM ANALYZE stock_metadata"))

    def create_tables_if_not_exists(self):
        """
        테이블이 없으면 생성 (models.py의 ORM 모델 사용)
        """
        Base.metadata.create_all(self.engine)

    def get_stats(self) -> Dict:
        """
        DB 통계 조회

        Returns:
            통계 딕셔너리
        """
        with self.engine.connect() as conn:
            stats = {}

            # 종목 수
            stats['total_symbols'] = conn.execute(
                text("SELECT COUNT(*) FROM stock_metadata")
            ).scalar() or 0

            stats['active_symbols'] = conn.execute(
                text("SELECT COUNT(*) FROM stock_metadata WHERE status = 'ACTIVE'")
            ).scalar() or 0

            # 가격 데이터
            stats['total_price_records'] = conn.execute(
                text("SELECT COUNT(*) FROM stock_prices")
            ).scalar() or 0

            stats['price_date_range'] = conn.execute(
                text("SELECT MIN(date), MAX(date) FROM stock_prices")
            ).fetchone()

            # 시장 지수 데이터
            stats['total_index_records'] = conn.execute(
                text("SELECT COUNT(*) FROM market_indices")
            ).scalar() or 0

            stats['index_date_range'] = conn.execute(
                text("SELECT MIN(date), MAX(date) FROM market_indices")
            ).fetchone()

            return stats

    def get_prev_close_batch(self, symbols: List[str], target_date=None) -> Dict[str, float]:
        """
        여러 종목의 전일 종가를 일괄 조회

        Args:
            symbols: 종목 코드 리스트
            target_date: 기준일 (기본값: 오늘) - 이 날짜 이전의 가장 최근 종가를 조회

        Returns:
            {symbol: prev_close} 딕셔너리
        """
        if not symbols:
            return {}

        from datetime import date as date_type
        if target_date is None:
            target_date = date_type.today()
        elif isinstance(target_date, str):
            from datetime import datetime
            target_date = datetime.strptime(target_date, '%Y-%m-%d').date()

        with self.engine.connect() as conn:
            # 각 종목별로 target_date 이전의 가장 최근 종가를 조회
            query = text("""
                SELECT DISTINCT ON (symbol) symbol, close
                FROM stock_prices
                WHERE symbol = ANY(:symbols)
                  AND date < :target_date
                ORDER BY symbol, date DESC
            """)
            result = conn.execute(query, {
                "symbols": symbols,
                "target_date": target_date
            }).fetchall()

            return {row[0]: float(row[1]) for row in result}

    def update_prediction_results(
        self,
        predictions: List[Dict],
        actual_prices: Dict[str, Dict[str, float]]
    ) -> int:
        """
        GapPredictions 테이블의 실제 결과 업데이트

        Args:
            predictions: 예측 리스트 (id, stock_code, stock_open, predicted_direction, expected_return, max_return_if_up 포함)
            actual_prices: 실제 가격 딕셔너리 {stock_code: {'close': float, 'high': float, 'low': float}}

        Returns:
            업데이트된 행 수
        """
        if not predictions:
            return 0

        updated_count = 0

        with self.engine.begin() as conn:
            for pred in predictions:
                stock_code = pred['stock_code']

                if stock_code not in actual_prices:
                    continue

                price_data = actual_prices[stock_code]
                stock_open = pred['stock_open']
                actual_close = price_data['close']
                actual_high = price_data['high']
                actual_low = price_data['low']

                # 실제 수익률 계산
                actual_return = ((actual_close - stock_open) / stock_open) * 100
                actual_max_return = ((actual_high - stock_open) / stock_open) * 100

                # 예측 차이 계산
                expected_return = pred['expected_return']
                return_diff = expected_return - actual_return

                max_return_if_up = pred.get('max_return_if_up')
                max_return_diff = None
                if max_return_if_up is not None:
                    max_return_diff = max_return_if_up - actual_max_return

                # 예측 방향 정확도 (1: 맞음, 0: 틀림)
                predicted_direction = pred['predicted_direction']
                actual_direction = 1 if actual_return > 0 else 0
                direction_correct = 1 if predicted_direction == actual_direction else 0

                # 업데이트
                update_query = text("""
                    UPDATE predictions
                    SET actual_close = :actual_close,
                        actual_high = :actual_high,
                        actual_low = :actual_low,
                        actual_return = :actual_return,
                        return_diff = :return_diff,
                        actual_max_return = :actual_max_return,
                        max_return_diff = :max_return_diff,
                        direction_correct = :direction_correct,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                """)

                conn.execute(update_query, {
                    "id": pred['id'],
                    "actual_close": actual_close,
                    "actual_high": actual_high,
                    "actual_low": actual_low,
                    "actual_return": round(actual_return, 2),
                    "return_diff": round(return_diff, 2),
                    "actual_max_return": round(actual_max_return, 2),
                    "max_return_diff": round(max_return_diff, 2) if max_return_diff is not None else None,
                    "direction_correct": direction_correct,
                })

                updated_count += 1

        return updated_count

    def get_active_auto_strategies(self, account_type: str = "mock") -> List[Dict]:
        """
        status=ACTIVE, is_auto=True인 전략 목록 조회

        Args:
            account_type: 계좌 유형 필터 ("mock", "paper", "real")

        Returns:
            전략 정보 리스트
            [
                {
                    "id": 1,
                    "account_id": 1,
                    "strategy_id": 1,
                    "strategy_name": "Gap Trading",
                    "ls_ratio": 0.5,
                    "tp_ratio": 0.3,
                    "account_number": "12345678",
                    "nickname": "user1"
                },
                ...
            ]

        Note:
            - UserStrategy가 user_id 대신 account_id를 FK로 사용하도록 변경됨
            - account_type으로 필터링 (기존 role 대신)
        """
        with self.engine.connect() as conn:
            query = text("""
                SELECT
                    us.id,
                    us.account_id,
                    us.strategy_id,
                    si.name as strategy_name,
                    us.ls_ratio,
                    us.tp_ratio,
                    sw.weight_type as strategy_weight_type,
                    us.investment_weight,
                    a.account_number,
                    a.account_name,
                    u.nickname
                FROM user_strategy us
                JOIN strategy_info si ON us.strategy_id = si.id
                JOIN stock_accounts a ON us.account_id = a.id
                JOIN users u ON a.user_uid = u.uid
                LEFT JOIN strategy_weight sw ON us.weight_type_id = sw.id
                WHERE us.status = 'ACTIVE'
                  AND us.is_auto = true
                  AND (us.is_deleted = false OR us.is_deleted IS NULL)
                  AND a.account_type = :account_type
            """)
            result = conn.execute(query, {"account_type": account_type})
            return [dict(row._mapping) for row in result]

    def get_user_account_credentials(self, user_ids: List[int] = None) -> List[Dict]:
        """
        유저 계좌 정보 조회 (실거래용)

        Args:
            user_ids: 특정 유저 ID 목록 (None이면 전체)

        Returns:
            유저 계좌 정보 리스트
            [
                {
                    "user_id": 1,
                    "nickname": "user1",
                    "account_id": 1,
                    "account_number": "12345678",
                    "account_name": "test",
                    "account_type": "real",
                    "account_balance": 1000000.00,
                    "app_key": "xxx",
                    "app_secret": "yyy",
                    "kis_access_token": "zzz",
                    "kis_token_expired_at": "2026-01-15 10:00:00"
                },
                ...
            ]
        """
        with self.engine.connect() as conn:
            if user_ids:
                query = text("""
                    SELECT
                        u.uid as user_id,
                        u.nickname,
                        a.id as account_id,
                        a.account_number,
                        a.account_name,
                        a.account_type,
                        a.account_balance,
                        a.app_key,
                        a.app_secret,
                        a.kis_access_token,
                        a.kis_token_expired_at
                    FROM users u
                    JOIN stock_accounts a ON u.uid = a.user_uid
                    WHERE u.uid = ANY(:user_ids)
                    ORDER BY u.uid, a.id
                """)
                result = conn.execute(query, {"user_ids": user_ids})
            else:
                query = text("""
                    SELECT
                        u.uid as user_id,
                        u.nickname,
                        a.id as account_id,
                        a.account_number,
                        a.account_name,
                        a.account_type,
                        a.account_balance,
                        a.app_key,
                        a.app_secret,
                        a.kis_access_token,
                        a.kis_token_expired_at
                    FROM users u
                    JOIN stock_accounts a ON u.uid = a.user_uid
                    ORDER BY u.uid, a.id
                """)
                result = conn.execute(query)

            return [dict(row._mapping) for row in result]

    def get_user_strategies_with_accounts(self, account_type: str = None) -> List[Dict]:
        """
        ACTIVE/is_auto=True 전략과 해당 유저의 계좌 정보를 함께 조회 (실거래용)

        Args:
            account_type: 계좌 유형 필터 (None이면 전체, "mock", "paper", "real")

        Returns:
            전략 + 계좌 정보 리스트
            [
                {
                    "strategy_id": 1,
                    "strategy_name": "Gap Trading",
                    "user_strategy_id": 1,
                    "account_id": 1,
                    "user_id": 1,
                    "nickname": "user1",
                    "ls_ratio": 0.5,
                    "tp_ratio": 0.3,
                    "account_number": "12345678",
                    "account_name": "test",
                    "account_type": "real",
                    "account_balance": 1000000.00,
                    "app_key": "xxx",
                    "app_secret": "yyy"
                },
                ...
            ]

        Note:
            - UserStrategy가 user_id 대신 account_id를 FK로 사용
            - user_id 조회는 account → user 관계를 통해
        """
        with self.engine.connect() as conn:
            base_query = """
                SELECT
                    si.id as strategy_id,
                    si.name as strategy_name,
                    us.id as user_strategy_id,
                    us.account_id,
                    a.hts_id,
                    u.uid as user_id,
                    u.nickname,
                    u.role,
                    us.ls_ratio,
                    us.tp_ratio,
                    sw.weight_type as strategy_weight_type,
                    us.investment_weight,
                    a.account_number,
                    a.account_name,
                    a.account_type,
                    a.account_balance,
                    a.app_key,
                    a.app_secret,
                    a.kis_access_token,
                    a.kis_token_expired_at
                FROM user_strategy us
                JOIN strategy_info si ON us.strategy_id = si.id
                JOIN stock_accounts a ON us.account_id = a.id
                JOIN users u ON a.user_uid = u.uid
                LEFT JOIN strategy_weight sw ON us.weight_type_id = sw.id
                WHERE us.status = 'ACTIVE'
                  AND us.is_auto = true
                  AND (us.is_deleted = false OR us.is_deleted IS NULL)
            """

            if account_type:
                query = text(base_query + " AND a.account_type = :account_type ORDER BY us.id, a.id")
                result = conn.execute(query, {"account_type": account_type})
            else:
                query = text(base_query + " ORDER BY us.id, a.id")
                result = conn.execute(query)

            return [dict(row._mapping) for row in result]

    # =====================================================
    # 새로운 테이블용 메서드들
    # =====================================================

    def upsert_daily_strategy(self, daily_strategy_data: Dict) -> int:
        """
        DailyStrategy 테이블 INSERT/UPDATE

        Args:
            daily_strategy_data: 일별 전략 데이터

        Returns:
            생성/수정된 id
        """
        with self.engine.begin() as conn:
            stmt = insert(DailyStrategy).values(daily_strategy_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['id'],
                set_={
                    'buy_amount': stmt.excluded.buy_amount,
                    'sell_amount': stmt.excluded.sell_amount,
                    'total_profit_rate': stmt.excluded.total_profit_rate,
                    'total_profit_amount': stmt.excluded.total_profit_amount,
                    'updated_at': text('CURRENT_TIMESTAMP'),
                }
            )
            result = conn.execute(stmt)
            return result.rowcount

    def upsert_daily_strategy_stock(self, stock_data: List[Dict]) -> int:
        """
        DailyStrategyStock 테이블 UPSERT

        Args:
            stock_data: 일별 전략 종목 데이터 리스트

        Returns:
            저장된 행 수
        """
        if not stock_data:
            return 0

        with self.engine.begin() as conn:
            stmt = insert(DailyStrategyStock).values(stock_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['id'],
                set_={
                    'target_price': stmt.excluded.target_price,
                    'target_quantity': stmt.excluded.target_quantity,
                    'target_sell_price': stmt.excluded.target_sell_price,
                    'stop_loss_price': stmt.excluded.stop_loss_price,
                    'buy_price': stmt.excluded.buy_price,
                    'buy_quantity': stmt.excluded.buy_quantity,
                    'sell_price': stmt.excluded.sell_price,
                    'sell_quantity': stmt.excluded.sell_quantity,
                    'profit_rate': stmt.excluded.profit_rate,
                    'updated_at': text('CURRENT_TIMESTAMP'),
                }
            )
            result = conn.execute(stmt)
            return result.rowcount

    def upsert_order(self, order_data: Dict) -> int:
        """
        Order 테이블 UPSERT

        Args:
            order_data: 주문 데이터

        Returns:
            저장된 행 수
        """
        with self.engine.begin() as conn:
            stmt = insert(Order).values(order_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['order_no'],
                set_={
                    'status': stmt.excluded.status,
                    'total_executed_quantity': stmt.excluded.total_executed_quantity,
                    'total_executed_price': stmt.excluded.total_executed_price,
                    'remaining_quantity': stmt.excluded.remaining_quantity,
                    'is_fully_executed': stmt.excluded.is_fully_executed,
                    'updated_at': text('CURRENT_TIMESTAMP'),
                }
            )
            result = conn.execute(stmt)
            return result.rowcount

    def insert_order_execution(self, execution_data: Dict) -> int:
        """
        OrderExecution 테이블 INSERT

        Args:
            execution_data: 체결 데이터

        Returns:
            생성된 id
        """
        with self.engine.begin() as conn:
            stmt = insert(OrderExecution).values(execution_data)
            result = conn.execute(stmt)
            return result.rowcount

    def upsert_hour_candle(self, candle_data: List[Dict]) -> int:
        """
        HourCandleData 테이블 UPSERT

        Args:
            candle_data: 1시간봉 캔들 데이터 리스트

        Returns:
            저장된 행 수
        """
        if not candle_data:
            return 0

        with self.engine.begin() as conn:
            stmt = insert(HourCandleData).values(candle_data)
            stmt = stmt.on_conflict_do_update(
                constraint='uq_hour_candle_stock_date_hour',
                set_={
                    'open': stmt.excluded.open,
                    'high': stmt.excluded.high,
                    'low': stmt.excluded.low,
                    'close': stmt.excluded.close,
                    'volume': stmt.excluded.volume,
                    'trade_count': stmt.excluded.trade_count,
                    'updated_at': text('CURRENT_TIMESTAMP'),
                }
            )
            result = conn.execute(stmt)
            return result.rowcount

    def update_account_token(
        self,
        account_id: int,
        access_token: str,
        expired_at: 'datetime'
    ) -> bool:
        """
        계좌의 KIS 토큰 정보 업데이트

        Args:
            account_id: 계좌 ID
            access_token: 발급받은 access_token
            expired_at: 토큰 만료 시간 (datetime)

        Returns:
            업데이트 성공 여부
        """
        with self.engine.begin() as conn:
            query = text("""
                UPDATE stock_accounts
                SET kis_access_token = :access_token,
                    kis_token_expired_at = :expired_at,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :account_id
            """)
            result = conn.execute(query, {
                "account_id": account_id,
                "access_token": access_token,
                "expired_at": expired_at
            })
            return result.rowcount > 0
