"""
User Strategy Operators

사용자 전략 관련 Airflow Operator

Airflow Connections 사용:
- stock_db: PostgreSQL 연결 (Stock Predict Database)
"""

from datetime import datetime, timedelta

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults

from utils.db_writer import StockDataWriter
from utils.api_client import KISAPIClient

def get_db_url(conn_id: str = 'stock_db') -> str:
    """
    Airflow Connection에서 DB URL 가져오기
    """
    conn = BaseHook.get_connection(conn_id)
    return f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"


class UserStrategyOperator(BaseOperator):
    """
    사용자 전략 조회 Operator

    is_mock 여부에 따라 다른 데이터를 조회합니다:
    - is_mock=True: status=ACTIVE, is_auto=True인 전략 목록만 조회 (모의투자용)
    - is_mock=False: 전략 + 유저 계좌 정보(appkey, appsecret 포함) 조회 (실거래용)

    Args:
        is_mock: 모의투자 여부 (기본값: True)
        db_conn_id: Airflow DB connection ID (기본값: stock_db)

    Returns:
        XCom으로 다음 값을 저장합니다:
        - 'user_strategies': 전략 정보 리스트
        - 'user_accounts': 계좌 정보 리스트 (is_mock=False일 때만)

    Note:
        모의투자(is_mock=True)에서는 계좌 정보 없이 전략만 조회하여
        실제 주문 없이 시뮬레이션만 수행합니다.
    """

    template_fields = ('is_mock',)

    @apply_defaults
    def __init__(
        self,
        is_mock=True,
        db_conn_id='stock_db',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.is_mock = is_mock
        self.db_conn_id = db_conn_id

    def execute(self, context):
        mode = "Mock" if self.is_mock else "Real"
        print("=" * 80)
        print(f"User Strategy Load ({mode} Mode)")
        print("=" * 80)

        db_url = get_db_url(self.db_conn_id)
        db_writer = StockDataWriter(db_url)

        # 전략 + 계좌 정보 함께 조회 (공통)
        strategies_with_accounts = db_writer.get_user_strategies_with_accounts()

        print(f"[Strategies with Accounts]")
        print(f"  - Total: {len(strategies_with_accounts)}")

        # 유저별로 계좌 정보 추출 (중복 제거)
        seen_accounts = set()
        user_accounts = []

        for s in strategies_with_accounts:
            if self.is_mock:
                print(f"    - {s['nickname']}: {s['strategy_name']} "
                      f"(ls_ratio={s['ls_ratio']}, tp_ratio={s['tp_ratio']})")
            else:
                print(f"    - {s['nickname']} ({s['account_number']}): {s['strategy_name']} "
                      f"| app_key={s['app_key'][:8]}...")

            # 유저+계좌 조합으로 중복 체크
            key = (s['user_id'], s['account_id'])
            if key not in seen_accounts:
                seen_accounts.add(key)

                # 계좌 정보 구성 (공통)
                role = s.get('role', 'USER')
                account_data = {
                    'user_id': s['user_id'],
                    'nickname': s['nickname'],
                    'role': role,  
                    'hts_id': s['hts_id'],
                    'account_type': s['account_type'],
                    'account_id': s['account_id'],
                    'account_number': s['account_number'],
                    'account_balance': float(s['account_balance']) if s.get('account_balance') else 0.0,
                    'app_key': s['app_key'],
                    'app_secret': s['app_secret'],
                }

                # account_type에 따라 토큰 발급 (PAPER, REAL만 토큰 필요)
                account_type = s.get('account_type', '').upper()
                should_issue_token = account_type in ('PAPER', 'REAL') and not self.is_mock

                if should_issue_token:
                    # 항상 새 토큰 발급 (DAG가 토큰 발급의 주체)
                    kis_client = KISAPIClient(s['app_key'], s['app_secret'])
                    expires_in, ws_token, access_token = kis_client.get_websocket_user_token()

                    # 만료 시간: 23시간 후 (24시간 - 1시간 버퍼)
                    token_expires_at = datetime.now() + timedelta(hours=23)

                    # DB에 토큰 저장 (항상 업데이트)
                    db_writer.update_account_token(s['account_id'], access_token, token_expires_at)

                    account_data.update({
                        'expires_in': expires_in,
                        'ws_token': ws_token,
                        'access_token': access_token,
                    })
                    print(f"      -> Token issued for {s['nickname']} (expires: {token_expires_at})")

                user_accounts.append(account_data)

        # XCom 저장 (공통)
        context['task_instance'].xcom_push(key='user_strategies', value=strategies_with_accounts)
        context['task_instance'].xcom_push(key='user_accounts', value=user_accounts)

        print(f"\n[User Accounts]")
        print(f"  - Total: {len(user_accounts)} (unique)")
        for acc in user_accounts:
            print(f"    - {acc['nickname']} ({acc['account_number']})")

        return len(strategies_with_accounts)
