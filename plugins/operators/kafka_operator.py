"""
Kafka Operators

범용 카프카 발행 Operator

Airflow Connections 사용:
- kafka_default: Kafka 연결
- kis_api: 한국투자증권 API 연결
"""

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults

from utils.common import get_kis_credentials


class KafkaPublishOperator(BaseOperator):
    """
    범용 카프카 메시지 발행 Operator

    Args:
        kafka_topic: 카프카 토픽
        message: 발행할 메시지 (Dict)
        message_key: 메시지 키 (기본값: None, 자동 생성)
        kafka_conn_id: Airflow Kafka connection ID (기본값: kafka_default)
        fallback_to_file: 카프카 실패 시 파일 저장 여부 (기본값: True)
        fallback_dir: 파일 저장 디렉토리 (기본값: /opt/airflow/data/kafka_messages)

    Note:
        message는 Dict 또는 XCom 키를 통해 가져올 수 있습니다.
    """

    template_fields = ('kafka_topic', 'message', 'message_key')

    @apply_defaults
    def __init__(
        self,
        kafka_topic: str,
        message: Optional[Dict[str, Any]] = None,
        message_key: Optional[str] = None,
        kafka_conn_id: str = 'kafka_default',
        fallback_to_file: bool = True,
        fallback_dir: str = '/opt/airflow/data/kafka_messages',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.kafka_topic = kafka_topic
        self.message = message
        self.message_key = message_key
        self.kafka_conn_id = kafka_conn_id
        self.fallback_to_file = fallback_to_file
        self.fallback_dir = fallback_dir

    def execute(self, context):
        print("=" * 80)
        print(f"Kafka Publish (topic: {self.kafka_topic})")
        print("=" * 80)

        if not self.message:
            print("No message to publish")
            return 0

        # 메시지 처리: 문자열인 경우 JSON으로 파싱, 딕셔너리인 경우 그대로 사용
        message = self.message
        if isinstance(message, str):
            try:
                # JSON 문자열인 경우
                message = json.loads(message)
            except json.JSONDecodeError:
                # Python 딕셔너리 문자열인 경우 (Jinja 템플릿으로 변환된 경우)
                import ast
                try:
                    message = ast.literal_eval(message)
                except (ValueError, SyntaxError) as e:
                    raise ValueError(f"Invalid message format: {e}")
        
        if not isinstance(message, dict):
            raise ValueError(f"Message must be dict or JSON string, got {type(message)}")

        # 메시지 키 생성
        key = self.message_key or f"msg_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        print(f"Message key: {key}")
        print(f"Message: {json.dumps(message, ensure_ascii=False, default=str)[:500]}...")

        # 카프카 발행 시도
        try:
            from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook

            producer_hook = KafkaProducerHook(kafka_config_id=self.kafka_conn_id)
            producer = producer_hook.get_producer()

            producer.produce(
                topic=self.kafka_topic,
                key=key.encode('utf-8'),
                value=json.dumps(message, ensure_ascii=False, default=str).encode('utf-8')
            )
            producer.flush()

            print(f"Kafka publish success - topic={self.kafka_topic}")
            return 1

        except ImportError:
            print("apache-airflow-providers-apache-kafka is not installed")
            if self.fallback_to_file:
                return self._save_to_file(context, key)
            raise

        except Exception as e:
            print(f"Kafka publish failed: {str(e)}")
            if self.fallback_to_file:
                return self._save_to_file(context, key)
            raise

    def _save_to_file(self, context, key: str) -> int:
        """카프카 실패 시 파일로 저장"""
        os.makedirs(self.fallback_dir, exist_ok=True)

        execution_date = context['execution_date']
        filename = f"{self.kafka_topic}_{execution_date.strftime('%Y%m%d_%H%M%S')}_{key}.json"
        output_file = os.path.join(self.fallback_dir, filename)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.message, f, ensure_ascii=False, indent=2, default=str)

        print(f"Fallback file saved: {output_file}")
        return 1


class PublishAccountStrategyOperator(BaseOperator):
    """
    UserStrategy 결과를 카프카로 발행하는 Operator

    UserStrategyOperator의 XCom 결과를 받아서 카프카로 발행합니다.
    - is_mock=True: 전략 + 계좌 정보 발행 (토큰 없음, 계좌 웹소켓 연결 안함)
    - is_mock=False: 전략 + 계좌 + 토큰 정보 발행 (계좌 웹소켓 연결)

    Args:
        is_mock: 모의투자 여부 (기본값: True)
        kafka_topic: 카프카 토픽 (기본값: kis_websocket_commands)
        kafka_conn_id: Airflow Kafka connection ID (기본값: kafka_default)
        kis_conn_id: Airflow KIS API connection ID (기본값: kis_api)
        strategy_xcom_task_id: 전략 XCom을 가져올 task_id
        fallback_to_file: 카프카 실패 시 파일 저장 여부

    메시지 형식:
        {
            "command": "START",
            "timestamp": "2026-01-15T09:00:00+09:00",
            "target": "ACCOUNT",
            "config": {
                "env_dv": "real" or "demo",
                "appkey": "xxx",
                "is_mock": true/false,
                "users": [  # 유저별 그룹화된 정보
                    {
                        "user_id": 1,
                        "nickname": "user1",
                        "account": { ... },  # 계좌 정보는 항상 포함, is_mock=False일 때만 토큰 포함
                        "strategies": [...]
                    }
                ],
                "stocks": []
            }
        }
    """

    template_fields = ('is_mock', 'kafka_topic', 'strategy_xcom_task_id')

    @apply_defaults
    def __init__(
        self,
        is_mock: bool = True,
        kafka_topic: str = 'kis_websocket_commands',
        kafka_conn_id: str = 'kafka_default',
        kis_conn_id: str = 'kis_api',
        strategy_xcom_task_id: str = 'user_strategy_create',
        fallback_to_file: bool = True,
        fallback_dir: str = '/opt/airflow/data/kafka_messages',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.is_mock = is_mock
        self.kafka_topic = kafka_topic
        self.kafka_conn_id = kafka_conn_id
        self.kis_conn_id = kis_conn_id
        self.strategy_xcom_task_id = strategy_xcom_task_id
        self.fallback_to_file = fallback_to_file
        self.fallback_dir = fallback_dir

    def execute(self, context):
        print("=" * 80)
        print(f"Publish Account Strategy (Role-based)")
        print("=" * 80)

        # XCom에서 전략 정보 가져오기
        strategies = context['task_instance'].xcom_pull(
            key='user_strategies',
            task_ids=self.strategy_xcom_task_id
        )

        if not strategies:
            print("No strategies found in XCom")
            return 0

        print(f"Loaded {len(strategies)} strategies from XCom")

        # 계좌 정보 가져오기
        accounts = context['task_instance'].xcom_pull(
            key='user_accounts',
            task_ids=self.strategy_xcom_task_id
        )

        if not accounts:
            print("No accounts found in XCom")
            return 0

        # KST 타임스탬프
        kst = timezone(timedelta(hours=9))
        timestamp = datetime.now(kst).isoformat()

        # 기본 앱키 가져오기 (Airflow Connection에서)
        app_key, app_secret = get_kis_credentials(self.kis_conn_id)

        # account_type별로 계좌 분리 - 대소문자 무시
        # MOCK만 mock 그룹, PAPER와 REAL은 real 그룹 (PAPER는 실제 API 사용)
        mock_accounts = [acc for acc in accounts if acc.get('account_type', '').lower() == 'mock']
        real_accounts = [acc for acc in accounts if acc.get('account_type', '').lower() in ('real', 'paper')]

        # 계좌 ID 기준으로 전략 분리
        mock_strategies = [s for s in strategies if s.get('account_id') in {acc.get('account_id') for acc in mock_accounts}]
        real_strategies = [s for s in strategies if s.get('account_id') in {acc.get('account_id') for acc in real_accounts}]

        published_count = 0

        # MOCK/PAPER 계좌 메시지 발행
        if mock_accounts and mock_strategies:
            print(f"\n[MOCK/PAPER Accounts] {len(mock_accounts)} accounts, {len(mock_strategies)} strategies")
            mock_message = self._build_message(timestamp, app_key, mock_strategies, mock_accounts, is_mock=True)
            message_key = f"account_strategy_mock_{timestamp}"
            published_count += self._publish_to_kafka(context, mock_message, message_key)
        else:
            print("\n[MOCK/PAPER Accounts] No accounts or strategies found")

        # REAL 계좌 메시지 발행
        if real_accounts and real_strategies:
            print(f"\n[REAL Accounts] {len(real_accounts)} accounts, {len(real_strategies)} strategies")
            real_message = self._build_message(timestamp, app_key, real_strategies, real_accounts, is_mock=False)
            message_key = f"account_strategy_real_{timestamp}"
            published_count += self._publish_to_kafka(context, real_message, message_key)
        else:
            print("\n[REAL Accounts] No accounts or strategies found")

        return published_count

    def _build_message(
        self,
        timestamp: str,
        app_key: str,
        strategies: List[Dict],
        accounts: List[Dict],
        is_mock: bool
    ) -> Dict:
        """메시지 생성 - 유저별 그룹화"""

        if not accounts:
            print("Warning: No accounts provided")
            accounts = []

        # 계좌별로 전략 그룹화 (account_id 기준)
        strategies_by_account = {}
        for s in strategies:
            account_id = s.get('account_id')
            if account_id not in strategies_by_account:
                strategies_by_account[account_id] = []
            # investment_weight(비중) * account_balance = 실제 투자 금액
            investment_weight_raw = s.get('investment_weight')
            account_balance_raw = s.get('account_balance')

            # None 체크 및 기본값 처리 (investment_weight 기본값: 0.9)
            if investment_weight_raw is None:
                investment_weight = 0.9  # DB 기본값과 동일
            else:
                investment_weight = float(investment_weight_raw)

            account_balance = float(account_balance_raw or 0.0)
            total_investment = investment_weight * account_balance

            # 디버깅 로그
            if total_investment == 0:
                print(f"Warning: total_investment is 0 for strategy {s.get('user_strategy_id')}: "
                      f"investment_weight={investment_weight}, account_balance={account_balance}")

            strategies_by_account[account_id].append({
                "user_strategy_id": s.get('user_strategy_id') or s.get('id'),
                "strategy_id": s.get('strategy_id'),
                "strategy_name": s.get('strategy_name'),
                "strategy_weight_type": s.get('strategy_weight_type'),
                "ls_ratio": s.get('ls_ratio'),
                "tp_ratio": s.get('tp_ratio'),
                "account_id": s.get('account_id'),
                "account_number": s.get('account_number'),
                "total_investment": total_investment,
            })

        # 유저 리스트 생성
        users = []
        for acc in accounts:
            user_id = acc.get('user_id')
            
            # 계좌 정보 구성
            account_data = {
                "account_id": acc.get('account_id'),
                "account_type": acc.get('account_type'),
                "hts_id": acc.get('hts_id'),
                "account_no": acc.get('account_number'),
                "account_product_code": "01",
                "account_balance": acc.get('account_balance'),
                "app_key": acc.get('app_key'),
                "app_secret": acc.get('app_secret'),
            }
            
            # is_mock에 따라 토큰 포함 여부 결정
            if is_mock:
                account_data.update({
                    "access_token": "",
                    "ws_token": "",
                    "expires_in": 0,
                })
            else:
                account_data.update({
                    "access_token": acc.get('access_token', ''),
                    "ws_token": acc.get('ws_token', ''),
                    "expires_in": acc.get('expires_in', 0),
                })
            
            account_id = acc.get('account_id')
            users.append({
                "user_id": user_id,
                "nickname": acc.get('nickname'),
                "account": account_data,
                "strategies": strategies_by_account.get(account_id, [])
            })

        # env_dv 결정
        env_dv = "demo" if is_mock else "real"

        return {
            "command": "START",
            "timestamp": timestamp,
            "target": "ACCOUNT",
            "config": {
                "env_dv": env_dv,
                "appkey": app_key,
                "is_mock": is_mock,
                "users": users,  # 유저별 그룹화된 정보
                "stocks": []
            }
        }

    def _publish_to_kafka(self, context, message: Dict, key: str) -> int:
        """카프카 발행"""
        config = message.get('config', {})
        is_mock = config.get('is_mock')

        print(f"\n[Message to publish]")
        print(f"  - command: {message.get('command')}")
        print(f"  - target: {message.get('target')}")
        print(f"  - is_mock: {is_mock}")

        # Mock/Real 모두 users 형식 사용
        users = config.get('users', [])
        total_strategies = sum(len(u.get('strategies', [])) for u in users)
        print(f"  - users: {len(users)}")
        print(f"  - total strategies: {total_strategies}")
        for u in users:
            print(f"    - {u.get('nickname')}: {len(u.get('strategies', []))} strategies")

        try:
            from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook

            producer_hook = KafkaProducerHook(kafka_config_id=self.kafka_conn_id)
            producer = producer_hook.get_producer()

            producer.produce(
                topic=self.kafka_topic,
                key=key.encode('utf-8'),
                value=json.dumps(message, ensure_ascii=False, default=str).encode('utf-8')
            )
            producer.flush()

            print(f"\nKafka publish success - topic={self.kafka_topic}")
            context['task_instance'].xcom_push(key='published_message', value=message)
            return 1

        except ImportError:
            print("apache-airflow-providers-apache-kafka is not installed")
            if self.fallback_to_file:
                return self._save_to_file(context, message, key)
            raise

        except Exception as e:
            print(f"Kafka publish failed: {str(e)}")
            if self.fallback_to_file:
                return self._save_to_file(context, message, key)
            raise

    def _save_to_file(self, context, message: Dict, key: str) -> int:
        """카프카 실패 시 파일로 저장"""
        os.makedirs(self.fallback_dir, exist_ok=True)

        execution_date = context['execution_date']
        filename = f"account_strategy_{execution_date.strftime('%Y%m%d_%H%M%S')}.json"
        output_file = os.path.join(self.fallback_dir, filename)

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(message, f, ensure_ascii=False, indent=2, default=str)

        print(f"Fallback file saved: {output_file}")
        context['task_instance'].xcom_push(key='published_message', value=message)
        return 1
