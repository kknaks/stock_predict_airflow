"""
Stock Data Operators Common Module

공통 함수 및 상수 정의
"""

from airflow.hooks.base import BaseHook


# ============================================================
# RateLimiter 설정
# 한투 API 초당 20건 제한
# 병렬 배치 4개 동시 실행 시 (DAG max_active_tasks=4):
#   각 배치가 독립 프로세스에서 실행되므로 안전 마진 포함
#   20 / 4 = 5건/초 per batch → 4건/초로 여유 확보
# ============================================================
MAX_PARALLEL_BATCHES = 4
RATE_LIMIT_PER_BATCH = (20 // MAX_PARALLEL_BATCHES) - 1  # 4건/초 (안전 마진)


def get_db_url(conn_id: str = 'stock_db') -> str:
    """
    Airflow Connection에서 DB URL 가져오기

    Args:
        conn_id: Airflow connection ID (기본값: stock_db)

    Returns:
        PostgreSQL 연결 문자열
    """
    conn = BaseHook.get_connection(conn_id)
    # psycopg2 드라이버 사용을 위해 URI 직접 생성
    return f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"


def get_kis_credentials(conn_id: str = 'kis_api') -> tuple:
    """
    Airflow Connection에서 한투 API 자격증명 가져오기

    Args:
        conn_id: Airflow connection ID (기본값: kis_api)

    Returns:
        (app_key, app_secret) 튜플
    """
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson
    return extra.get('app_key'), extra.get('app_secret')


def get_krx_service_key(conn_id: str = 'krx_api') -> str:
    """
    Airflow Connection에서 금융위원회 API 서비스 키 가져오기

    Args:
        conn_id: Airflow connection ID (기본값: krx_api)

    Returns:
        service_key
    """
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson
    return extra.get('service_key')
