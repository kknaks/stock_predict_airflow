"""
Utils 모듈

Airflow plugins 폴더에서 사용되는 유틸리티 모듈입니다.
Airflow 플러그인 매니저가 직접 이 파일을 로드할 때는 import를 건너뜁니다.
operators에서 `from utils.api_client import ...` 형태로 import할 때 정상 작동합니다.
"""

try:
    from .api_client import KRXAPIClient, KISAPIClient, KISMasterClient
    from .rate_limiter import RateLimiter, BatchRateLimiter
    from .technical_calculator import TechnicalCalculator
    from .db_writer import StockDataWriter

    __all__ = [
        'KRXAPIClient',
        'KISAPIClient',
        'KISMasterClient',
        'RateLimiter',
        'BatchRateLimiter',
        'TechnicalCalculator',
        'StockDataWriter',
    ]
except ImportError:
    # Airflow 플러그인 매니저가 직접 로드할 때는 건너뜀
    pass
