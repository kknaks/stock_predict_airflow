"""
API 호출 속도 제한 모듈

초당 최대 호출 횟수를 제한하여 API rate limit 준수
"""

import time
import asyncio
from typing import Callable, Any
from functools import wraps


class RateLimiter:
    """
    API 호출 속도 제한 (Token Bucket 알고리즘)

    초당 최대 호출 횟수를 제한
    """

    def __init__(self, max_calls: int = 20, time_window: float = 1.0):
        """
        Args:
            max_calls: 시간 창 내 최대 호출 횟수 (기본 20)
            time_window: 시간 창 (초, 기본 1.0초)
        """
        self.max_calls = max_calls
        self.time_window = time_window
        self.call_times = []

    def __call__(self, func: Callable) -> Callable:
        """
        동기 함수용 데코레이터

        Usage:
            rate_limiter = RateLimiter(max_calls=20)

            @rate_limiter
            def api_call(symbol):
                return requests.get(f"/api/{symbol}")
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            self._wait_if_needed()
            return func(*args, **kwargs)
        return wrapper

    def __call_async__(self, func: Callable) -> Callable:
        """
        비동기 함수용 데코레이터

        Usage:
            rate_limiter = RateLimiter(max_calls=20)

            @rate_limiter.__call_async__
            async def api_call(symbol):
                return await httpx.get(f"/api/{symbol}")
        """
        @wraps(func)
        async def wrapper(*args, **kwargs):
            await self._wait_if_needed_async()
            return await func(*args, **kwargs)
        return wrapper

    def _wait_if_needed(self):
        """
        필요 시 대기 (동기)

        시간 창 내 호출 횟수가 max_calls 이상이면 대기
        """
        now = time.time()

        # 시간 창 밖의 호출 기록 제거
        self.call_times = [t for t in self.call_times if now - t < self.time_window]

        # 호출 횟수 초과 시 대기
        if len(self.call_times) >= self.max_calls:
            sleep_time = self.time_window - (now - self.call_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
                # 대기 후 다시 정리
                now = time.time()
                self.call_times = [t for t in self.call_times if now - t < self.time_window]

        # 호출 시각 기록
        self.call_times.append(time.time())

    async def _wait_if_needed_async(self):
        """
        필요 시 대기 (비동기)
        """
        now = time.time()

        # 시간 창 밖의 호출 기록 제거
        self.call_times = [t for t in self.call_times if now - t < self.time_window]

        # 호출 횟수 초과 시 대기
        if len(self.call_times) >= self.max_calls:
            sleep_time = self.time_window - (now - self.call_times[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
                # 대기 후 다시 정리
                now = time.time()
                self.call_times = [t for t in self.call_times if now - t < self.time_window]

        # 호출 시각 기록
        self.call_times.append(time.time())


class BatchRateLimiter:
    """
    배치 API 호출 속도 제한

    여러 요청을 배치로 묶어서 제한
    """

    def __init__(
        self,
        max_calls_per_second: int = 20,
        batch_size: int = 100
    ):
        """
        Args:
            max_calls_per_second: 초당 최대 호출 횟수
            batch_size: 배치 크기 (한 번에 처리할 항목 수)
        """
        self.rate_limiter = RateLimiter(max_calls=max_calls_per_second)
        self.batch_size = batch_size

    def process_batch(
        self,
        items: list,
        process_func: Callable[[Any], Any]
    ) -> list:
        """
        배치 단위로 항목 처리

        Args:
            items: 처리할 항목 리스트
            process_func: 각 항목에 적용할 함수

        Returns:
            처리 결과 리스트

        Example:
            limiter = BatchRateLimiter(max_calls_per_second=20, batch_size=100)
            symbols = ["005930", "000660", ...]

            def fetch_stock_data(symbol):
                return api_client.get_stock_ohlcv(symbol, ...)

            results = limiter.process_batch(symbols, fetch_stock_data)
        """
        results = []

        for i in range(0, len(items), self.batch_size):
            batch = items[i:i + self.batch_size]

            for item in batch:
                self.rate_limiter._wait_if_needed()
                result = process_func(item)
                results.append(result)

        return results

    async def process_batch_async(
        self,
        items: list,
        process_func: Callable[[Any], Any]
    ) -> list:
        """
        배치 단위로 항목 처리 (비동기)

        Args:
            items: 처리할 항목 리스트
            process_func: 각 항목에 적용할 비동기 함수

        Returns:
            처리 결과 리스트
        """
        results = []

        for i in range(0, len(items), self.batch_size):
            batch = items[i:i + self.batch_size]

            for item in batch:
                await self.rate_limiter._wait_if_needed_async()
                result = await process_func(item)
                results.append(result)

        return results
