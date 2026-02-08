"""
API 클라이언트 모듈

KRX 및 한국투자증권 API 호출
한투 API 연동 완료 (MCP 기반)

Note:
    Rate Limit은 외부 RateLimiter로 전역 관리
    (다른 서비스들과 함께 초당 20건 제한 공유)
"""

import json
import logging
import os
import ssl
import tempfile
import urllib.request
import zipfile
from typing import List, Dict, Optional, TYPE_CHECKING
from datetime import date, datetime, timedelta
import requests
import pandas as pd

if TYPE_CHECKING:
    from .rate_limiter import RateLimiter

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class KRXAPIClient:
    """
    KRX 상장 종목 조회 클라이언트

    금융위원회 API (data.go.kr) 사용
    """

    # 금융위원회 API
    API_URL = "http://apis.data.go.kr/1160100/service/GetKrxListedInfoService/getItemInfo"

    def __init__(self, service_key: str):
        """
        KRX API 클라이언트 초기화

        Args:
            service_key: 금융위원회 API 서비스 키 (data.go.kr에서 발급)
        """
        self.service_key = service_key

    def get_listed_symbols(
        self,
        market: str = "ALL",  # "KOSPI", "KOSDAQ", "ALL"
        base_date: Optional[datetime] = None
    ) -> List[Dict]:
        """
        KRX 상장 종목 리스트 조회

        Args:
            market: 조회할 시장 (KOSPI, KOSDAQ, ALL)
            base_date: 기준일 (기본값: 오늘)

        Returns:
            종목 리스트
            [
                {
                    "symbol": "005930",
                    "name": "삼성전자",
                    "exchange": "KOSPI",
                    "sector": "전기전자",
                    "market_cap": 500000000000,
                    "listing_date": "1975-06-11",
                    "status": "ACTIVE"
                },
                ...
            ]
        """
        if base_date is None:
            base_date = datetime.now()

        # 휴일 대비 10일 전까지 조회
        start_date = base_date - timedelta(days=10)

        params = {
            "serviceKey": self.service_key,
            "numOfRows": 10000,
            "pageNo": 1,
            "resultType": "json",
            "beginBasDt": start_date.strftime("%Y%m%d"),
            "endBasDt": (base_date + timedelta(days=1)).strftime("%Y%m%d"),
        }

        try:
            response = requests.get(self.API_URL, params=params)
            response.raise_for_status()

            data = response.json()
            items = data["response"]["body"]["items"]["item"]

            # 가장 최근 상장 데이터만 필터링
            items = self._filter_latest_items(items, base_date, start_date)

            # 시장 필터링
            if market != "ALL":
                market_code = "KOSPI" if market == "KOSPI" else "KOSDAQ"
                items = [item for item in items if item.get("mrktCtg") == market_code]

            # 표준 형식으로 변환
            result = []
            for item in items:
                result.append({
                    "symbol": item.get("srtnCd", "")[:6],  # 단축코드 (6자리)
                    "name": item.get("itmsNm", ""),  # 종목명
                    "exchange": item.get("mrktCtg", ""),  # 시장구분 (KOSPI/KOSDAQ)
                    "sector": item.get("idxIndNm", ""),  # 업종명
                    "market_cap": int(item.get("mrktTotAmt", 0)),  # 시가총액
                    "listing_date": self._format_date(item.get("lstgDt", "")),  # 상장일
                    "status": "ACTIVE"
                })

            logger.info("✓ 상장 종목 조회 완료: %d개", len(result))
            return result

        except requests.RequestException as e:
            logger.error("금융위원회 API 요청 실패: %s", str(e))
            raise
        except (KeyError, TypeError) as e:
            logger.error("API 응답 파싱 실패: %s", str(e))
            raise

    def _filter_latest_items(
        self,
        items: List[Dict],
        end_date: datetime,
        start_date: datetime
    ) -> List[Dict]:
        """가장 최근 날짜의 데이터만 필터링"""
        cur_date = end_date

        while cur_date > start_date:
            date_str = cur_date.strftime("%Y%m%d")
            filtered = [item for item in items if item.get("basDt") == date_str]

            if filtered:
                logger.info("기준일: %s (%d개 종목)", date_str, len(filtered))
                return filtered

            cur_date = cur_date - timedelta(days=1)

        return items

    @staticmethod
    def _format_date(date_str: str) -> Optional[str]:
        """날짜 형식 변환 (YYYYMMDD -> YYYY-MM-DD) + 유효성 검사"""
        if len(date_str) == 8:
            try:
                parsed = datetime.strptime(date_str, '%Y%m%d')
                return parsed.strftime('%Y-%m-%d')
            except ValueError:
                return None
        return None


class KISAPIClient:
    """
    한국투자증권 API 클라이언트

    - 접근 토큰은 Airflow Variable에 저장해서 Task 간 공유
    - 일봉 데이터 조회 (종목/지수)
    """

    # 서버 URL
    REAL_SERVER = "https://openapi.koreainvestment.com:9443"
    
    # 토큰 캐시 키 (기본값, 인스턴스별로 app_key 기반 suffix 추가)
    _TOKEN_CACHE_PREFIX = "kis_access_token"
    _TOKEN_EXPIRES_PREFIX = "kis_token_expires_at"
    _WS_TOKEN_CACHE_PREFIX = "kis_ws_token"
    _WS_TOKEN_EXPIRES_PREFIX = "kis_ws_token_expires_at"
    DEMO_SERVER = "https://openapivts.koreainvestment.com:29443"

    # API 엔드포인트
    TOKEN_URL = "/oauth2/tokenP"
    WS_TOKEN_URL = "/oauth2/Approval"
    STOCK_DAILY_URL = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    INDEX_DAILY_URL = "/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice"
    STOCK_PRICE_URL = "/uapi/domestic-stock/v1/quotations/inquire-price"
    INDEX_PRICE_URL = "/uapi/domestic-stock/v1/quotations/inquire-index-price"
    EXP_UPDOWN_URL = "/uapi/domestic-stock/v1/ranking/exp-trans-updown"
    HOLIDAY_URL = "/uapi/domestic-stock/v1/quotations/chk-holiday"
    STOCK_INFO_URL = "/uapi/domestic-stock/v1/quotations/search-stock-info"

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        env: str = "real",  # "real" or "demo"
        rate_limiter: Optional["RateLimiter"] = None
    ):
        """
        한투 API 클라이언트 초기화

        Args:
            app_key: 앱 키
            app_secret: 앱 시크릿
            env: 환경 (real: 실전, demo: 모의)
            rate_limiter: 전역 RateLimiter (다른 서비스와 공유)
        """
        self.app_key = app_key
        self.app_secret = app_secret
        self.env = env
        self.base_url = self.REAL_SERVER if env == "real" else self.DEMO_SERVER
        self.rate_limiter = rate_limiter

        # app_key 기반 캐시 키 (KRX/NXT 분리)
        key_suffix = app_key[-8:] if app_key else ""
        self.TOKEN_CACHE_KEY = f"{self._TOKEN_CACHE_PREFIX}_{key_suffix}"
        self.TOKEN_EXPIRES_KEY = f"{self._TOKEN_EXPIRES_PREFIX}_{key_suffix}"
        self.WS_TOKEN_CACHE_KEY = f"{self._WS_TOKEN_CACHE_PREFIX}_{key_suffix}"
        self.WS_TOKEN_EXPIRES_KEY = f"{self._WS_TOKEN_EXPIRES_PREFIX}_{key_suffix}"

        # 토큰 캐싱 (한 번만 발급)
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._ws_token: Optional[str] = None
        self._ws_token_expires_at: Optional[datetime] = None

    def get_access_token(self) -> str:
        """
        OAuth 액세스 토큰 발급 (Airflow Variable로 Task 간 공유)

        토큰이 없거나 만료됐으면 새로 발급받고, 아니면 캐싱된 토큰 반환

        Returns:
            액세스 토큰
        """
        from airflow.models import Variable
        
        # 1. 먼저 Airflow Variable에서 캐시된 토큰 확인
        cached_token = Variable.get(self.TOKEN_CACHE_KEY, default_var=None)
        cached_expires = Variable.get(self.TOKEN_EXPIRES_KEY, default_var=None)
        
        if cached_token and cached_expires:
            expires_at = datetime.fromisoformat(cached_expires)
            if datetime.now() < expires_at:
                # logger.info("✓ 캐싱된 토큰 사용 (만료: %s)", expires_at)
                self._access_token = cached_token
                self._token_expires_at = expires_at
                return cached_token

        # 2. 새 토큰 발급
        logger.info("접근 토큰 발급 요청...")

        url = f"{self.base_url}{self.TOKEN_URL}"

        headers = {
            "Content-Type": "application/json",
            "Accept": "text/plain",
            "charset": "UTF-8"
        }

        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }

        try:
            response = requests.post(url, data=json.dumps(data), headers=headers)

            if response.status_code == 200:
                result = response.json()
                self._access_token = result.get("access_token")

                # 토큰 만료 시간 설정 (보통 24시간이지만 1시간 전에 갱신)
                expires_in = result.get("expires_in", 86400)  # 기본 24시간
                self._token_expires_at = datetime.now() + timedelta(seconds=expires_in - 3600)

                # 3. Airflow Variable에 토큰 저장 (다른 Task에서도 사용 가능)
                try:
                    Variable.set(self.TOKEN_CACHE_KEY, self._access_token)
                except Exception:
                    Variable.delete(self.TOKEN_CACHE_KEY)
                    Variable.set(self.TOKEN_CACHE_KEY, self._access_token)
                try:
                    Variable.set(self.TOKEN_EXPIRES_KEY, self._token_expires_at.isoformat())
                except Exception:
                    Variable.delete(self.TOKEN_EXPIRES_KEY)
                    Variable.set(self.TOKEN_EXPIRES_KEY, self._token_expires_at.isoformat())

                logger.info("✓ 토큰 발급 성공 (만료: %s)", self._token_expires_at)
                return self._access_token
            else:
                logger.error("토큰 발급 실패: %s - %s", response.status_code, response.text)
                raise Exception(f"토큰 발급 실패: {response.status_code}")

        except requests.RequestException as e:
            logger.error("요청 실패: %s", str(e))
            raise
    def get_access_user_token(self, app_key: str, app_secret: str):
        """
        OAuth 사용자 액세스 토큰 발급 (Airflow Variable로 Task 간 공유)

        토큰이 없거나 만료됐으면 새로 발급받고, 아니면 캐싱된 토큰 반환
        
        Returns:
            사용자 액세스 토큰
        """

        url = f"{self.base_url}{self.TOKEN_URL}"

        headers = {
            "Content-Type": "application/json",
            "Accept": "text/plain",
            "charset": "UTF-8"
        }

        data = {
            "grant_type": "client_credentials",
            "appkey": app_key,
            "appsecret": app_secret,
        }

        try:
            response = requests.post(url, data=json.dumps(data), headers=headers)

            if response.status_code == 200:
                result = response.json()
                access_token = result.get("access_token")

                # 토큰 만료 시간 설정 (보통 24시간이지만 1시간 전에 갱신)
                expires_in = result.get("expires_in", 86400)  # 기본 24시간

                logger.info("✓ 토큰 발급 성공 (만료: %s)", self._token_expires_at)
                return expires_in, access_token
            else:
                logger.error("토큰 발급 실패: %s - %s", response.status_code, response.text)
                raise Exception(f"토큰 발급 실패: {response.status_code}")

        except requests.RequestException as e:
            logger.error("요청 실패: %s", str(e))
            raise

    def get_websocket_token(self) -> str:
        """
        WebSocket 접속키 발급 (Airflow Variable로 Task 간 공유)

        WebSocket 접속키는 별도로 발급받아야 하며, access_token과는 다릅니다.
        토큰이 없거나 만료됐으면 새로 발급받고, 아니면 캐싱된 토큰 반환

        Returns:
            WebSocket 접속키 (approval_key)
        """
        from airflow.models import Variable
        
        # 1. 먼저 Airflow Variable에서 캐시된 토큰 확인
        cached_token = Variable.get(self.WS_TOKEN_CACHE_KEY, default_var=None)
        cached_expires = Variable.get(self.WS_TOKEN_EXPIRES_KEY, default_var=None)
        
        if cached_token and cached_expires:
            expires_at = datetime.fromisoformat(cached_expires)
            if datetime.now() < expires_at:
                logger.info("✓ 캐싱된 웹소켓 토큰 사용 (만료: %s)", expires_at)
                self._ws_token = cached_token
                self._ws_token_expires_at = expires_at
                return cached_token

        # 2. 새 웹소켓 토큰 발급
        logger.info("웹소켓 접속키 발급 요청...")

        url = f"{self.base_url}{self.WS_TOKEN_URL}"

        headers = {
            "Content-Type": "application/json",
            "Accept": "text/plain",
            "charset": "UTF-8"
        }

        # access_token도 함께 전송 (선택사항이지만 권장)
        access_token = self.get_access_token()
        
        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
            "token": access_token,  # access_token 포함
        }

        try:
            response = requests.post(url, data=json.dumps(data), headers=headers)

            if response.status_code == 200:
                result = response.json()
                
                # 응답 형식 확인 (approval_key 또는 다른 필드명일 수 있음)
                ws_token = result.get("approval_key") or result.get("access_token") or result.get("token")
                
                if not ws_token:
                    logger.error("웹소켓 토큰 응답 형식 오류: %s", result)
                    raise Exception("웹소켓 토큰 응답에 approval_key가 없습니다")
                
                self._ws_token = ws_token

                # 토큰 만료 시간 설정 (보통 24시간이지만 1시간 전에 갱신)
                expires_in = result.get("expires_in", 86400)  # 기본 24시간
                self._ws_token_expires_at = datetime.now() + timedelta(seconds=expires_in - 3600)

                # 3. Airflow Variable에 토큰 저장 (다른 Task에서도 사용 가능)
                Variable.set(self.WS_TOKEN_CACHE_KEY, self._ws_token)
                Variable.set(self.WS_TOKEN_EXPIRES_KEY, self._ws_token_expires_at.isoformat())

                logger.info("✓ 웹소켓 접속키 발급 성공 (만료: %s)", self._ws_token_expires_at)
                return self._ws_token
            else:
                logger.error("웹소켓 토큰 발급 실패: %s - %s", response.status_code, response.text)
                raise Exception(f"웹소켓 토큰 발급 실패: {response.status_code}")

        except requests.RequestException as e:
            logger.error("요청 실패: %s", str(e))
            raise
    
    def get_websocket_user_token(self):
        """
        WebSocket 접속키 발급 (캐싱 없이 매번 새로 발급)

        WebSocket 접속키는 별도로 발급받아야 하며, access_token과는 다릅니다.

        Returns:
            (expires_in, ws_token, access_token) 튜플
        """
        # access_token 먼저 발급
        expires_in, access_token = self.get_access_user_token(self.app_key, self.app_secret)

        # WebSocket 토큰 발급
        ws_expires_in, ws_token = self.get_websocket_token_with_access_token(access_token)

        return expires_in, ws_token, access_token

    def get_websocket_token_with_access_token(self, access_token: str):
        """
        이미 발급된 access_token을 사용하여 WebSocket 접속키만 발급

        Args:
            access_token: 이미 발급된 access_token

        Returns:
            (expires_in, ws_token) 튜플
        """
        url = f"{self.base_url}{self.WS_TOKEN_URL}"

        headers = {
            "Content-Type": "application/json",
            "Accept": "text/plain",
            "charset": "UTF-8"
        }

        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
            "token": access_token,
        }

        try:
            response = requests.post(url, data=json.dumps(data), headers=headers)

            if response.status_code == 200:
                result = response.json()

                # 응답 형식 확인 (approval_key 또는 다른 필드명일 수 있음)
                ws_token = result.get("approval_key") or result.get("access_token") or result.get("token")

                if not ws_token:
                    logger.error("웹소켓 토큰 응답 형식 오류: %s", result)
                    raise Exception("웹소켓 토큰 응답에 approval_key가 없습니다")

                # 토큰 만료 시간 설정 (보통 24시간이지만 1시간 전에 갱신)
                expires_in = result.get("expires_in", 86400)  # 기본 24시간

                logger.info("✓ 웹소켓 접속키 발급 성공")
                return expires_in, ws_token
            else:
                logger.error("웹소켓 토큰 발급 실패: %s - %s", response.status_code, response.text)
                raise Exception(f"웹소켓 토큰 발급 실패: {response.status_code}")

        except requests.RequestException as e:
            logger.error("요청 실패: %s", str(e))
            raise

    def _get_headers(self, tr_id: str) -> Dict:
        """API 요청 헤더 생성"""
        return {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.get_access_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }

    def _wait_for_rate_limit(self):
        """전역 RateLimiter 대기 (있는 경우에만)"""
        if self.rate_limiter:
            self.rate_limiter._wait_if_needed()

    def get_stock_ohlcv(
        self,
        symbol: str,
        start_date: str,  # "20200101"
        end_date: str,    # "20251231"
        period: str = "D",  # D (일봉), W (주봉), M (월봉), Y (년봉)
        market_code: str = "J"  # J (KRX), NX (NXT), UN (통합)
    ) -> List[Dict]:
        """
        종목 OHLCV 조회 (일/주/월/년봉)

        Args:
            symbol: 종목 코드 (6자리)
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)
            period: 기간 구분 (D/W/M/Y)

        Returns:
            OHLCV 데이터 리스트
        """
        url = f"{self.base_url}{self.STOCK_DAILY_URL}"
        tr_id = "FHKST03010100"

        params = {
            "FID_COND_MRKT_DIV_CODE": market_code,
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_PERIOD_DIV_CODE": period,
            "FID_ORG_ADJ_PRC": "0",  # 수정주가
        }

        all_data = []
        tr_cont = ""

        while True:
            # 전역 Rate Limiter 대기
            self._wait_for_rate_limit()

            headers = self._get_headers(tr_id)
            if tr_cont:
                headers["tr_cont"] = "N"

            try:
                response = requests.get(url, headers=headers, params=params)

                if response.status_code == 200:
                    result = response.json()

                    if result.get("rt_cd") == "0":  # 성공
                        output2 = result.get("output2", [])

                        for item in output2:
                            all_data.append({
                                "date": self._format_date(item.get("stck_bsop_date", "")),
                                "open": int(item.get("stck_oprc", 0)),
                                "high": int(item.get("stck_hgpr", 0)),
                                "low": int(item.get("stck_lwpr", 0)),
                                "close": int(item.get("stck_clpr", 0)),
                                "volume": int(item.get("acml_vol", 0)),
                            })

                        # 연속 조회 여부 확인
                        tr_cont = response.headers.get("tr_cont", "")
                        if tr_cont not in ["M", "F"]:
                            break
                    else:
                        logger.error(
                            "API 에러 [%s]: %s - %s",
                            symbol,
                            result.get("msg_cd"),
                            result.get("msg1")
                        )
                        break
                else:
                    logger.error("HTTP 에러 [%s]: %s", symbol, response.status_code)
                    break

            except requests.RequestException as e:
                logger.error("요청 실패 [%s]: %s", symbol, str(e))
                break

        return all_data

    def get_market_index(
        self,
        index_code: str,  # "0001" (KOSPI), "1001" (KOSDAQ), "2001" (KOSPI200)
        start_date: str,
        end_date: str,
        period: str = "D"
    ) -> List[Dict]:
        """
        시장 지수 OHLCV 조회 (일/주/월/년봉)

        Args:
            index_code: 지수 코드
                - "0001": KOSPI 종합
                - "1001": KOSDAQ 종합
                - "2001": KOSPI200
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)
            period: 기간 구분 (D/W/M/Y)

        Returns:
            지수 OHLCV 데이터 리스트
        """
        url = f"{self.base_url}{self.INDEX_DAILY_URL}"
        tr_id = "FHKUP03500100"

        params = {
            "FID_COND_MRKT_DIV_CODE": "U",  # 업종
            "FID_INPUT_ISCD": index_code,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_PERIOD_DIV_CODE": period,
        }

        all_data = []
        tr_cont = ""

        while True:
            # 전역 Rate Limiter 대기
            self._wait_for_rate_limit()

            headers = self._get_headers(tr_id)
            if tr_cont:
                headers["tr_cont"] = "N"

            try:
                response = requests.get(url, headers=headers, params=params)

                if response.status_code == 200:
                    result = response.json()

                    if result.get("rt_cd") == "0":  # 성공
                        output2 = result.get("output2", [])

                        for item in output2:
                            all_data.append({
                                "date": self._format_date(item.get("stck_bsop_date", "")),
                                "open": float(item.get("bstp_nmix_oprc", 0)),
                                "high": float(item.get("bstp_nmix_hgpr", 0)),
                                "low": float(item.get("bstp_nmix_lwpr", 0)),
                                "close": float(item.get("bstp_nmix_prpr", 0)),
                                "volume": int(item.get("acml_vol", 0)),
                            })

                        # 연속 조회 여부 확인
                        tr_cont = response.headers.get("tr_cont", "")
                        if tr_cont not in ["M", "F"]:
                            break
                    else:
                        logger.error(
                            "API 에러 [지수 %s]: %s - %s",
                            index_code,
                            result.get("msg_cd"),
                            result.get("msg1")
                        )
                        break
                else:
                    logger.error("HTTP 에러 [지수 %s]: %s", index_code, response.status_code)
                    break

            except requests.RequestException as e:
                logger.error("요청 실패 [지수 %s]: %s", index_code, str(e))
                break

        return all_data

    def get_stock_ohlcv_batch(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str
    ) -> Dict[str, List[Dict]]:
        """
        여러 종목 OHLCV 일괄 조회

        Args:
            symbols: 종목 코드 리스트
            start_date: 시작일
            end_date: 종료일

        Returns:
            종목별 OHLCV 데이터
            {
                "005930": [{...}, {...}],
                "000660": [{...}, {...}],
                ...
            }

        Note:
            Rate Limit은 전역 RateLimiter에서 관리
            (get_stock_ohlcv 내부에서 _wait_for_rate_limit 호출)
        """
        results = {}

        for i, symbol in enumerate(symbols):
            try:
                # Rate Limit은 get_stock_ohlcv 내부에서 처리됨
                data = self.get_stock_ohlcv(symbol, start_date, end_date)
                results[symbol] = data
                logger.info("✓ [%d/%d] %s: %d건", i + 1, len(symbols), symbol, len(data))
            except Exception as e:
                logger.error("✗ [%d/%d] %s: %s", i + 1, len(symbols), symbol, str(e))
                results[symbol] = []

        return results

    @staticmethod
    def _format_date(date_str: str) -> Optional[str]:
        """날짜 형식 변환 (YYYYMMDD -> YYYY-MM-DD) + 유효성 검사"""
        if len(date_str) == 8:
            try:
                parsed = datetime.strptime(date_str, '%Y%m%d')
                return parsed.strftime('%Y-%m-%d')
            except ValueError:
                return None
        return None

    def get_expected_fluctuation_ranking(
        self,
        market: str = "J",  # J: 전체, K: KOSPI, Q: KOSDAQ
        sort_cls: str = "1",  # 1: 상승률, 2: 하락률, 3: 보합
        mkop_cls: str = "0",  # 0: 평균거래량, 1: 거래증가율, 2: 평균거래회전율, 3: 거래금액순, 4: 평균거래금액회전율
        fid_input: str = "0001",  # 0000:전체, 0001:거래소, 1001:코스닥, 2001:코스피200, 4001: KRX100
    ) -> List[Dict]:
        """
        예상체결 상승/하락 순위 조회 (장전 시간외)

        Args:
            market: 시장 구분
                - "J": 전체
                - "K": KOSPI
                - "Q": KOSDAQ
            sort_cls: 정렬 구분
                - "1": 상승률 순
                - "2": 하락률 순
                - "3": 보합
            blng_cls: 순위 구분
                - "0": 평균거래량
                - "1": 거래증가율
                - "2": 평균거래회전율
                - "3": 거래금액순
                - "4": 평균거래금액회전율

        Returns:
            예상체결 상승/하락 종목 리스트
            [
                {
                    "rank": 1,
                    "symbol": "005930",
                    "name": "삼성전자",
                    "current_price": 75000,
                    "change_rate": 3.45,
                    "change_price": 2500,
                    "expected_price": 77500,
                    "expected_volume": 1234567,
                    ...
                },
                ...
            ]

        Note:
            - 이 API는 연속조회(페이지네이션)를 지원하지 않습니다.
            - 한 번에 최대 30건 정도 반환됩니다.
            - 장전 시간외 (08:30~09:00) 에 유효한 데이터입니다.
        """
        url = f"{self.base_url}{self.EXP_UPDOWN_URL}"
        tr_id = "FHPST01820000"

        params = {
            "fid_rank_sort_cls_code": sort_cls,  # 1: 상승률, 2: 하락률
            "fid_cond_mrkt_div_code": market,  # J: 전체
            "fid_cond_scr_div_code": "20182",  # 화면번호 (고정)
            "fid_input_iscd": fid_input,
            "fid_div_cls_code": "0",  # 0: 전체 (고정)
            "fid_aply_rang_prc_1": "",  # 가격대 시작 (빈값: 전체)
            "fid_aply_rang_prc_2": "",  # 가격대 종료 (빈값: 전체)
            "fid_vol_cnt": "",  # 거래량 조건 (빈값: 전체)
            "fid_pbmn": "",  # 거래대금 조건 (빈값: 전체)
            "fid_blng_cls_code": "0",  # 0: 평균거래량
            "fid_mkop_cls_code": mkop_cls,  # 0:장전예상1:장마감예상
        }

        # 전역 Rate Limiter 대기
        self._wait_for_rate_limit()

        headers = self._get_headers(tr_id)

        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                result = response.json()

                if result.get("rt_cd") == "0":  # 성공
                    output = result.get("output", [])

                    ranking_data = []
                    for i, item in enumerate(output, 1):
                        ranking_data.append({
                            "rank": i,
                            "symbol": item.get("stck_shrn_iscd", ""),  # 종목코드
                            "name": item.get("hts_kor_isnm", ""),  # 종목명
                            "current_price": int(item.get("stck_prpr", 0)),  # 현재가
                            "change_sign": item.get("prdy_vrss_sign", ""),  # 대비부호
                            "change_price": int(item.get("prdy_vrss", 0)),  # 전일대비
                            "change_rate": float(item.get("prdy_ctrt", 0)),  # 등락률
                            "expected_price": int(item.get("expt_pric", 0)),  # 예상체결가
                            "expected_change_price": int(item.get("expt_vrss", 0)),  # 예상대비
                            "expected_change_sign": item.get("expt_vrss_sign", ""),  # 예상대비부호
                            "expected_change_rate": float(item.get("expt_ctrt", 0)),  # 예상등락률
                            "expected_volume": int(item.get("expt_vol", 0)),  # 예상거래량
                            "acml_vol": int(item.get("acml_vol", 0)),  # 누적거래량
                            "acml_tr_pbmn": int(item.get("acml_tr_pbmn", 0)),  # 누적거래대금
                        })

                    logger.info("✓ 예상체결 %s 순위 조회 완료: %d건",
                               "상승" if sort_cls == "1" else "하락", len(ranking_data))
                    return ranking_data
                else:
                    logger.error(
                        "API 에러: %s - %s",
                        result.get("msg_cd"),
                        result.get("msg1")
                    )
                    return []
            else:
                logger.error("HTTP 에러: %s", response.status_code)
                return []

        except requests.RequestException as e:
            logger.error("요청 실패: %s", str(e))
            return []

    def get_current_price(
        self,
        symbol: str,
        market_code: str = "J"
    ) -> Optional[Dict]:
        """
        주식 현재가 시세 조회 (단일 종목)

        Args:
            symbol: 종목 코드 (6자리)
            market_code: 시장 구분 코드
                - "J": KRX (정규장, 09:00~15:30)
                - "NX": NXT (08:00~08:30, 09:00~15:30)
                - "UN": 통합 (NXT + KRX 모두)

        Returns:
            현재가 정보 딕셔너리
            {
                "symbol": "005930",
                "name": "삼성전자",
                "current_price": 75000,
                "open_price": 74000,
                "high_price": 76000,
                "low_price": 73500,
                "prev_close": 74500,
                "change_price": 500,
                "change_rate": 0.67,
                "volume": 12345678,
                ...
            }
        """
        url = f"{self.base_url}{self.STOCK_PRICE_URL}"
        tr_id = "FHKST01010100"

        params = {
            "FID_COND_MRKT_DIV_CODE": market_code,
            "FID_INPUT_ISCD": symbol,
        }

        # 전역 Rate Limiter 대기
        self._wait_for_rate_limit()

        headers = self._get_headers(tr_id)

        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                result = response.json()

                if result.get("rt_cd") == "0":  # 성공
                    output = result.get("output", {})

                    return {
                        "symbol": symbol,
                        "market_name": output.get("rprs_mrkt_kor_name", ""),  # 대표 시장명 (KOSPI/KOSDAQ)
                        "current_price": int(output.get("stck_prpr", 0)),  # 현재가
                        "open_price": int(output.get("stck_oprc", 0)),  # 시가
                        "high_price": int(output.get("stck_hgpr", 0)),  # 고가
                        "low_price": int(output.get("stck_lwpr", 0)),  # 저가
                        "base_price": int(output.get("stck_sdpr", 0)),  # 기준가 (가격제한폭 기준, 전일종가는 DB에서 조회)
                        "change_sign": output.get("prdy_vrss_sign", ""),  # 대비부호
                        "change_price": int(output.get("prdy_vrss", 0)),  # 전일대비
                        "change_rate": float(output.get("prdy_ctrt", 0)),  # 등락률
                        "volume": int(output.get("acml_vol", 0)),  # 누적거래량
                        "trade_amount": int(output.get("acml_tr_pbmn", 0)),  # 누적거래대금
                        "market_cap": int(output.get("hts_avls", 0)),  # 시가총액
                        "per": float(output.get("per", 0) or 0),  # PER
                        "pbr": float(output.get("pbr", 0) or 0),  # PBR
                    }
                else:
                    logger.error(
                        "API 에러 [%s]: %s - %s",
                        symbol,
                        result.get("msg_cd"),
                        result.get("msg1")
                    )
                    return None
            else:
                logger.error("HTTP 에러 [%s]: %s", symbol, response.status_code)
                return None

        except requests.RequestException as e:
            logger.error("요청 실패 [%s]: %s", symbol, str(e))
            return None

    def get_current_price_batch(
        self,
        symbols: List[str],
        market_code: str = "J"
    ) -> List[Dict]:
        """
        여러 종목 현재가 일괄 조회

        Args:
            symbols: 종목 코드 리스트
            market_code: 시장 구분 코드
                - "J": KRX (정규장)
                - "NX": NXT
                - "UN": 통합 (NXT + KRX)

        Returns:
            현재가 정보 리스트
        """
        results = []

        for i, symbol in enumerate(symbols):
            try:
                data = self.get_current_price(symbol, market_code=market_code)
                if data:
                    results.append(data)

                if (i + 1) % 10 == 0:
                    logger.info("✓ [%d/%d] 현재가 조회 중...", i + 1, len(symbols))

            except Exception as e:
                logger.error("✗ [%d/%d] %s: %s", i + 1, len(symbols), symbol, str(e))

        logger.info("✓ 현재가 조회 완료: %d/%d건", len(results), len(symbols))
        return results

    def get_index_current_price(
        self,
        index_code: str  # "0001" (KOSPI), "1001" (KOSDAQ), "2001" (KOSPI200)
    ) -> Optional[Dict]:
        """
        국내업종 현재지수 조회

        Args:
            index_code: 지수 코드
                - "0001": KOSPI 종합
                - "1001": KOSDAQ 종합
                - "2001": KOSPI200

        Returns:
            지수 정보 딕셔너리
            {
                "index_code": "0001",
                "index_name": "코스피",
                "current_value": 2550.12,
                "open_value": 2540.00,
                "high_value": 2560.50,
                "low_value": 2535.00,
                "change_value": 15.32,
                "change_rate": 0.60,
                "volume": 500000000,
                ...
            }
        """
        url = f"{self.base_url}{self.INDEX_PRICE_URL}"
        tr_id = "FHPUP02100000"

        params = {
            "FID_COND_MRKT_DIV_CODE": "U",  # 업종
            "FID_INPUT_ISCD": index_code,
        }

        # 전역 Rate Limiter 대기
        self._wait_for_rate_limit()

        headers = self._get_headers(tr_id)

        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                result = response.json()

                if result.get("rt_cd") == "0":  # 성공
                    output = result.get("output", {})

                    # 지수명 매핑
                    index_name_map = {
                        "0001": "KOSPI",
                        "1001": "KOSDAQ", 
                        "2001": "KOSPI200"
                    }

                    return {
                        "index_code": index_code,
                        "index_name": index_name_map.get(index_code, "기타"),
                        "current_value": float(output.get("bstp_nmix_prpr", 0)),  # 현재지수
                        "open_value": float(output.get("bstp_nmix_oprc", 0)),  # 시가
                        "high_value": float(output.get("bstp_nmix_hgpr", 0)),  # 고가
                        "low_value": float(output.get("bstp_nmix_lwpr", 0)),  # 저가
                        "change_sign": output.get("prdy_vrss_sign", ""),  # 대비부호
                        "change_value": float(output.get("bstp_nmix_prdy_vrss", 0)),  # 전일대비
                        "change_rate": float(output.get("bstp_nmix_prdy_ctrt", 0)),  # 등락률
                        "volume": int(output.get("acml_vol", 0)),  # 누적거래량
                        "trade_amount": int(output.get("acml_tr_pbmn", 0)),  # 누적거래대금
                        "up_count": int(output.get("ascn_issu_cnt", 0)),  # 상승종목수
                        "down_count": int(output.get("down_issu_cnt", 0)),  # 하락종목수
                        "unchanged_count": int(output.get("stnr_issu_cnt", 0)),  # 보합종목수
                    }
                else:
                    logger.error(
                        "API 에러 [지수 %s]: %s - %s",
                        index_code,
                        result.get("msg_cd"),
                        result.get("msg1")
                    )
                    return None
            else:
                logger.error("HTTP 에러 [지수 %s]: %s", index_code, response.status_code)
                return None

        except requests.RequestException as e:
            logger.error("요청 실패 [지수 %s]: %s", index_code, str(e))
            return None

    def get_all_index_current_prices(self) -> Dict:
        """
        KOSPI, KOSDAQ, KOSPI200 현재지수 일괄 조회

        Returns:
            지수별 현재가 딕셔너리
            {
                "kospi": {...},
                "kosdaq": {...},
                "kospi200": {...}
            }
        """
        result = {}

        index_map = {
            "0001": "kospi",
            "1001": "kosdaq",
            "2001": "kospi200"
        }

        for code, name in index_map.items():
            data = self.get_index_current_price(code)
            if data:
                result[name] = data

        logger.info("✓ 시장 지수 조회 완료: %d개", len(result))
        return result

    # 휴장일 캐시 키
    HOLIDAY_CACHE_KEY = "kis_holiday_cache"

    def check_holiday(self, base_date: str, use_cache: bool = True) -> Optional[Dict]:
        """
        국내휴장일 조회 (Airflow Variable 캐싱 지원)

        ⚠️ 중요: 이 API는 원장서비스와 연동되어 있어 1일 1회 호출 권장
        → 같은 날짜에 대해 캐시된 결과가 있으면 API 호출 없이 반환

        Args:
            base_date: 기준일자 (YYYYMMDD)
            use_cache: 캐시 사용 여부 (기본값: True)

        Returns:
            휴장일 정보 딕셔너리
            {
                "date": "2026-01-13",
                "day_of_week": "1",  # 1:월 ~ 7:일
                "is_business_day": True,   # 영업일여부
                "is_trading_day": True,    # 거래일여부
                "is_market_open": True,    # 개장일여부 (주문 가능)
                "is_settlement_day": True  # 결제일여부
            }
        """
        from airflow.models import Variable
        import json

        # 1. 캐시 확인 (같은 날짜면 API 호출 생략)
        if use_cache:
            try:
                cached = Variable.get(self.HOLIDAY_CACHE_KEY, default_var=None)
                if cached:
                    cached_data = json.loads(cached)
                    cached_date = cached_data.get("_cached_date")
                    
                    if cached_date == base_date:
                        logger.info("✓ 휴장일 캐시 사용 (날짜: %s)", base_date)
                        # 캐시 메타데이터 제거 후 반환
                        result = {k: v for k, v in cached_data.items() if not k.startswith("_")}
                        return result
            except Exception as e:
                logger.warning("캐시 읽기 실패: %s", str(e))

        # 2. API 호출
        url = f"{self.base_url}{self.HOLIDAY_URL}"
        tr_id = "CTCA0903R"

        params = {
            "BASS_DT": base_date,
            "CTX_AREA_FK": "",
            "CTX_AREA_NK": "",
        }

        # 전역 Rate Limiter 대기
        self._wait_for_rate_limit()

        headers = self._get_headers(tr_id)

        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                result = response.json()

                if result.get("rt_cd") == "0":  # 성공
                    output = result.get("output", [])

                    if not output:
                        logger.warning("휴장일 데이터가 없습니다: %s", base_date)
                        return None

                    # 첫 번째 결과 (해당 날짜)
                    item = output[0] if isinstance(output, list) else output

                    # 날짜 포맷 변환
                    date_str = item.get("bass_dt", base_date)
                    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

                    holiday_info = {
                        "date": formatted_date,
                        "day_of_week": item.get("wday_dvsn_cd", ""),  # 1:월 ~ 7:일
                        "is_business_day": item.get("bzdy_yn", "N") == "Y",
                        "is_trading_day": item.get("tr_day_yn", "N") == "Y",
                        "is_market_open": item.get("opnd_yn", "N") == "Y",  # 주문 가능 여부
                        "is_settlement_day": item.get("sttl_day_yn", "N") == "Y",
                    }

                    # 3. 캐시 저장
                    if use_cache:
                        try:
                            cache_data = {**holiday_info, "_cached_date": base_date}
                            Variable.set(self.HOLIDAY_CACHE_KEY, json.dumps(cache_data))
                            logger.info("✓ 휴장일 캐시 저장 완료 (날짜: %s)", base_date)
                        except Exception as e:
                            logger.warning("캐시 저장 실패: %s", str(e))

                    return holiday_info
                else:
                    logger.error(
                        "API 에러: %s - %s",
                        result.get("msg_cd"),
                        result.get("msg1")
                    )
                    return None
            else:
                logger.error("HTTP 에러: %s", response.status_code)
                return None

        except requests.RequestException as e:
            logger.error("요청 실패: %s", str(e))
            return None

    def get_stock_info(self, symbol: str, prdt_type_cd: str = "300") -> Optional[Dict]:
        """
        주식기본조회 - 종목 상세 정보 조회 (NXT 지원 여부 포함)

        Args:
            symbol: 종목 코드 (6자리)
            prdt_type_cd: 상품유형코드
                - "300": 주식, ETF, ETN, ELW
                - "301": 선물옵션
                - "302": 채권
                - "306": ELS

        Returns:
            종목 정보 딕셔너리
            {
                "symbol": "005930",
                "name": "삼성전자",
                "exchange": "KOSPI",
                "listing_date": "1975-06-11",
                "is_nxt_tradable": True,    # NXT 거래 가능 여부
                "is_nxt_stopped": False,    # NXT 거래정지 여부
                "is_trading_stopped": False, # 일반 거래정지 여부
                "is_managed": False,        # 관리종목 여부
                ...
            }
        """
        url = f"{self.base_url}{self.STOCK_INFO_URL}"
        tr_id = "CTPF1002R"

        params = {
            "PRDT_TYPE_CD": prdt_type_cd,
            "PDNO": symbol,
        }

        # 전역 Rate Limiter 대기
        self._wait_for_rate_limit()

        headers = self._get_headers(tr_id)

        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                result = response.json()

                if result.get("rt_cd") == "0":  # 성공
                    output = result.get("output", {})

                    # 시장 구분 코드 -> 시장명 매핑
                    market_id = output.get("mket_id_cd", "")
                    exchange_map = {
                        "STK": "KOSPI",
                        "KSQ": "KOSDAQ",
                        "KNX": "KONEX",
                    }

                    # 상장일 포맷 변환
                    kospi_listing = output.get("scts_mket_lstg_dt", "")
                    kosdaq_listing = output.get("kosdaq_mket_lstg_dt", "")
                    listing_date = self._format_date(kospi_listing or kosdaq_listing)

                    return {
                        "symbol": symbol,
                        "name": output.get("prdt_name", ""),  # 상품명
                        "name_abbr": output.get("prdt_abrv_name", ""),  # 약어명
                        "name_eng": output.get("prdt_eng_name", ""),  # 영문명
                        "exchange": exchange_map.get(market_id, market_id),
                        "listing_date": listing_date,
                        "par_value": int(output.get("papr", 0) or 0),  # 액면가
                        "listed_shares": int(output.get("lstg_stqt", 0) or 0),  # 상장주수
                        "capital": int(output.get("cpta", 0) or 0),  # 자본금
                        "prev_close": int(output.get("bfdy_clpr", 0) or 0),  # 전일종가
                        "current_close": int(output.get("thdt_clpr", 0) or 0),  # 당일종가
                        # NXT 관련 필드
                        "is_nxt_tradable": output.get("cptt_trad_tr_psbl_yn", "N") == "Y",
                        "is_nxt_stopped": output.get("nxt_tr_stop_yn", "N") == "Y",
                        # 거래 상태 필드
                        "is_trading_stopped": output.get("tr_stop_yn", "N") == "Y",
                        "is_managed": output.get("admn_item_yn", "N") == "Y",  # 관리종목
                        # 기타 분류
                        "is_kospi200": output.get("kospi200_item_yn", "N") == "Y",
                        "is_etf": output.get("etf_dvsn_cd", "") != "",
                        "stock_type": output.get("stck_kind_cd", ""),  # 주식종류코드
                    }
                else:
                    logger.error(
                        "API 에러 [%s]: %s - %s",
                        symbol,
                        result.get("msg_cd"),
                        result.get("msg1")
                    )
                    return None
            else:
                logger.error("HTTP 에러 [%s]: %s", symbol, response.status_code)
                return None

        except requests.RequestException as e:
            logger.error("요청 실패 [%s]: %s", symbol, str(e))
            return None

    def is_nxt_tradable(self, symbol: str) -> bool:
        """
        NXT 거래 가능 여부 확인 (단일 종목)

        NXT 거래 가능 조건:
        - cptt_trad_tr_psbl_yn == 'Y' (NXT 거래종목)
        - nxt_tr_stop_yn == 'N' (NXT 거래정지 아님)

        Args:
            symbol: 종목 코드 (6자리)

        Returns:
            NXT 거래 가능 여부 (True/False)
        """
        info = self.get_stock_info(symbol)
        if info is None:
            return False

        return info.get("is_nxt_tradable", False) and not info.get("is_nxt_stopped", True)

    def get_nxt_tradable_stocks(self, symbols: List[str]) -> List[Dict]:
        """
        NXT 거래 가능 종목 필터링 (여러 종목)

        Args:
            symbols: 종목 코드 리스트

        Returns:
            NXT 거래 가능 종목 정보 리스트
            [
                {
                    "symbol": "005930",
                    "name": "삼성전자",
                    "is_nxt_tradable": True,
                    ...
                },
                ...
            ]
        """
        nxt_tradable = []

        for i, symbol in enumerate(symbols):
            try:
                info = self.get_stock_info(symbol)

                if info and info.get("is_nxt_tradable") and not info.get("is_nxt_stopped"):
                    nxt_tradable.append(info)

                if (i + 1) % 50 == 0:
                    logger.info("✓ [%d/%d] NXT 종목 조회 중... (가능: %d개)",
                               i + 1, len(symbols), len(nxt_tradable))

            except Exception as e:
                logger.error("✗ [%d/%d] %s: %s", i + 1, len(symbols), symbol, str(e))

        logger.info("✓ NXT 거래 가능 종목 조회 완료: %d/%d건", len(nxt_tradable), len(symbols))
        return nxt_tradable


class KISMasterClient:
    """
    한국투자증권 종목 마스터 파일 다운로드 클라이언트

    KRX API 대신 한투에서 제공하는 마스터 파일(mst) 직접 다운로드
    - KOSPI: kospi_code.mst
    - KOSDAQ: kosdaq_code.mst

    Reference:
        https://github.com/koreainvestment/open-trading-api/tree/main/stocks_info
    """

    # 마스터 파일 다운로드 URL
    KOSPI_URL = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
    KOSDAQ_URL = "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"

    def __init__(self):
        """마스터 파일 다운로드 클라이언트 초기화"""
        # SSL 인증서 검증 비활성화 (한투 서버 특성)
        ssl._create_default_https_context = ssl._create_unverified_context

    def get_listed_symbols(self, market: str = "ALL") -> List[Dict]:
        """
        상장 종목 리스트 조회 (마스터 파일 기반)

        Args:
            market: 조회할 시장 ("KOSPI", "KOSDAQ", "ALL")

        Returns:
            종목 리스트
            [
                {
                    "symbol": "005930",
                    "name": "삼성전자",
                    "exchange": "KOSPI",
                    "sector": "전기전자",
                    "market_cap": 500000000000,
                    "listing_date": "1975-06-11",
                    "status": "ACTIVE"
                },
                ...
            ]
        """
        result = []

        if market in ["KOSPI", "ALL"]:
            kospi_df = self._download_and_parse("KOSPI")
            result.extend(self._convert_to_dict_list(kospi_df, "KOSPI"))

        if market in ["KOSDAQ", "ALL"]:
            kosdaq_df = self._download_and_parse("KOSDAQ")
            result.extend(self._convert_to_dict_list(kosdaq_df, "KOSDAQ"))

        logger.info("✓ 마스터 파일에서 종목 조회 완료: %d개", len(result))
        return result

    def _download_and_parse(self, market: str) -> pd.DataFrame:
        """
        마스터 파일 다운로드 및 파싱

        Args:
            market: "KOSPI" or "KOSDAQ"

        Returns:
            파싱된 DataFrame
        """
        url = self.KOSPI_URL if market == "KOSPI" else self.KOSDAQ_URL
        file_prefix = "kospi_code" if market == "KOSPI" else "kosdaq_code"

        with tempfile.TemporaryDirectory() as tmp_dir:
            zip_path = os.path.join(tmp_dir, f"{file_prefix}.zip")
            mst_path = os.path.join(tmp_dir, f"{file_prefix}.mst")

            # 다운로드
            logger.info("마스터 파일 다운로드 중: %s", market)
            urllib.request.urlretrieve(url, zip_path)

            # 압축 해제
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(tmp_dir)

            # 파싱
            df = self._parse_mst_file(mst_path, tmp_dir)

            logger.info("✓ %s 마스터 파일 파싱 완료: %d개 종목", market, len(df))
            return df

    def _parse_mst_file(self, file_path: str, tmp_dir: str) -> pd.DataFrame:
        """
        MST 파일 파싱 (한투 포맷)

        Args:
            file_path: MST 파일 경로
            tmp_dir: 임시 디렉토리

        Returns:
            파싱된 DataFrame
        """
        tmp_fil1 = os.path.join(tmp_dir, "code_part1.tmp")
        tmp_fil2 = os.path.join(tmp_dir, "code_part2.tmp")

        with open(tmp_fil1, mode="w", encoding="utf-8") as wf1, \
             open(tmp_fil2, mode="w", encoding="utf-8") as wf2:

            with open(file_path, mode="r", encoding="cp949") as f:
                for row in f:
                    # Part 1: 단축코드, 표준코드, 한글명
                    rf1 = row[0:len(row) - 228]
                    rf1_1 = rf1[0:9].rstrip()    # 단축코드
                    rf1_2 = rf1[9:21].rstrip()   # 표준코드
                    rf1_3 = rf1[21:].strip()     # 한글명
                    wf1.write(f"{rf1_1},{rf1_2},{rf1_3}\n")

                    # Part 2: 나머지 필드 (228바이트 고정)
                    rf2 = row[-228:]
                    wf2.write(rf2)

        # Part 1 읽기
        part1_columns = ['단축코드', '표준코드', '한글명']
        df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, encoding='utf-8')

        # Part 2 필드 스펙 (고정 폭)
        field_specs = [
            2, 1, 4, 4, 4,    # 그룹코드, 시가총액규모, 지수업종대/중/소분류
            1, 1, 1, 1, 1,    # 제조업~KOSPI50
            1, 1, 1, 1, 1,    # KRX~KRX100
            1, 1, 1, 1, 1,    # KRX자동차~SPAC
            1, 1, 1, 1, 1,    # KRX에너지화학~KRX건설
            1, 1, 1, 1, 1,    # Non1~KRX섹터_운송
            1, 9, 5, 5, 1,    # SRI~거래정지
            1, 1, 2, 1, 1,    # 정리매매~불성실공시
            1, 2, 2, 2, 3,    # 우회상장~증거금비율
            1, 3, 12, 12, 8,  # 신용가능~상장일자
            15, 21, 2, 7, 1,  # 상장주수~우선주
            1, 1, 1, 1, 9,    # 공매도과열~매출액
            9, 9, 5, 9, 8,    # 영업이익~기준년월
            9, 3, 1, 1, 1     # 시가총액~대주가능
        ]

        part2_columns = [
            '그룹코드', '시가총액규모', '지수업종대분류', '지수업종중분류', '지수업종소분류',
            '제조업', '저유동성', '지배구조지수종목', 'KOSPI200섹터업종', 'KOSPI100',
            'KOSPI50', 'KRX', 'ETP', 'ELW발행', 'KRX100',
            'KRX자동차', 'KRX반도체', 'KRX바이오', 'KRX은행', 'SPAC',
            'KRX에너지화학', 'KRX철강', '단기과열', 'KRX미디어통신', 'KRX건설',
            'Non1', 'KRX증권', 'KRX선박', 'KRX섹터_보험', 'KRX섹터_운송',
            'SRI', '기준가', '매매수량단위', '시간외수량단위', '거래정지',
            '정리매매', '관리종목', '시장경고', '경고예고', '불성실공시',
            '우회상장', '락구분', '액면변경', '증자구분', '증거금비율',
            '신용가능', '신용기간', '전일거래량', '액면가', '상장일자',
            '상장주수', '자본금', '결산월', '공모가', '우선주',
            '공매도과열', '이상급등', 'KRX300', 'KOSPI', '매출액',
            '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월',
            '시가총액', '그룹사코드', '회사신용한도초과', '담보대출가능', '대주가능'
        ]

        df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns)

        # 병합
        df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)

        return df

    def _convert_to_dict_list(self, df: pd.DataFrame, exchange: str) -> List[Dict]:
        """
        DataFrame을 표준 딕셔너리 리스트로 변환

        Args:
            df: 마스터 파일 DataFrame
            exchange: 시장 구분 ("KOSPI" or "KOSDAQ")

        Returns:
            표준화된 종목 정보 리스트
        """
        result = []

        for _, row in df.iterrows():
            symbol = str(row['단축코드']).strip().zfill(6)

            # 유효한 종목만 (숫자로 시작하는 6자리 코드)
            if not symbol[:1].isdigit():
                continue

            # 상장일 포맷 변환 (유효성 검사 포함)
            listing_date_raw = str(row.get('상장일자', ''))
            listing_date = None
            if len(listing_date_raw) == 8:
                try:
                    # 날짜 유효성 검사
                    parsed_date = datetime.strptime(listing_date_raw, '%Y%m%d')
                    listing_date = parsed_date.strftime('%Y-%m-%d')
                except ValueError:
                    # 잘못된 날짜는 None 처리
                    logger.warning("잘못된 상장일자: %s (종목: %s)", listing_date_raw, symbol)
                    listing_date = None

            # 시가총액 (억 단위 -> 원 단위)
            market_cap_raw = row.get('시가총액', 0)
            try:
                market_cap = int(float(market_cap_raw)) * 100000000  # 억 -> 원
            except (ValueError, TypeError):
                market_cap = 0

            # 업종 코드 -> 업종명 매핑 (대분류 기준)
            sector = self._get_sector_name(row.get('지수업종대분류', ''))

            # 산업 코드 -> 산업명 매핑 (중분류 기준)
            industry = self._get_industry_name(
                row.get('지수업종대분류', ''),
                row.get('지수업종중분류', '')
            )

            # 거래정지/관리종목 상태 확인
            status = "ACTIVE"
            if row.get('거래정지') == 'Y':
                status = "SUSPENDED"
            elif row.get('관리종목') == 'Y':
                status = "WARNING"

            result.append({
                "symbol": symbol,
                "name": str(row['한글명']).strip(),
                "exchange": exchange,
                "sector": sector,
                "industry": industry,
                "market_cap": market_cap,
                "listing_date": listing_date,
                "status": status
            })

        return result

    @staticmethod
    def _get_sector_name(sector_code: str) -> str:
        """
        업종 코드를 업종명으로 변환

        Args:
            sector_code: 마스터 파일의 지수업종대분류 코드 (1~2자리)

        Returns:
            업종명
        """
        # KOSPI 마스터 파일 지수업종대분류 코드 매핑
        # 실제 마스터 파일에서 사용되는 코드: 0, 16, 17, 18, 19, 20, 21, 26, 27, 28, 29, 30
        sector_map = {
            "0": "기타",           # ETF, 펀드, 리츠 등 비주식 종목
            "16": "섬유의복",
            "17": "전기가스업",
            "18": "건설업",
            "19": "운수창고업",
            "20": "통신업",
            "21": "금융업",        # 은행, 증권, 보험, 지주사 포함
            "26": "서비스업",
            "27": "제조업",        # 음식료, 화학, 의약품, 철강, 기계, 전기전자 등 포함
            "28": "유통업",
            "29": "건설업",        # 건설 관련 추가 코드
            "30": "기타금융",      # 기타 금융 관련
        }

        code = str(sector_code).strip()
        return sector_map.get(code, "기타")

    @staticmethod
    def _get_industry_name(sector_code: str, industry_code: str) -> str:
        """
        중분류 코드를 산업명으로 변환

        Args:
            sector_code: 마스터 파일의 지수업종대분류 코드 (1~2자리)
            industry_code: 마스터 파일의 지수업종중분류 코드 (1~2자리)

        Returns:
            산업명
        """
        sector = str(sector_code).strip()
        industry = str(industry_code).strip()

        # 대분류 27(제조업)의 중분류
        if sector == "27":
            industry_map = {
                "0": "기타제조업",
                "5": "음식료품",
                "6": "섬유의복",
                "7": "종이목재",
                "8": "화학",
                "9": "의약품",
                "10": "비금속광물",
                "11": "철강금속",
                "12": "기계",
                "13": "전기전자",
                "14": "의료정밀",
                "15": "운수장비",
            }
            return industry_map.get(industry, "기타제조업")

        # 대분류 21(금융업)의 중분류
        elif sector == "21":
            industry_map = {
                "0": "지주회사",
                "24": "증권",
                "25": "보험",
            }
            return industry_map.get(industry, "금융업")

        # 대분류 16(섬유의복) - 중분류 없음
        elif sector == "16":
            return "섬유의복"

        # 대분류 17(전기가스업) - 중분류 없음
        elif sector == "17":
            return "전기가스"

        # 대분류 18(건설업) - 중분류 없음
        elif sector == "18":
            return "건설"

        # 대분류 19(운수창고업) - 중분류 없음
        elif sector == "19":
            return "운수창고"

        # 대분류 20(통신업) - 중분류 없음
        elif sector == "20":
            return "통신"

        # 대분류 26(서비스업) - 중분류 없음
        elif sector == "26":
            return "서비스"

        # 대분류 28(유통업) - 중분류 없음
        elif sector == "28":
            return "유통"

        # 대분류 29(건설) - 중분류 없음
        elif sector == "29":
            return "건설"

        # 대분류 30(기타금융) - 중분류 없음
        elif sector == "30":
            return "기타금융"

        # 대분류 0 또는 기타 (ETF, 펀드, 리츠 등)
        else:
            return None
