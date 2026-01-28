# Stock Predict Airflow

KOSPI/KOSDAQ 주식 데이터 수집 및 기술지표 계산 자동화 파이프라인

---

## 📋 개요

- **목적**: 전 종목 OHLCV 데이터 수집 및 기술지표 계산
- **데이터 저장**: PostgreSQL (stock_prices, market_indices, stock_metadata)
- **스케줄**: 평일 18:00 (장 마감 후)
- **API**: 한국투자증권 API (초당 20건 제한)

---

## 🏗️ 프로젝트 구조

```
stock_predict_airflow/
├── docker-compose.yml              # Airflow 컨테이너 설정
├── Dockerfile                      # Custom Airflow 이미지
├── requirements.txt                # Python 의존성
├── .env.example                    # 환경변수 템플릿
├── dags/
│   ├── backfill_stock_data.py      # 과거 데이터 백필
│   └── daily_stock_data.py         # 일일 데이터 수집
├── plugins/
│   └── utils/
│       ├── api_client.py           # KRX/한투 API 클라이언트 (TODO)
│       ├── rate_limiter.py         # API 속도 제한 (20req/s)
│       ├── technical_calculator.py # 기술지표 계산
│       └── db_writer.py            # DB UPSERT 유틸
└── README.md
```

---

## 🚀 빠른 시작

### 1. 환경 설정

```bash
cd /Users/admin/git/study/stock_predict_airflow

# .env 파일 생성
cp .env.example .env

# .env 편집 (DB 정보, API 키 입력)
vi .env
```

**.env 예시**:
```bash
# PostgreSQL (기존 5432 포트)
POSTGRES_HOST=host.docker.internal
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
AIRFLOW_DB=airflow_db
STOCK_DB=stock_predict_db

# 한투 API
KIS_APP_KEY=your_app_key
KIS_APP_SECRET=your_app_secret
```

### 2. Docker Compose 실행

```bash
# Airflow 컨테이너 시작
docker-compose up -d

# 로그 확인
docker-compose logs -f airflow-webserver
```

### 3. Airflow UI 접속

```bash
open http://localhost:8080

# 로그인
ID: admin
PW: admin
```

---

## 📊 DAG 설명

### 1️⃣ backfill_stock_data (백필)

**목적**: 과거 데이터 일괄 수집

**트리거**: Manual (한 번만 실행)

**파라미터**:
```json
{
  "start_date": "2020-01-01",
  "end_date": "2025-12-31"
}
```

**실행 방법**:
1. Airflow UI → DAGs → `backfill_stock_data`
2. Trigger DAG with config 클릭
3. JSON 파라미터 입력 → Trigger

**소요 시간**: 약 30분 ~ 1시간 (종목 수, API 속도에 따라)

---

### 2️⃣ daily_stock_data (일일 수집)

**목적**: 매일 당일 데이터 수집

**스케줄**: `0 18 * * 1-5` (평일 18:00)

**자동 실행**: 평일 장 마감 후 자동

**수동 실행**:
```bash
airflow dags trigger daily_stock_data
```

**소요 시간**: 약 5분

---

## 🔧 TODO: API 구현 필요

현재 API 클라이언트는 **플레이스홀더**입니다. 실제 구현이 필요합니다.

### 1. KRX API (종목 리스트)

**파일**: `plugins/utils/api_client.py`

**구현 필요**:
```python
class KRXAPIClient:
    def get_listed_symbols(self, market="ALL"):
        # TODO: KRX API 호출 로직 구현
        # http://data.krx.co.kr API 사용
        pass
```

### 2. 한국투자증권 API (OHLCV)

**파일**: `plugins/utils/api_client.py`

**구현 필요**:
```python
class KISAPIClient:
    def get_stock_ohlcv(self, symbol, start_date, end_date):
        # TODO: 한투 일봉 조회 API 구현
        # API: /uapi/domestic-stock/v1/quotations/inquire-daily-price
        pass

    def get_market_index(self, index_code, start_date, end_date):
        # TODO: 한투 지수 조회 API 구현
        pass
```

**참고**:
- API 문서: https://apiportal.koreainvestment.com/
- 속도 제한: 초당 20건 (RateLimiter 사용)

---

## 📈 데이터 플로우

```
1. 종목 리스트 조회 (KRX)
   ↓
2. 시장 지수 수집 (KOSPI/KOSDAQ)
   ↓
3. 시장 지수 갭 계산
   ↓
4. MarketIndices 테이블 저장
   ↓
5. 종목별 OHLCV 수집 (병렬, 속도 제한)
   ↓
6. StockPrices 테이블 저장 (원본)
   ↓
7. 기술지표 계산 (RSI, ATR, Bollinger, etc)
   ↓
8. StockPrices 테이블 업데이트 (기술지표)
   ↓
완료
```

---

## 🗄️ DB 테이블

### 1. stock_metadata
- **용도**: 종목 메타 정보
- **PK**: symbol
- **컬럼**: name, exchange, sector, industry, market_cap, status

### 2. stock_prices
- **용도**: 일별 OHLCV + 기술지표
- **Unique**: (symbol, date)
- **컬럼**: 원본 (5개) + 기술지표 (25개)

### 3. market_indices
- **용도**: 시장 지수 (KOSPI/KOSDAQ)
- **PK**: date
- **컬럼**: kospi_open/close, kosdaq_open/close, gap_pct

---

## 🧪 테스트

### DB 데이터 확인

```sql
-- 저장된 종목 수
SELECT COUNT(DISTINCT symbol) FROM stock_metadata WHERE status = 'ACTIVE';

-- 가격 데이터 범위
SELECT MIN(date), MAX(date) FROM stock_prices;

-- 기술지표 계산 확인
SELECT
    COUNT(*) as total,
    COUNT(rsi_14) as rsi_count,
    COUNT(atr_14) as atr_count
FROM stock_prices
WHERE date >= '2025-01-01';

-- 최근 시장 지수
SELECT * FROM market_indices ORDER BY date DESC LIMIT 5;
```

### Airflow UI 확인

- DAG Runs: 모든 태스크 성공 (green)
- Logs: 에러 메시지 없음
- Duration: 예상 시간 내 완료

---

## ⚠️ 주의사항

### API 호출
- 초당 20건 제한 준수 (RateLimiter 사용)
- 네트워크 오류 시 재시도 (retries=3)
- 토큰 만료 시 재발급

### DB 성능
- 대량 INSERT 후 VACUUM ANALYZE 실행
- 인덱스 유지 (symbol + date)
- UPSERT 사용 (중복 방지)

### Docker 메모리
- 최소 4GB RAM 권장
- Worker 동시 실행 제한 (max_active_runs=1)

---

## 📞 문의

문제 발생 시:
1. Airflow UI → DAG → Logs 확인
2. Docker logs: `docker-compose logs -f`
3. DB 연결: `.env` 파일 확인

---

## 📝 변경 이력

| 날짜 | 버전 | 내용 |
|------|------|------|
| 2026-01-12 | 1.0 | 초기 버전 (API TODO) |
