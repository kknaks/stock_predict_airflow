# WebSocket NXT/KRX 분리 명세

## 개요

NXT 시장(08:00 개장)과 KRX 시장(09:00 개장)을 별도 WebSocket 커넥션으로 관리하기 위해
Airflow에서 PRICE START 명령을 2회 분리 발행한다.

- WebSocket 서버는 **1개**, 내부에서 NXT/KRX **멀티 커넥션** 관리
- PRICE START: **2번 발행** (NXT 07:50, KRX 08:50)
- ACCOUNT START: 기존 유지 (07:50에 1번)
- KIS API 키: `kis_api` (KRX), `kis_api_nxt` (NXT) — **별도 키 사용**

---

## 타임라인

```
07:50 KST ─── websocket_nxt_start DAG ───┐
              │                           │
              ├─ PRICE START (NXT)        │  ← kis_api_nxt 키 사용
              ├─ UserStrategy 조회        │
              └─ ACCOUNT START 발행       │
                                          │
08:00 KST ─── NXT 시장 개장 ─────────────┘
              │
08:50 KST ─── websocket_krx_start DAG ───┐
              │                           │
              └─ PRICE START (KRX)        │  ← kis_api 키 사용
                                          │
09:00 KST ─── KRX 시장 개장 ─────────────┘
              │
20:01 KST ─── websocket_end DAG
              └─ STOP ALL                    ← NXT/KRX 둘 다 종료
```

---

## Kafka 메시지 스펙

### 토픽: `kis_websocket_commands`

#### 1) NXT PRICE START (07:50)

```json
{
  "command": "START",
  "timestamp": "2025-01-01T07:50:00",
  "target": "PRICE",
  "exchange_type": "NXT",
  "tokens": {
    "access_token": "...",
    "token_type": "Bearer",
    "expires_in": 86400,
    "ws_token": "...",
    "ws_expires_in": 86400
  },
  "config": {
    "env_dv": "real",
    "appkey": "<kis_api_nxt의 app_key>",
    "accounts": [],
    "stocks": ["005930"]
  }
}
```

- **Kafka key**: `websocket_PRICE_NXT`

#### 2) KRX PRICE START (08:50)

```json
{
  "command": "START",
  "timestamp": "2025-01-01T08:50:00",
  "target": "PRICE",
  "exchange_type": "KRX",
  "tokens": {
    "access_token": "...",
    "token_type": "Bearer",
    "expires_in": 86400,
    "ws_token": "...",
    "ws_expires_in": 86400
  },
  "config": {
    "env_dv": "real",
    "appkey": "<kis_api의 app_key>",
    "accounts": [],
    "stocks": ["005930"]
  }
}
```

- **Kafka key**: `websocket_PRICE_KRX`

#### 3) STOP ALL (20:01) — 변경 없음

```json
{
  "command": "STOP",
  "timestamp": "2025-01-01T20:00:00+09:00",
  "target": "ALL"
}
```

---

## 메시지 변경점 (기존 대비)

| 필드 | 기존 | 변경 후 |
|------|------|---------|
| `exchange_type` | 없음 | `"NXT"` 또는 `"KRX"` (신규 필드) |
| Kafka key | `websocket_PRICE` | `websocket_PRICE_NXT` / `websocket_PRICE_KRX` |
| `config.appkey` | 단일 키 | NXT/KRX 별도 키 |
| `tokens` | 단일 토큰 | NXT/KRX 별도 토큰 |

**`exchange_type` 필드가 없는 경우**: 기존 호환성을 위해 `None`으로 처리 가능 (ACCOUNT START 등)

---

## WebSocket 서버 처리 가이드

### Consumer 수신 시 분기 로직

```python
message = consume_from_kafka("kis_websocket_commands")

if message["command"] == "START":
    target = message["target"]
    exchange_type = message.get("exchange_type")  # "NXT" | "KRX" | None

    if target == "PRICE":
        if exchange_type == "NXT":
            # NXT 전용 WebSocket 커넥션 생성
            # - message["tokens"]로 NXT 인증
            # - message["config"]["appkey"]는 NXT 전용 키
            start_nxt_price_connection(message)

        elif exchange_type == "KRX":
            # KRX 전용 WebSocket 커넥션 생성
            # - message["tokens"]로 KRX 인증
            # - message["config"]["appkey"]는 KRX 전용 키
            start_krx_price_connection(message)

    elif target == "ACCOUNT":
        # 기존과 동일 (exchange_type 없음)
        start_account_connection(message)

elif message["command"] == "STOP":
    if message["target"] == "ALL":
        # NXT + KRX 모두 종료
        stop_all_connections()
```

### 핵심 포인트

1. **NXT/KRX는 별도 KIS API 키**를 사용하므로 각각 독립된 WebSocket 커넥션 필요
2. **PRICE START가 2번** 들어옴 — `exchange_type`으로 구분
3. **ACCOUNT START는 1번** (07:50, `exchange_type` 없음) — 기존과 동일
4. **STOP ALL은 1번** — NXT/KRX 커넥션 모두 종료
5. 종목 구독 시 NXT 종목은 NXT 커넥션으로, KRX 종목은 KRX 커넥션으로 라우팅

---

## KIS API 키 매핑

| Connection ID | 용도 | 사용 DAG |
|--------------|------|----------|
| `kis_api` | KRX 전용 | `websocket_krx_start` |
| `kis_api_nxt` | NXT 전용 | `websocket_nxt_start` |
