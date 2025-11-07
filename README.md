# Binance Options Collector V2 - Compact Version

## Overview

This is the **simplest** and **most efficient** version of the collector.

### Key Insight

Binance's `BTC@markPrice` stream is **ONE** stream that broadcasts mark prices for **ALL** BTC options simultaneously. No need to:
- Call REST API to get option list
- Create multiple WebSocket connections
- Subscribe to individual symbols

Just connect to `BTC@markPrice` and receive everything!

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Binance WebSocket                                          │
│  wss://nbstream.binance.com/eoptions/stream                 │
│  ?streams=BTC@markPrice                                     │
│                                                             │
│  Broadcasts ALL BTC option mark prices every few seconds   │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ Single WebSocket connection
                   ▼
        ┌──────────────────────┐
        │   Collector          │
        │  - Parse symbol      │
        │  - Extract metadata  │
        │  - Push to Redis     │
        └──────────┬───────────┘
                   │
                   │ Redis Streams
                   ▼
        ┌──────────────────────┐
        │   Redis              │
        │  Stream buffer       │
        └──────────┬───────────┘
                   │
                   │ Consumer group
                   ▼
        ┌──────────────────────┐
        │   Persister          │
        │  - Read from Redis   │
        │  - Write Parquet     │
        │  - Auto-cleanup      │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │   data/              │
        │  - BTC-251226-*.parq │
        │  - One file per opt  │
        └──────────────────────┘
```

---

## Quick Start

### **Start the System**

```bash
cd v2_markprice

# Build and start
docker-compose up -d

# View logs
docker-compose logs -f

# Check status
docker-compose ps
```

### **Monitor Collection**

```bash
# Check Redis stream length
docker-compose exec redis redis-cli XLEN binance:options:markprice

# View collector logs
docker-compose logs -f collector

# View persister logs
docker-compose logs -f persister

# Check files being created
ls -lh data/
```

### **Stop the System**

```bash
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

---

## Data Format

### **WebSocket Message Format**

The `BTC@markPrice` stream sends messages like this:

```json
{
  "stream": "BTC@markPrice",
  "data": [
    {
      "e": "markPrice",
      "E": 1762422727047,
      "s": "BTC-251226-100000-C",
      "mp": "8637.4"
    },
    {
      "e": "markPrice",
      "E": 1762422727047,
      "s": "BTC-251226-100000-P",
      "mp": "5069.9"
    },
    ...
  ]
}
```

**Key points:**
- `data` is an **ARRAY** of mark prices (not a single object)
- Broadcasts mark prices for ALL BTC options at once
- Updates every few seconds
- Symbol format: `BTC-YYMMDD-STRIKE-TYPE`

### **Parquet File Schema**

Each `data/SYMBOL.parquet` file contains:

| Column | Type | Description |
|--------|------|-------------|
| timestamp | timestamp[ms] | Collection time (UTC) |
| symbol | string | Option symbol |
| mark_price | float64 | Mark price (fair value) |
| strike_price | int32 | Strike price |
| option_type | string | CALL or PUT |
| expiry_date | date32 | Expiration date |

---

## Analysis Example

### **Read Data**

```python
import pandas as pd
import glob

# Read a specific option
df = pd.read_parquet('data/BTC-251226-100000-C.parquet')
print(df.head())
print(f"Total records: {len(df)}")

# Read all options
files = glob.glob('data/*.parquet')
all_data = []

for f in files:
    df = pd.read_parquet(f)
    latest = df.iloc[-1]  # Get latest mark price
    all_data.append(latest)

df_all = pd.DataFrame(all_data)
print(f"\nTotal options tracked: {len(df_all)}")
```

### **Put/Call Analysis**

```python
# Assume BTC price is ~$103,000
btc_price = 103000

# Filter OTM options
otm_calls = df_all[
    (df_all['option_type'] == 'CALL') &
    (df_all['strike_price'] > btc_price)
]

otm_puts = df_all[
    (df_all['option_type'] == 'PUT') &
    (df_all['strike_price'] < btc_price)
]

# Calculate average premiums
call_avg = otm_calls['mark_price'].mean()
put_avg = otm_puts['mark_price'].mean()

print(f"OTM Call Avg Premium: ${call_avg:.2f}")
print(f"OTM Put Avg Premium: ${put_avg:.2f}")
print(f"Difference: ${call_avg - put_avg:.2f}")

if call_avg > put_avg:
    print("→ BULLISH sentiment")
else:
    print("→ BEARISH sentiment")
```

---

## Configuration

Edit `docker-compose.yml`:

```yaml
environment:
  - BATCH_SIZE=100          # Messages per batch
  - FLUSH_INTERVAL=10       # Seconds between flushes
  - REDIS_STREAM_KEY=...    # Change stream key if needed
```

---

## Troubleshooting

### **No data being collected**

```bash
# Check collector logs
docker-compose logs --tail=100 collector

# Should see:
# "✓ Connected to BTC@markPrice stream"
# "Messages: 100 | Marks processed: 5000+ | Unique symbols: 500+"
```

### **Files not being created**

```bash
# Check persister logs
docker-compose logs --tail=100 persister

# Check Redis has messages
docker-compose exec redis redis-cli XLEN binance:options:markprice

# Should be > 0
```

### **Redis connection errors**

```bash
# Test Redis
docker-compose exec redis redis-cli ping
# Should return: PONG

# Restart services
docker-compose restart collector persister
```

---

## Resource Usage

**Much more efficient than v2_markprice_all:**

| Metric | v2_markprice_all | v2_markprice (this) |
|--------|-----------------|---------------------|
| WebSocket connections | 3 | 1 |
| API calls | Every startup | None |
| Memory | ~500MB | ~200MB |
| CPU | ~15% | ~5% |

---

## Comparison: V1 vs V2

| Feature | V1 (Trade) | V2 (MarkPrice Compact) |
|---------|-----------|----------------------|
| Stream Type | @trade | @markPrice |
| Data Collected | Trade executions | Mark price (fair value) |
| WebSocket Connections | 1 combined | 1 single stream |
| API Calls | Daily | None |
| Options Monitored | Nearest expiry only | ALL options |
| File Structure | `data/YYYYMMDD/SYMBOL.parquet` | `data/SYMBOL.parquet` |
| Best For | Trade flow analysis | Sentiment / premium analysis |

---

## Cost Estimate

**Google Cloud e2-micro** (0.25-2 vCPU, 1GB RAM):
- Handles 1 WebSocket + data processing
- **~$7.50/month** (regular)
- **~$2.25/month** (spot instance)

**Storage:**
- ~50MB/day (compressed)
- ~1.5GB/month
- **~$0.06/month**

**Total: ~$2-8/month**

---

## Next Steps

1. **Validate data collection** - Let it run for a few hours
2. **Build analysis scripts** - Put/Call ratio, premium comparisons
3. **Add monitoring** - Grafana dashboard for Redis/file metrics
4. **Scale if needed** - Add more persister workers for high throughput


네, 맞습니다. **해당 스크립트가 안정적으로 작동하게 하려면 버킷 주소를 바꾸는 것이 첫 번째 단계입니다.**

하지만 `cron`으로 이 스크립트를 실행하는 것은 사용자가 **GCE에 SSH로 접속해서 직접 실행하는 것과 환경이 완전히 다르기 때문에**, 버킷 주소만 바꾸면 **거의 100% 실패하게 됩니다.**

정말 중요한 3가지 "cron 환경 함정"이 있습니다.

### 1\. (가장 중요) 인증 문제

  * **문제점:** `cron` 작업은 사용자가 로그인하지 않은 상태(non-interactive)에서 최소한의 환경으로 실행됩니다. 즉, 사용자가 SSH 터미널에서 `gcloud auth login`이나 `gcloud auth configure-docker`로 얻은 인증 정보에 **전혀 접근할 수 없습니다.**
  * **해결책 (Best Practice):** `cron` 잡이 GCS에 접근하도록 인증 키 파일을 VM에 복사하는 것은 보안상 좋지 않습니다. 대신, **GCE VM 자체가 GCS에 접근할 권한을 갖도록** VM의 "서비스 계정"을 설정해야 합니다.

**조치 방법:**

1.  GCE VM 인스턴스 세부정보 페이지로 이동합니다.
2.  **'API 및 ID 관리'** 섹션에서 **'서비스 계정'** 이름을 확인합니다.
3.  GCP의 **'IAM 및 관리자'** \> **'IAM'** 메뉴로 이동합니다.
4.  해당 서비스 계정(@https://www.google.com/search?q=...gserviceaccount.com)을 찾아서 '수정'(연필 아이콘)을 누릅니다.
5.  \*\*'다른 역할 추가'\*\*를 클릭하여 `kfac-quant-db` 버킷에 대한 쓰기 권한을 부여합니다.
      * **역할:** `Storage Object Creator` (가장 이상적) 또는 `Storage Object Admin`

이렇게 하면 `gcloud` 명령어는 별도 로그인 없이 VM에 부여된 이 권한을 사용해 자동으로 인증됩니다.

### 2\. `gcloud` 명령어 경로 문제

  * **문제점:** `cron`은 `PATH` 환경 변수가 극도로 최소화되어 있습니다. `/usr/bin`, `/bin` 등 기본 경로만 알고 있어서 `gcloud` 명령어를 어디서 찾아야 할지 모를 수 있습니다.
  * **해결책:** `gcloud` 명령어의 **절대 경로**를 스크립트에 직접 명시해야 합니다.

**조치 방법:**

1.  GCE VM의 SSH 터미널에서 `which gcloud` 명령어를 실행하여 경로를 확인합니다. (예: `/usr/bin/gcloud` 또는 `/google-cloud-sdk/bin/gcloud` 등)
2.  스크립트의 `gcloud storage cp` 부분을 이 절대 경로로 수정합니다.

### 3\. 로깅 및 실패 모니터링 문제

  * **문제점:** `cron` 작업은 실패해도 사용자에게 알려주지 않고 조용히 실패합니다. (일명 "Silent Failure")
  * **해결책:** `cron` 작업의 모든 실행 결과(성공/실패 로그)를 파일로 저장해야 합니다.

**조치 방법:**
`crontab -e`로 `cron` 작업을 등록할 때, 명령어 뒤에 로깅 구문을 추가합니다.

(예: 매일 새벽 1시에 실행)

```crontab
0 1 * * * /path/to/your/script.sh >> /var/log/archive-gcs.log 2>&1
```

  * `>> /var/log/archive-gcs.log`: 표준 출력(echo 등)을 이 파일에 추가합니다.
  * `2>&1`: 표준 에러(오류 메시지)도 표준 출력과 같은 파일로 보냅니다.

-----

### 🧐 수정된 스크립트 및 Crontab 예시

이 3가지 문제점을 반영한 최종 버전입니다.

#### 1\. 수정된 `archive.sh` 스크립트

(스크립트의 `rm` 전 `$?` 확인 로직은 매우 훌륭합니다. 그대로 유지했습니다.)

```bash
#!/bin/bash
set -euo pipefail

# --- Configuration ---
# 1. (수정) 버킷 주소를 정확하게 변경
GCS_BUCKET="gs://kfac-quant-db/Binance-Option"

# 2. (수정) gcloud 명령어의 절대 경로 (which gcloud로 확인한 경로 사용)
GCLOUD_BIN="/usr/bin/gcloud" 

DATA_DIR="/data"
# --- End of Configuration ---

# ... (스크립트 나머지 부분은 동일) ...
CURRENT_DATE_YYMMDD=$(date -u +%y%m%d)

echo "--- Starting Daily Archive and Cleanup ---"
# ... (중략) ...

find "$DATA_DIR" -maxdepth 1 -type f -name "*.parquet" | while read -r FILE_PATH; do
  FILENAME=$(basename "$FILE_PATH")
  FILE_DATE_YYMMDD=$(echo "$FILENAME" | cut -d'-' -f2)

  if ! [[ "$FILE_DATE_YYMMDD" =~ ^[0-9]{6}$ ]]; then
    echo "Skipping file with invalid date format: $FILENAME"
    continue
  fi

  if [ "$FILE_DATE_YYMMDD" -lt "$CURRENT_DATE_YYMMDD" ]; then
    echo "Found expired file: $FILENAME"
    echo "  -> Archiving to $GCS_BUCKET ..."
    
    # 3. (수정) 절대 경로 변수 사용
    "$GCLOUD_BIN" storage cp "$FILE_PATH" "$GCS_BUCKET/"

    if [ $? -eq 0 ]; then
      echo "  -> Archive successful. Deleting local file."
      rm "$FILE_PATH"
    else
      echo "  -> ERROR: Failed to archive $FILENAME. The local file will be kept." >&2
    fi
  fi
done

echo "--- Archive and Cleanup Complete ---"
```

#### 2\. `crontab -e` 등록 예시 (로깅 포함)

```crontab
# 매일 새벽 1시(서버 시간 기준)에 아카이브 스크립트를 실행하고 로그를 남깁니다.
0 1 * * * /home/user/archive.sh >> /home/user/logs/archive-gcs.log 2>&1
```

**요약:**
버킷 주소 변경은 필수이지만, 스크립트가 `cron`에서 안정적으로 돌게 하려면 **(1) VM 서비스 계정 권한 부여**와 **(2) `gcloud` 절대 경로 사용**이 반드시 병행되어야 합니다.

alias docker-compose='sudo docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$(pwd):/workdir" \
    -w /workdir \
    docker/compose:latest'