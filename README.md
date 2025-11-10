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
docker-compose up -d --build

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

# Chekc the persister
docker exec -it binance_persister_v2_compact /bin/bash
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

alias docker-compose='sudo docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$(pwd):/workdir" \
    -w /workdir \
    docker/compose:latest'