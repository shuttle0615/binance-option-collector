# Binance Options Collector V2 - Compact Version

## Overview

This is the **simplest** and **most efficient** version of the collector.

### Key Insight

Binance's `btcusdt@optionMarkPrice` stream is **ONE** stream that broadcasts mark prices for **ALL** BTC options simultaneously. No need to:
- Call REST API to get option list
- Create multiple WebSocket connections
- Subscribe to individual symbols

Just connect to `btcusdt@optionMarkPrice` and receive everything - including Greeks, implied volatility, and order book data!

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│  Binance WebSocket                                                │
│  wss://fstream.binance.com/market/stream                          │
│  ?streams=btcusdt@optionMarkPrice                                 │
│                                                                   │
│  Broadcasts ALL BTC options every 1s with:                       │
│  • Mark prices + Index price                                     │
│  • Greeks (delta, gamma, theta, vega)                            │
│  • Implied volatility (bid/ask/mark)                             │
│  • Order book (best bid/ask + quantities)                        │
└──────────────────┬────────────────────────────────────────────────┘
                   │
                   │ Single WebSocket connection
                   ▼
        ┌──────────────────────┐
        │   Collector          │
        │  - Parse symbol      │
        │  - Extract metadata  │
        │  - Capture 23 fields │
        │  - Push to Redis     │
        └──────────┬───────────┘
                   │
                   │ Redis Streams (23 fields per message)
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
        │  - 27 columns        │
        │  - Auto-cleanup      │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │   data/              │
        │  - BTC-251226-*.parq │
        │  - One file per opt  │
        │  - 27 columns each   │
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

The `btcusdt@optionMarkPrice` stream sends messages like this:

```json
{
  "stream": "btcusdt@optionMarkPrice",
  "data": [
    {
      "s": "BTC-251120-126000-C",    // Symbol
      "mp": "770.543",               // Mark price
      "E": 1762867543321,            // Event time
      "e": "markPrice",              // Event type
      "i": "104334.60217391",        // Index price
      "P": "0.000",                  // Estimated Settle Price
      "bo": "0.000",                 // Best buy price
      "ao": "900.000",               // Best sell price
      "bq": "0.0000",                // Best buy quantity
      "aq": "0.2000",                // Best sell quantity
      "b": "-1.0",                   // Buy Implied volatility
      "a": "0.98161161",             // Sell Implied volatility
      "hl": "924.652",               // Buy Maximum price
      "ll": "616.435",               // Sell Minimum price
      "vo": "0.9408058",             // Mark Implied Volatility
      "rf": "0.0",                   // Risk free rate
      "d": "0.11111964",             // Delta
      "t": "-164.26702615",          // Theta
      "g": "0.00001245",             // Gamma
      "v": "30.63855919"             // Vega
    },
    ...
  ]
}
```

**Key points:**
- WebSocket URL: `wss://fstream.binance.com/market/stream?streams=btcusdt@optionMarkPrice`
- `data` is an **ARRAY** of mark prices (not a single object)
- Broadcasts mark prices for ALL BTC options at once
- Updates every 1000ms (1 second)
- Symbol format: `BTC-YYMMDD-STRIKE-TYPE`
- **NEW**: Now includes Greeks, implied volatility, and order book data!

### **Parquet File Schema**

Each `data/SYMBOL.parquet` file contains **27 columns** with comprehensive options data:

#### Basic Information
| Column | Type | Description |
|--------|------|-------------|
| timestamp | timestamp[ms] | Collection time (UTC) |
| symbol | string | Option symbol (e.g., BTC-251226-100000-C) |
| event_type | string | Event type (always "markPrice") |
| strike_price | int32 | Strike price |
| option_type | string | CALL or PUT |
| expiry_date | date32 | Expiration date |

#### Pricing Data
| Column | Type | Description |
|--------|------|-------------|
| mark_price | float64 | Mark price (fair value) |
| index_price | float64 | Underlying BTC index price |
| estimated_settle_price | float64 | Estimated settlement price |

#### Order Book / Liquidity
| Column | Type | Description |
|--------|------|-------------|
| best_bid_price | float64 | Best buy price |
| best_ask_price | float64 | Best sell price |
| best_bid_quantity | float64 | Best buy quantity |
| best_ask_quantity | float64 | Best sell quantity |
| high_price_limit | float64 | Maximum allowed price |
| low_price_limit | float64 | Minimum allowed price |

#### Implied Volatility
| Column | Type | Description |
|--------|------|-------------|
| bid_iv | float64 | Buy-side implied volatility |
| ask_iv | float64 | Sell-side implied volatility |
| mark_iv | float64 | Mark implied volatility |

#### Risk Metrics
| Column | Type | Description |
|--------|------|-------------|
| risk_free_rate | float64 | Risk-free interest rate |

#### Greeks (Option Sensitivity)
| Column | Type | Description |
|--------|------|-------------|
| delta | float64 | Delta (price sensitivity to underlying) |
| gamma | float64 | Gamma (delta sensitivity to underlying) |
| theta | float64 | Theta (time decay) |
| vega | float64 | Vega (volatility sensitivity) |

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

### **Put/Call Sentiment Analysis**

```python
# Use actual BTC index price from the data
btc_price = df_all['index_price'].iloc[0]
print(f"Current BTC Index Price: ${btc_price:,.2f}")

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

### **Implied Volatility Analysis**

```python
# Analyze IV skew between calls and puts
calls = df_all[df_all['option_type'] == 'CALL']
puts = df_all[df_all['option_type'] == 'PUT']

avg_call_iv = calls['mark_iv'].mean()
avg_put_iv = puts['mark_iv'].mean()

print(f"Average Call IV: {avg_call_iv:.2%}")
print(f"Average Put IV: {avg_put_iv:.2%}")
print(f"IV Skew (Put - Call): {(avg_put_iv - avg_call_iv):.2%}")

# Analyze bid-ask IV spread (measure of uncertainty)
df_all['iv_spread'] = df_all['ask_iv'] - df_all['bid_iv']
print(f"\nAverage IV Bid-Ask Spread: {df_all['iv_spread'].mean():.2%}")
```

### **Greeks Risk Analysis**

```python
# Portfolio delta exposure
total_delta = df_all['delta'].sum()
print(f"Total Delta Exposure: {total_delta:.4f}")

# Find options with highest gamma (most sensitive to price moves)
high_gamma = df_all.nlargest(10, 'gamma')[['symbol', 'strike_price', 'gamma', 'delta']]
print("\nTop 10 Highest Gamma Options:")
print(high_gamma)

# Theta analysis (time decay)
avg_call_theta = calls['theta'].mean()
avg_put_theta = puts['theta'].mean()
print(f"\nAverage Call Theta (daily decay): ${avg_call_theta:.2f}")
print(f"Average Put Theta (daily decay): ${avg_put_theta:.2f}")

# Vega exposure (volatility risk)
total_vega = df_all['vega'].sum()
print(f"\nTotal Vega Exposure: {total_vega:.2f}")
print("(1% increase in IV would change portfolio value by ${:.2f})".format(total_vega))
```

### **Liquidity Analysis**

```python
# Calculate bid-ask spread
df_all['spread'] = df_all['best_ask_price'] - df_all['best_bid_price']
df_all['spread_pct'] = (df_all['spread'] / df_all['mark_price']) * 100

# Find most liquid options
liquid_options = df_all.nsmallest(10, 'spread_pct')[
    ['symbol', 'mark_price', 'spread', 'spread_pct', 'best_bid_quantity', 'best_ask_quantity']
]
print("Top 10 Most Liquid Options (lowest spread %):")
print(liquid_options)

# Calculate total liquidity
total_bid_liquidity = (df_all['best_bid_price'] * df_all['best_bid_quantity']).sum()
total_ask_liquidity = (df_all['best_ask_price'] * df_all['best_ask_quantity']).sum()
print(f"\nTotal Bid Liquidity: ${total_bid_liquidity:,.2f}")
print(f"Total Ask Liquidity: ${total_ask_liquidity:,.2f}")
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