#!/usr/bin/env python3
"""
Binance Options WebSocket Collector V2 - Compact Version
- Single BTC@markPrice stream (broadcasts ALL BTC options)
- No API calls needed - stream provides all data
- Pushes to Redis for persistence
"""

import asyncio
import websockets
import json
import redis
import os
import signal
from datetime import datetime, timezone
from typing import Dict


class BinanceOptionsCollectorV2:
    def __init__(self):
        # Updated WebSocket URL - using combined stream format
        self.ws_url = 'wss://fstream.binance.com/market/stream?streams=btcusdt@optionMarkPrice'

        # Redis connection
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=False)
        self.stream_key = os.getenv('REDIS_STREAM_KEY', 'binance:options:markprice')

        # State
        self.should_stop = False
        self.symbols_seen = set()

        print(f"Collector V2 (Compact) initialized:")
        print(f"  WebSocket URL: {self.ws_url}")
        print(f"  Redis: {redis_host}:{redis_port}")
        print(f"  Stream Key: {self.stream_key}")
        print(f"  Stream: btcusdt@optionMarkPrice (single stream, ALL options)")

    def parse_symbol(self, symbol: str) -> Dict:
        """
        Parse option symbol to extract metadata
        Format: BTC-YYMMDD-STRIKE-TYPE
        Example: BTC-251226-100000-C
        """
        try:
            parts = symbol.split('-')
            if len(parts) != 4:
                return {}

            _, date_str, strike_str, opt_type = parts

            # Parse expiry date
            year = 2000 + int(date_str[:2])
            month = int(date_str[2:4])
            day = int(date_str[4:6])
            expiry_date = f"{year:04d}-{month:02d}-{day:02d}"

            # Parse strike and type
            strike_price = int(strike_str)
            option_type = 'CALL' if opt_type == 'C' else 'PUT'

            return {
                'strike_price': strike_price,
                'option_type': option_type,
                'expiry_date': expiry_date
            }
        except Exception as e:
            print(f"Error parsing symbol {symbol}: {e}")
            return {}

    async def process_mark_price(self, mark_data: Dict):
        """Process a single mark price event and push to Redis - now captures ALL fields"""
        try:
            symbol = mark_data['s']
            timestamp = mark_data['E']

            # Track unique symbols
            if symbol not in self.symbols_seen:
                self.symbols_seen.add(symbol)
                if len(self.symbols_seen) % 50 == 0:
                    print(f"[INFO] Tracking {len(self.symbols_seen)} unique symbols")

            # Parse symbol metadata
            metadata = self.parse_symbol(symbol)

            # Prepare enriched data with ALL available fields
            enriched_data = {
                # Basic fields
                'symbol': symbol,
                'timestamp': str(timestamp),
                'event_type': mark_data.get('e', 'markPrice'),

                # Parsed metadata
                'strike_price': str(metadata.get('strike_price', 0)),
                'option_type': metadata.get('option_type', 'UNKNOWN'),
                'expiry_date': metadata.get('expiry_date', ''),

                # Pricing fields
                'mark_price': mark_data.get('mp', '0'),
                'index_price': mark_data.get('i', '0'),
                'estimated_settle_price': mark_data.get('P', '0'),

                # Best bid/ask
                'best_bid_price': mark_data.get('bo', '0'),
                'best_ask_price': mark_data.get('ao', '0'),
                'best_bid_quantity': mark_data.get('bq', '0'),
                'best_ask_quantity': mark_data.get('aq', '0'),

                # Implied volatility
                'bid_iv': mark_data.get('b', '0'),
                'ask_iv': mark_data.get('a', '0'),
                'mark_iv': mark_data.get('vo', '0'),

                # Price limits
                'high_price_limit': mark_data.get('hl', '0'),
                'low_price_limit': mark_data.get('ll', '0'),

                # Risk metrics
                'risk_free_rate': mark_data.get('rf', '0'),

                # Greeks
                'delta': mark_data.get('d', '0'),
                'theta': mark_data.get('t', '0'),
                'gamma': mark_data.get('g', '0'),
                'vega': mark_data.get('v', '0'),
            }

            # Push to Redis Stream
            self.redis_client.xadd(
                self.stream_key,
                enriched_data,
                maxlen=100000
            )

        except Exception as e:
            print(f"Error processing mark price: {e}")
            print(f"Mark data: {mark_data}")

    async def listen_stream(self):
        """Listen to BTC@markPrice stream"""
        retry_count = 0
        max_retries = 10

        while not self.should_stop and retry_count < max_retries:
            try:
                async with websockets.connect(self.ws_url, ping_interval=30, ping_timeout=10) as websocket:
                    print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] ✓ Connected to BTC@markPrice stream\n")
                    retry_count = 0

                    message_count = 0
                    mark_count = 0

                    while not self.should_stop:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=60)
                            data = json.loads(message)
                            message_count += 1

                            # Debug every 100 messages
                            if message_count % 100 == 0:
                                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                                      f"Messages: {message_count} | Marks processed: {mark_count} | "
                                      f"Unique symbols: {len(self.symbols_seen)}")

                            # Process mark price array
                            if 'data' in data:
                                mark_array = data['data']

                                # BTC@markPrice sends data as ARRAY
                                if isinstance(mark_array, list):
                                    for mark_data in mark_array:
                                        if mark_data.get('e') == 'markPrice':
                                            await self.process_mark_price(mark_data)
                                            mark_count += 1

                        except asyncio.TimeoutError:
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            print(f"Connection closed, reconnecting...")
                            break

            except Exception as e:
                retry_count += 1
                wait_time = min(2 ** retry_count, 60)
                print(f"Connection error: {e}. Retry {retry_count}/{max_retries} in {wait_time}s...")
                await asyncio.sleep(wait_time)

        print(f"Stopped listening")

    def start(self):
        """Start the collector"""
        loop = asyncio.get_event_loop()

        # Handle shutdown signals
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        try:
            loop.run_until_complete(self.listen_stream())
        finally:
            loop.close()

    async def shutdown(self):
        """Graceful shutdown"""
        print("\n\nShutdown signal received...")
        self.should_stop = True


def main():
    print("="*80)
    print("Binance BTC Options WebSocket Collector V2 (Compact)")
    print("="*80)
    print("\nFeatures:")
    print("  - Single BTC@markPrice stream (broadcasts ALL options)")
    print("  - No API calls needed")
    print("  - Automatic symbol parsing")
    print("  - Data pushed to Redis Streams")
    print("\nPress Ctrl+C to stop\n")

    collector = BinanceOptionsCollectorV2()

    # Test Redis connection
    try:
        collector.redis_client.ping()
        print("✓ Redis connection OK\n")
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        print("Make sure Redis is running and accessible\n")
        return

    collector.start()


if __name__ == "__main__":
    main()
