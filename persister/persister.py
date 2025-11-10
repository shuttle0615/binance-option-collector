#!/usr/bin/env python3
"""
Binance Options Data Persister V2
- Reads mark price data from Redis Streams
- Writes one Parquet file per option (not split by day)
- Auto-deletes files for expired options
"""

import redis
import os
import signal
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Dict, List


class OptionDataPersisterV2:
    def __init__(self):
        # Redis connection
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.stream_key = os.getenv('REDIS_STREAM_KEY', 'binance:options:markprice')

        # Consumer group config
        self.group_name = 'binance_persister_v2'
        self.consumer_name = 'persister_worker_1'

        # Storage config - single directory, one file per option
        self.storage_path = Path(os.getenv('STORAGE_PATH', '/data'))
        self.storage_path.mkdir(parents=True, exist_ok=True)

        # Batch processing
        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        self.flush_interval = int(os.getenv('FLUSH_INTERVAL', '10'))  # seconds

        # State
        self.should_stop = False
        self.message_count = 0
        self.last_flush_time = time.time()
        self.last_cleanup_time = time.time()

        # Define Parquet schema for mark price data
        self.schema = pa.schema([
            ('timestamp', pa.timestamp('ms')),
            ('symbol', pa.string()),
            ('mark_price', pa.float64()),
            ('strike_price', pa.int32()),
            ('option_type', pa.string()),
            ('expiry_date', pa.date32()),
        ])

        print(f"Persister V2 initialized:")
        print(f"  Redis: {redis_host}:{redis_port}")
        print(f"  Stream Key: {self.stream_key}")
        print(f"  Consumer Group: {self.group_name}")
        print(f"  Consumer Name: {self.consumer_name}")
        print(f"  Storage Path: {self.storage_path}")
        print(f"  Storage Mode: ONE FILE PER OPTION")
        print(f"  Batch Size: {self.batch_size}")
        print(f"  Flush Interval: {self.flush_interval}s")

    def setup_consumer_group(self):
        """Create consumer group if it doesn't exist"""
        try:
            self.redis_client.xgroup_create(
                name=self.stream_key,
                groupname=self.group_name,
                id='0',
                mkstream=True
            )
            print(f"✓ Created consumer group '{self.group_name}'")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                print(f"✓ Consumer group '{self.group_name}' already exists")
            else:
                raise

    def parse_message(self, message_data: Dict) -> Dict:
        """Parse Redis message into structured format"""
        try:
            # Convert timestamp from string to int
            timestamp_ms = int(message_data['timestamp'])

            # Convert to datetime
            dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

            # Parse expiry_date
            expiry_dt = datetime.strptime(message_data['expiry_date'], '%Y-%m-%d').date()

            return {
                'timestamp': dt,
                'symbol': message_data['symbol'],
                'mark_price': float(message_data['mark_price']),
                'strike_price': int(message_data['strike_price']),
                'option_type': message_data['option_type'],
                'expiry_date': expiry_dt,
            }
        except Exception as e:
            print(f"Error parsing message: {e}")
            print(f"Message data: {message_data}")
            return None

    def get_file_path(self, symbol: str) -> Path:
        """
        Get file path for a given symbol
        V2: One file per option, no date subdirectories
        """
        # File name: SYMBOL.parquet
        file_path = self.storage_path / f"{symbol}.parquet"
        return file_path

    def write_to_parquet(self, symbol: str, mark_prices: List[Dict]):
        """Append mark prices to Parquet file"""
        if not mark_prices:
            return

        try:
            # Convert to DataFrame
            df = pd.DataFrame(mark_prices)

            # Get file path
            file_path = self.get_file_path(symbol)

            # Check if file exists
            if file_path.exists():
                # Read existing data
                existing_df = pd.read_parquet(file_path)

                # Ensure timestamp column is timezone-aware (UTC)
                if existing_df['timestamp'].dt.tz is None:
                    existing_df['timestamp'] = existing_df['timestamp'].dt.tz_localize('UTC')

                # Append new data
                df = pd.concat([existing_df, df], ignore_index=True)

                # Remove duplicates based on timestamp
                df = df.drop_duplicates(subset=['timestamp'], keep='last')

                # Sort by timestamp
                df = df.sort_values('timestamp')

            # Write to Parquet with compression
            table = pa.Table.from_pandas(df, schema=self.schema)
            pq.write_table(
                table,
                file_path,
                compression='snappy',
                use_dictionary=True
            )

            # Sample output (print 1% of writes to avoid spam)
            import random
            if random.random() < 0.01:
                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                      f"💾 Wrote {len(mark_prices)} marks for {symbol} "
                      f"(Total: {len(df)} records, Size: {file_path.stat().st_size / 1024:.1f}KB)")

        except Exception as e:
            print(f"Error writing to Parquet: {e}")
            print(f"Symbol: {symbol}, Marks: {len(mark_prices)}")

    def cleanup_expired_options(self):
        """Delete files for options that have expired"""
        try:
            now = datetime.now(timezone.utc)

            # List all parquet files
            parquet_files = list(self.storage_path.glob("*.parquet"))

            deleted_count = 0

            for file_path in parquet_files:
                try:
                    # Read just the metadata to check expiry
                    df = pd.read_parquet(file_path, columns=['expiry_date'])

                    if len(df) > 0:
                        # Get the expiry date (should be same for all rows)
                        expiry_date = df['expiry_date'].iloc[0]

                        # Convert to datetime for comparison
                        expiry_datetime = datetime.combine(expiry_date, datetime.min.time()).replace(tzinfo=timezone.utc)

                        # Add 1 day grace period
                        expiry_with_grace = expiry_datetime + timedelta(days=1)

                        if now > expiry_with_grace:
                            print(f"🗑️  Deleting expired option file: {file_path.name} (expired: {expiry_date})")
                            file_path.unlink()
                            deleted_count += 1

                except Exception as e:
                    print(f"Error checking file {file_path.name}: {e}")

            if deleted_count > 0:
                print(f"✓ Cleaned up {deleted_count} expired option files")

        except Exception as e:
            print(f"Error in cleanup: {e}")

    def process_batch(self, messages: List):
        """Process a batch of messages from Redis"""
        if not messages:
            return []

        # Group messages by symbol
        grouped_marks = defaultdict(list)
        processed_ids = []

        for stream_name, message_list in messages:
            for message_id, message_data in message_list:
                # Parse message
                parsed = self.parse_message(message_data)

                if parsed:
                    symbol = parsed['symbol']
                    grouped_marks[symbol].append(parsed)
                    processed_ids.append(message_id)
                else:
                    # Still ack failed messages to avoid reprocessing
                    processed_ids.append(message_id)

        # Write each symbol's marks to Parquet
        for symbol, marks in grouped_marks.items():
            self.write_to_parquet(symbol, marks)

        return processed_ids

    def run(self):
        """Main processing loop"""
        print("\n" + "="*80)
        print("Starting data persistence service V2...")
        print("="*80 + "\n")

        # Setup consumer group
        self.setup_consumer_group()

        print("Waiting for messages...\n")

        while not self.should_stop:
            try:
                # Read messages from Redis Stream
                messages = self.redis_client.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_key: '>'},
                    count=self.batch_size,
                    block=5000  # Block for 5 seconds
                )

                if messages:
                    # Process the batch
                    processed_ids = self.process_batch(messages)

                    # Acknowledge processed messages
                    if processed_ids:
                        for msg_id in processed_ids:
                            try:
                                self.redis_client.xack(self.stream_key, self.group_name, msg_id)
                            except Exception as e:
                                print(f"Error acknowledging message {msg_id}: {e}")

                        self.message_count += len(processed_ids)
                        self.last_flush_time = time.time()

                # Periodic status update
                if self.message_count > 0 and self.message_count % 10000 == 0:
                    print(f"📊 Processed {self.message_count} messages so far")

                # Periodic cleanup of expired options (every hour)
                # if time.time() - self.last_cleanup_time > 3600:
                #     self.cleanup_expired_options()
                #     self.last_cleanup_time = time.time()

            except KeyboardInterrupt:
                print("\n\nStopping persister...")
                break
            except Exception as e:
                print(f"Error in processing loop: {e}")
                time.sleep(5)

        print(f"\n✓ Persister stopped. Total messages processed: {self.message_count}")

    def start(self):
        """Start the persister with signal handling"""
        # Handle shutdown signals
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

        self.run()

    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        print("\n\nShutdown signal received...")
        self.should_stop = True


def main():
    print("="*80)
    print("Binance Options Data Persister V2")
    print("="*80)
    print("\nThis service reads mark price data from Redis and writes to Parquet files")
    print("Storage: ONE FILE PER OPTION (no daily splits)")
    print("Auto-cleanup: Expired options are deleted automatically")
    print("\nPress Ctrl+C to stop\n")

    persister = OptionDataPersisterV2()

    # Test Redis connection
    try:
        persister.redis_client.ping()
        print("✓ Redis connection OK\n")
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        print("Make sure Redis is running and accessible\n")
        return

    persister.start()


if __name__ == "__main__":
    main()
