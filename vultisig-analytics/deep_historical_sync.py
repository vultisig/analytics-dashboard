#!/usr/bin/env python3
"""
Deep Historical Sync Script
Fetches ALL Vultisig affiliate data going back to April 2024 (5,000+ records)
Uses aggressive pagination and backtracking to get complete historical dataset
"""
import time
import logging
import requests
from datetime import datetime, timezone
from database.connection import db_manager
from ingestors.thorchain import THORChainIngestor
from config import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DeepHistoricalSync:
    def __init__(self):
        self.ingestor = THORChainIngestor()
        self.api_url = config.THORCHAIN_API_URL
        self.target_earliest_date = datetime(2024, 4, 27, tzinfo=timezone.utc)
        self.total_processed = 0
        self.pages_processed = 0

    def make_direct_api_call(self, next_page_token=None, limit=50):
        """Make direct API call with better error handling and pagination"""
        params = {
            'type': 'swap',
            'affiliate': ','.join(config.VULTISIG_AFFILIATES),  # va,vi,v0
            'limit': limit
        }

        if next_page_token:
            params['nextPageToken'] = next_page_token

        retries = 0
        max_retries = 5
        base_delay = 3

        while retries < max_retries:
            try:
                url_with_params = f"{self.api_url}?{'&'.join([f'{k}={v}' for k,v in params.items()])}"
                logger.info(f"🌐 API Request: {url_with_params[:150]}...")

                response = requests.get(self.api_url, params=params, timeout=90)

                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 120))
                    logger.warning(f"⚠️ Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                if response.status_code in [502, 503, 504]:
                    retries += 1
                    delay = base_delay * (2 ** retries)
                    logger.warning(f"⚠️ Server error {response.status_code}. Retry {retries}/{max_retries} in {delay}s")
                    time.sleep(delay)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                retries += 1
                delay = base_delay * (2 ** retries)
                logger.warning(f"⚠️ Timeout. Retry {retries}/{max_retries} in {delay}s")
                time.sleep(delay)
                continue

            except Exception as e:
                retries += 1
                if retries >= max_retries:
                    logger.error(f"❌ API call failed after {max_retries} retries: {e}")
                    raise
                delay = base_delay * (2 ** retries)
                logger.warning(f"⚠️ Request error. Retry {retries}/{max_retries} in {delay}s: {e}")
                time.sleep(delay)
                continue

        raise Exception(f"Max retries exceeded for API call")

    def convert_timestamp_to_date(self, timestamp_str):
        """Convert THORChain timestamp to datetime"""
        try:
            ts_sec = int(timestamp_str) // 1_000_000_000
            return datetime.fromtimestamp(ts_sec, timezone.utc)
        except (ValueError, TypeError):
            return None

    def deep_historical_fetch(self, max_pages=1000):
        """Fetch deep historical data using aggressive pagination"""
        logger.info("🚀 Starting DEEP historical sync for ALL Vultisig affiliate data")
        logger.info(f"🎯 Target: Fetch back to {self.target_earliest_date.strftime('%Y-%m-%d')}")
        logger.info(f"📊 Expected: ~5,000+ records based on original CSV")

        # Clear existing data
        logger.info("🧹 Clearing existing data...")
        db_manager.execute_query('DELETE FROM swaps WHERE source = %s', ('thorchain',))

        next_page_token = None
        consecutive_empty_pages = 0
        earliest_date_seen = None

        try:
            while self.pages_processed < max_pages:
                self.pages_processed += 1

                logger.info(f"📄 Fetching page {self.pages_processed}/{max_pages}...")

                # Make API call
                data = self.make_direct_api_call(next_page_token=next_page_token, limit=50)
                actions = data.get('actions', [])

                if not actions:
                    consecutive_empty_pages += 1
                    logger.warning(f"📭 Empty page {self.pages_processed} (consecutive empty: {consecutive_empty_pages})")

                    if consecutive_empty_pages >= 3:
                        logger.info("🏁 Multiple consecutive empty pages - assuming end of data")
                        break

                    # Try to continue with next token anyway
                    next_token = data.get('nextPageToken')
                    if not next_token:
                        logger.info("🏁 No more pagination tokens available")
                        break
                    next_page_token = next_token
                    time.sleep(5)  # Longer delay for empty pages
                    continue
                else:
                    consecutive_empty_pages = 0  # Reset counter

                logger.info(f"📊 Processing {len(actions)} actions...")

                # Check date range of this page
                if actions:
                    first_date_str = actions[0].get('date', '')
                    last_date_str = actions[-1].get('date', '')

                    first_date = self.convert_timestamp_to_date(first_date_str)
                    last_date = self.convert_timestamp_to_date(last_date_str)

                    if first_date and last_date:
                        logger.info(f"📅 Page date range: {last_date.strftime('%Y-%m-%d')} to {first_date.strftime('%Y-%m-%d')}")

                        if not earliest_date_seen or last_date < earliest_date_seen:
                            earliest_date_seen = last_date

                        # Check if we've reached our target date
                        if last_date <= self.target_earliest_date:
                            logger.info(f"🎯 Reached target date! Last date: {last_date.strftime('%Y-%m-%d')}")
                            # Process this page and then stop
                            self.process_actions(actions)
                            break

                # Process the actions
                self.process_actions(actions)

                # Get next page token
                next_token = data.get('nextPageToken')
                if not next_token:
                    logger.info("🔗 No more pagination tokens - reached end")
                    break

                next_page_token = next_token
                logger.info(f"🔗 Next page token: {next_token[:50]}...")

                # Progress update
                if self.pages_processed % 10 == 0:
                    logger.info(f"📈 Progress: {self.pages_processed} pages, {self.total_processed} swaps")
                    if earliest_date_seen:
                        logger.info(f"📅 Earliest date so far: {earliest_date_seen.strftime('%Y-%m-%d %H:%M:%S')}")

                # Respectful delay between requests
                time.sleep(3)

        except KeyboardInterrupt:
            logger.warning("⚠️ Script interrupted by user")
        except Exception as e:
            logger.error(f"❌ Deep historical sync failed: {e}")
            raise

        # Final results
        self.show_final_results()

    def process_actions(self, actions):
        """Process a batch of actions into database"""
        swap_records = []
        for action in actions:
            parsed_swap = self.ingestor.parse_swap(action)
            if parsed_swap:
                swap_records.append(parsed_swap)

        if swap_records:
            inserted_count = db_manager.insert_swaps(swap_records)
            self.total_processed += inserted_count
            logger.info(f"✅ Inserted {inserted_count}/{len(swap_records)} swaps from page {self.pages_processed}")

            if inserted_count < len(swap_records):
                skipped = len(swap_records) - inserted_count
                logger.info(f"ℹ️ Skipped {skipped} duplicate records")
        else:
            logger.warning("⚠️ No valid swap records found in this batch")

    def show_final_results(self):
        """Show comprehensive final results"""
        logger.info("=" * 80)
        logger.info("🎉 DEEP HISTORICAL SYNC COMPLETED!")
        logger.info(f"📊 Pages processed: {self.pages_processed}")
        logger.info(f"📊 Total swaps inserted: {self.total_processed}")

        # Get database statistics
        try:
            stats = db_manager.execute_query('''
                SELECT
                    COUNT(*) as total_swaps,
                    COUNT(CASE WHEN affiliate_fee_usd > 0 THEN 1 END) as swaps_with_fees,
                    SUM(affiliate_fee_usd) as total_fees,
                    MIN(timestamp) as earliest_swap,
                    MAX(timestamp) as latest_swap,
                    COUNT(DISTINCT DATE(timestamp)) as unique_days
                FROM swaps WHERE source = 'thorchain'
            ''', fetch=True)[0]

            logger.info("📈 FINAL DATABASE STATISTICS:")
            logger.info(f"  💫 Total swaps: {stats['total_swaps']:,}")
            logger.info(f"  💰 Swaps with affiliate fees: {stats['swaps_with_fees']:,}")
            logger.info(f"  💰 Total affiliate fees: ${stats['total_fees']:,.4f}")
            logger.info(f"  📅 Date range: {stats['earliest_swap']} to {stats['latest_swap']}")
            logger.info(f"  📅 Unique days: {stats['unique_days']:,}")

            # Compare with original CSV
            original_count = 5027
            coverage_pct = (stats['total_swaps'] / original_count) * 100 if original_count > 0 else 0
            logger.info(f"  📊 Original CSV had: {original_count:,} records")
            logger.info(f"  📊 Coverage: {coverage_pct:.1f}% of original data")

        except Exception as e:
            logger.error(f"❌ Failed to get final statistics: {e}")

        # Refresh materialized views
        try:
            logger.info("🔄 Refreshing materialized views...")
            db_manager.execute_query("SELECT refresh_daily_metrics()")
            logger.info("✅ Materialized views refreshed")
        except Exception as e:
            logger.error(f"⚠️ Failed to refresh views: {e}")

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Deep historical sync for ALL Vultisig affiliate data")
    parser.add_argument("--max-pages", type=int, default=1000,
                        help="Maximum number of pages to fetch (default: 1000)")

    args = parser.parse_args()

    syncer = DeepHistoricalSync()
    try:
        syncer.deep_historical_fetch(max_pages=args.max_pages)
        logger.info("🎊 Deep historical sync completed successfully!")
    except Exception as e:
        logger.error(f"💥 Deep historical sync failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()