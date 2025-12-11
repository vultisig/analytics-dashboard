#!/usr/bin/env python3
"""
MayaChain Deep Historical Sync Script
Fetches ALL MayaChain Vultisig affiliate data using aggressive token pagination
Same approach that successfully fetched 5,000+ THORChain swaps
"""
import time
import logging
import requests
from datetime import datetime, timezone
from database.connection import db_manager
from ingestors.mayachain import MayaChainIngestor
from config import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MayaChainDeepSync:
    def __init__(self):
        self.ingestor = MayaChainIngestor()
        self.api_url = "https://midgard.mayachain.info/v2/actions"
        self.total_processed = 0
        self.pages_processed = 0

    def make_direct_api_call(self, next_page_token=None, limit=50):
        """Make direct API call with aggressive retry logic"""
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
                logger.info(f"🌐 MayaChain API Request: {url_with_params[:100]}...")

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

        raise Exception(f"Max retries exceeded for MayaChain API call")

    def convert_timestamp_to_date(self, timestamp_str):
        """Convert MayaChain timestamp to datetime"""
        try:
            ts_sec = int(timestamp_str) // 1_000_000_000
            return datetime.fromtimestamp(ts_sec, timezone.utc)
        except (ValueError, TypeError):
            return None

    def deep_mayachain_fetch(self, max_pages=2000):
        """Fetch ALL historical MayaChain data using aggressive pagination"""
        logger.info("🚀 Starting COMPLETE MayaChain historical sync for ALL Vultisig affiliate data")
        logger.info(f"🎯 Target: Fetch back to MayaChain launch (expecting hundreds/thousands of records)")

        # Clear existing MayaChain data
        logger.info("🧹 Clearing existing MayaChain data...")
        db_manager.execute_query('DELETE FROM swaps WHERE source = %s', ('mayachain',))

        next_page_token = None
        consecutive_empty_pages = 0
        earliest_date_seen = None

        try:
            while self.pages_processed < max_pages:
                self.pages_processed += 1

                logger.info(f"📄 Fetching MayaChain page {self.pages_processed}/{max_pages}...")

                # Make API call
                data = self.make_direct_api_call(next_page_token=next_page_token, limit=50)
                actions = data.get('actions', [])

                if not actions:
                    consecutive_empty_pages += 1
                    logger.warning(f"📭 Empty MayaChain page {self.pages_processed} (consecutive empty: {consecutive_empty_pages})")

                    if consecutive_empty_pages >= 3:
                        logger.info("🏁 Multiple consecutive empty pages - assuming end of MayaChain data")
                        break

                    # Try to continue with next token anyway
                    next_token = data.get('nextPageToken') or data.get('meta', {}).get('nextPageToken')
                    if not next_token:
                        logger.info("🏁 No more MayaChain pagination tokens available")
                        break
                    next_page_token = next_token
                    time.sleep(5)  # Longer delay for empty pages
                    continue
                else:
                    consecutive_empty_pages = 0  # Reset counter

                logger.info(f"📊 Processing {len(actions)} MayaChain actions...")

                # Check date range of this page
                if actions:
                    first_date_str = actions[0].get('date', '')
                    last_date_str = actions[-1].get('date', '')

                    first_date = self.convert_timestamp_to_date(first_date_str)
                    last_date = self.convert_timestamp_to_date(last_date_str)

                    if first_date and last_date:
                        logger.info(f"📅 MayaChain page date range: {last_date.strftime('%Y-%m-%d')} to {first_date.strftime('%Y-%m-%d')}")

                        if not earliest_date_seen or last_date < earliest_date_seen:
                            earliest_date_seen = last_date

                # Process the actions
                self.process_actions(actions)

                # Get next page token from either location
                next_token = data.get('nextPageToken') or data.get('meta', {}).get('nextPageToken')
                if not next_token:
                    logger.info("🔗 No more MayaChain pagination tokens - reached end")
                    break

                next_page_token = next_token
                logger.info(f"🔗 Next MayaChain token: {next_token[:50]}...")

                # Progress update
                if self.pages_processed % 10 == 0:
                    logger.info(f"📈 MayaChain Progress: {self.pages_processed} pages, {self.total_processed} swaps")
                    if earliest_date_seen:
                        logger.info(f"📅 Earliest MayaChain date so far: {earliest_date_seen.strftime('%Y-%m-%d %H:%M:%S')}")

                # Respectful delay between requests
                time.sleep(3)

        except KeyboardInterrupt:
            logger.warning("⚠️ MayaChain sync interrupted by user")
        except Exception as e:
            logger.error(f"❌ MayaChain deep historical sync failed: {e}")
            raise

        # Final results
        self.show_final_results()

    def process_actions(self, actions):
        """Process a batch of MayaChain actions into database"""
        swap_records = []
        for action in actions:
            parsed_swap = self.ingestor.parse_swap(action)
            if parsed_swap:
                swap_records.append(parsed_swap)

        if swap_records:
            inserted_count = db_manager.insert_swaps(swap_records)
            self.total_processed += inserted_count
            logger.info(f"✅ Inserted {inserted_count}/{len(swap_records)} MayaChain swaps from page {self.pages_processed}")

            if inserted_count < len(swap_records):
                skipped = len(swap_records) - inserted_count
                logger.info(f"ℹ️ Skipped {skipped} duplicate MayaChain records")
        else:
            logger.warning("⚠️ No valid MayaChain swap records found in this batch")

    def show_final_results(self):
        """Show comprehensive final results for MayaChain"""
        logger.info("=" * 80)
        logger.info("🎉 MAYACHAIN DEEP HISTORICAL SYNC COMPLETED!")
        logger.info(f"📊 Pages processed: {self.pages_processed}")
        logger.info(f"📊 Total MayaChain swaps inserted: {self.total_processed}")

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
                FROM swaps WHERE source = 'mayachain'
            ''', fetch=True)[0]

            logger.info("📈 FINAL MAYACHAIN DATABASE STATISTICS:")
            logger.info(f"  💫 Total MayaChain swaps: {stats['total_swaps']:,}")
            logger.info(f"  💰 Swaps with affiliate fees: {stats['swaps_with_fees']:,}")
            logger.info(f"  💰 Total MayaChain affiliate fees: ${stats['total_fees']:,.4f}")
            logger.info(f"  📅 MayaChain date range: {stats['earliest_swap']} to {stats['latest_swap']}")
            logger.info(f"  📅 MayaChain unique days: {stats['unique_days']:,}")

            # Calculate time span
            if stats['earliest_swap'] and stats['latest_swap']:
                time_span = stats['latest_swap'] - stats['earliest_swap']
                logger.info(f"  ⏰ MayaChain time span: {time_span.days} days")

        except Exception as e:
            logger.error(f"❌ Failed to get final MayaChain statistics: {e}")

        # Refresh materialized views
        try:
            logger.info("🔄 Refreshing materialized views...")
            db_manager.execute_query("SELECT refresh_daily_metrics()")
            logger.info("✅ Materialized views refreshed")
        except Exception as e:
            logger.error(f"⚠️ Failed to refresh views: {e}")

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Deep historical sync for ALL MayaChain Vultisig affiliate data")
    parser.add_argument("--max-pages", type=int, default=2000,
                        help="Maximum number of pages to fetch (default: 2000)")

    args = parser.parse_args()

    syncer = MayaChainDeepSync()
    try:
        syncer.deep_mayachain_fetch(max_pages=args.max_pages)
        logger.info("🎊 MayaChain deep historical sync completed successfully!")
    except Exception as e:
        logger.error(f"💥 MayaChain deep historical sync failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()