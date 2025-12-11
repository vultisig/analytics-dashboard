#!/usr/bin/env python3
"""
LiFi Deep Historical Sync Script
Fetches ALL LiFi Vultisig transfer data using aggressive pagination
"""
import time
import logging
import requests
from datetime import datetime, timezone
from database.connection import db_manager
from ingestors.lifi import LiFiIngestor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LiFiDeepSync:
    def __init__(self):
        self.ingestor = LiFiIngestor()
        self.api_url = "https://li.quest/v2/analytics/transfers"
        self.total_processed = 0
        self.pages_processed = 0

    def make_direct_api_call(self, next_page_token=None, limit=1000):
        """Make direct API call with retry logic"""
        params = {
            'integrator': 'vultisig-ios,vultisig-android',
            'limit': limit
        }

        if next_page_token:
            params['next'] = next_page_token

        retries = 0
        max_retries = 3
        base_delay = 2

        while retries < max_retries:
            try:
                logger.info(f"🌐 LiFi API Request: {self.api_url}?{'&'.join([f'{k}={v}' for k,v in params.items()])}...")

                response = requests.get(self.api_url, params=params, timeout=30)

                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"⚠️ Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
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

        raise Exception(f"Max retries exceeded for LiFi API call")

    def deep_lifi_fetch(self, max_pages=100):
        """Fetch ALL historical LiFi data using aggressive pagination"""
        logger.info("🚀 Starting COMPLETE LiFi historical sync for ALL Vultisig transfer data")

        # Clear existing LiFi data
        logger.info("🧹 Clearing existing LiFi data...")
        db_manager.execute_query('DELETE FROM swaps WHERE source = %s', ('lifi',))

        next_page_token = None
        consecutive_empty_pages = 0

        try:
            while self.pages_processed < max_pages:
                self.pages_processed += 1

                logger.info(f"📄 Fetching LiFi page {self.pages_processed}/{max_pages}...")

                # Make API call
                data = self.make_direct_api_call(next_page_token=next_page_token, limit=1000)
                transfers = data.get('data', [])

                if not transfers:
                    consecutive_empty_pages += 1
                    logger.warning(f"📭 Empty LiFi page {self.pages_processed} (consecutive empty: {consecutive_empty_pages})")

                    if consecutive_empty_pages >= 2:
                        logger.info("🏁 Multiple consecutive empty pages - assuming end of LiFi data")
                        break

                    # Check if there's still a next token
                    if not data.get('hasNext', False):
                        logger.info("🏁 No more LiFi pagination available")
                        break

                    next_page_token = data.get('next')
                    time.sleep(2)
                    continue
                else:
                    consecutive_empty_pages = 0  # Reset counter

                logger.info(f"📊 Processing {len(transfers)} LiFi transfers...")

                # Show date range
                if transfers:
                    timestamps = []
                    for transfer in transfers:
                        ts = transfer.get('sending', {}).get('timestamp')
                        if ts:
                            timestamps.append(datetime.fromtimestamp(ts, timezone.utc))

                    if timestamps:
                        earliest = min(timestamps)
                        latest = max(timestamps)
                        logger.info(f"📅 LiFi page date range: {earliest.strftime('%Y-%m-%d')} to {latest.strftime('%Y-%m-%d')}")

                # Process the transfers
                self.process_transfers(transfers)

                # Get next page token
                if not data.get('hasNext', False):
                    logger.info("🔗 No more LiFi pagination - reached end")
                    break

                next_page_token = data.get('next')
                logger.info(f"🔗 Next LiFi token: {next_page_token[:50]}...")

                # Progress update
                if self.pages_processed % 5 == 0:
                    logger.info(f"📈 LiFi Progress: {self.pages_processed} pages, {self.total_processed} transfers")

                # Respectful delay between requests
                time.sleep(1)

        except KeyboardInterrupt:
            logger.warning("⚠️ LiFi sync interrupted by user")
        except Exception as e:
            logger.error(f"❌ LiFi deep historical sync failed: {e}")
            raise

        # Final results
        self.show_final_results()

    def process_transfers(self, transfers):
        """Process a batch of LiFi transfers into database"""
        transfer_records = []
        for transfer in transfers:
            parsed_transfer = self.ingestor.parse_swap(transfer)
            if parsed_transfer:
                transfer_records.append(parsed_transfer)

        if transfer_records:
            inserted_count = db_manager.insert_swaps(transfer_records)
            self.total_processed += inserted_count
            logger.info(f"✅ Inserted {inserted_count}/{len(transfer_records)} LiFi transfers from page {self.pages_processed}")

            if inserted_count < len(transfer_records):
                skipped = len(transfer_records) - inserted_count
                logger.info(f"ℹ️ Skipped {skipped} duplicate LiFi records")
        else:
            logger.warning("⚠️ No valid LiFi transfer records found in this batch")

    def show_final_results(self):
        """Show comprehensive final results for LiFi"""
        logger.info("=" * 80)
        logger.info("🎉 LIFI DEEP HISTORICAL SYNC COMPLETED!")
        logger.info(f"📊 Pages processed: {self.pages_processed}")
        logger.info(f"📊 Total LiFi transfers inserted: {self.total_processed}")

        # Get database statistics
        try:
            stats = db_manager.execute_query('''
                SELECT
                    COUNT(*) as total_transfers,
                    COUNT(CASE WHEN affiliate_fee_usd > 0 THEN 1 END) as transfers_with_fees,
                    SUM(affiliate_fee_usd) as total_fees,
                    MIN(timestamp) as earliest_transfer,
                    MAX(timestamp) as latest_transfer,
                    COUNT(DISTINCT DATE(timestamp)) as unique_days,
                    AVG(in_amount_usd) as avg_volume
                FROM swaps WHERE source = 'lifi'
            ''', fetch=True)[0]

            logger.info("📈 FINAL LIFI DATABASE STATISTICS:")
            logger.info(f"  💫 Total LiFi transfers: {stats['total_transfers']:,}")
            logger.info(f"  💰 Transfers with affiliate fees: {stats['transfers_with_fees']:,}")
            logger.info(f"  💰 Total LiFi affiliate fees: ${stats['total_fees']:,.4f}")
            logger.info(f"  📊 Average transfer volume: ${stats['avg_volume']:,.2f}")
            logger.info(f"  📅 LiFi date range: {stats['earliest_transfer']} to {stats['latest_transfer']}")
            logger.info(f"  📅 LiFi unique days: {stats['unique_days']:,}")

            # Calculate time span
            if stats['earliest_transfer'] and stats['latest_transfer']:
                time_span = stats['latest_transfer'] - stats['earliest_transfer']
                logger.info(f"  ⏰ LiFi time span: {time_span.days} days")

        except Exception as e:
            logger.error(f"❌ Failed to get final LiFi statistics: {e}")

        # Refresh materialized views
        try:
            logger.info("🔄 Refreshing materialized views...")
            db_manager.execute_query("SELECT refresh_daily_metrics()")
            logger.info("✅ Materialized views refreshed")
        except Exception as e:
            logger.error(f"⚠️ Failed to refresh views: {e}")

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Deep historical sync for ALL LiFi Vultisig transfer data")
    parser.add_argument("--max-pages", type=int, default=100,
                        help="Maximum number of pages to fetch (default: 100)")

    args = parser.parse_args()

    syncer = LiFiDeepSync()
    try:
        syncer.deep_lifi_fetch(max_pages=args.max_pages)
        logger.info("🎊 LiFi deep historical sync completed successfully!")
    except Exception as e:
        logger.error(f"💥 LiFi deep historical sync failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()