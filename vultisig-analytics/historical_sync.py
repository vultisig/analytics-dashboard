#!/usr/bin/env python3
"""
Historical sync script to fetch ALL Vultisig affiliate data from THORChain
Similar to the original vulttcfee.py script but using the analytics database
"""
import time
import logging
from datetime import datetime
from database.connection import db_manager
from ingestors.thorchain import THORChainIngestor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def historical_sync(max_pages=None):
    """
    Fetch ALL historical Vultisig affiliate data from THORChain
    This will continue fetching until no more data is available
    """
    logger.info("🚀 Starting COMPLETE historical sync for Vultisig affiliates (va,vi,v0)")

    ingestor = THORChainIngestor()

    # Clear existing data and reset sync status
    logger.info("🧹 Clearing existing data...")
    db_manager.execute_query('DELETE FROM swaps WHERE source = %s', ('thorchain',))
    db_manager.update_sync_status('thorchain', next_page_token=None, error_count=0, last_error=None)

    next_page_token = None
    total_processed = 0
    pages_processed = 0
    total_records = 0

    try:
        while True:
            if max_pages and pages_processed >= max_pages:
                logger.info(f"🏁 Reached maximum pages limit ({max_pages})")
                break

            pages_processed += 1
            logger.info(f"📄 Fetching page {pages_processed}...")

            # Fetch data from API
            data = ingestor.fetch_data(next_page_token=next_page_token)
            actions = data.get('actions', [])

            if not actions:
                logger.info("📭 No more actions available - reached end of historical data")
                break

            total_records += len(actions)
            logger.info(f"📊 Processing {len(actions)} actions from API...")

            # Parse actions into swap records
            swap_records = []
            for action in actions:
                parsed_swap = ingestor.parse_swap(action)
                if parsed_swap:
                    swap_records.append(parsed_swap)

            # Insert into database
            if swap_records:
                inserted_count = db_manager.insert_swaps(swap_records)
                total_processed += inserted_count
                logger.info(f"✅ Inserted {inserted_count} swaps from page {pages_processed}")

                if inserted_count < len(swap_records):
                    skipped = len(swap_records) - inserted_count
                    logger.info(f"ℹ️  Skipped {skipped} duplicate records")
            else:
                logger.warning("⚠️  No valid swap records found on this page")

            # Get next page token
            next_token = data.get('nextPageToken')
            if not next_token:
                logger.info("🏁 No more pages available - historical sync complete!")
                break

            # Update sync status
            db_manager.update_sync_status(
                'thorchain',
                next_page_token=next_token,
                last_synced_timestamp=datetime.now(),
                error_count=0,
                last_error=None
            )

            next_page_token = next_token

            # Add delay to be nice to the API
            logger.info("⏳ Waiting 2 seconds before next request...")
            time.sleep(2)

            # Progress update every 10 pages
            if pages_processed % 10 == 0:
                logger.info(f"📈 Progress: {pages_processed} pages, {total_processed} swaps processed")

        # Final results
        logger.info("=" * 60)
        logger.info("✅ HISTORICAL SYNC COMPLETED!")
        logger.info(f"📊 Total API records processed: {total_records}")
        logger.info(f"📊 Total pages fetched: {pages_processed}")
        logger.info(f"📊 Total swaps inserted: {total_processed}")

        # Get final database stats
        stats = db_manager.execute_query('''
            SELECT
                COUNT(*) as total_swaps,
                COUNT(CASE WHEN affiliate_fee_usd > 0 THEN 1 END) as swaps_with_fees,
                SUM(affiliate_fee_usd) as total_fees,
                MIN(timestamp) as earliest_swap,
                MAX(timestamp) as latest_swap
            FROM swaps WHERE source = 'thorchain'
        ''', fetch=True)[0]

        logger.info(f"💰 Affiliate fee swaps: {stats['swaps_with_fees']}")
        logger.info(f"💰 Total affiliate fees: ${stats['total_fees']:,.4f}")
        logger.info(f"📅 Date range: {stats['earliest_swap']} to {stats['latest_swap']}")

        # Refresh materialized views
        logger.info("🔄 Refreshing materialized views...")
        db_manager.execute_query("SELECT refresh_daily_metrics()")
        logger.info("✅ Materialized views refreshed")

        return total_processed

    except KeyboardInterrupt:
        logger.warning("⚠️  Script interrupted by user")
        logger.info(f"📊 Partial results: {total_processed} swaps processed across {pages_processed} pages")
        return total_processed

    except Exception as e:
        logger.error(f"❌ Historical sync failed: {e}")
        logger.info(f"📊 Partial results: {total_processed} swaps processed across {pages_processed} pages")
        raise

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch ALL historical Vultisig affiliate data")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Maximum number of pages to fetch (default: unlimited)")

    args = parser.parse_args()

    try:
        total = historical_sync(max_pages=args.max_pages)
        logger.info(f"🎉 Historical sync completed successfully! Total swaps: {total}")
    except Exception as e:
        logger.error(f"💥 Historical sync failed: {e}")
        exit(1)