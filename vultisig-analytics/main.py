# main.py
import time
import logging
import schedule
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from database.connection import db_manager
from config import config
from ingestors.thorchain import THORChainIngestor
from ingestors.mayachain import MayaChainIngestor
from ingestors.lifi import LiFiIngestor
from ingestors.arkham_ingestor import ArkhamIngestor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class SyncService:
    def __init__(self):
        self.ingestors = {
            'arkham': ArkhamIngestor(),
            'thorchain': THORChainIngestor(),
            'mayachain': MayaChainIngestor(),
            'lifi': LiFiIngestor(),
        }
    
    def sync_source(self, source_name: str):
        """Sync data from a specific source"""
        logger.info(f"Starting sync for {source_name}")
        
        try:
            ingestor = self.ingestors[source_name]
            
            # Special handling for Arkham which has its own ingestion logic
            if source_name == 'arkham':
                try:
                    ingestor.ingest()
                    logger.info(f"Completed sync for {source_name}")
                    # Update sync status on success
                    db_manager.update_sync_status(
                        source_name,
                        next_page_token=None,
                        last_synced_timestamp=datetime.utcnow(),
                        error_count=0,
                        last_error=None
                    )
                except Exception as e:
                    logger.error(f"Sync failed for {source_name}: {e}")
                    # Update sync status on failure
                    db_manager.update_sync_status(
                        source_name,
                        next_page_token=None,
                        last_synced_timestamp=datetime.utcnow(),
                        error_count=1,
                        last_error=str(e)
                    )
                return

            sync_status = None
            try:
                sync_status = db_manager.get_sync_status(source_name)
            except Exception as e:
                logger.warning(f"Could not fetch sync status for {source_name}: {e}")
            
            if not sync_status:
                logger.info(f"No sync status found for {source_name}, starting fresh")
                sync_status = {}
            
            # Get latest transaction hash from database to detect duplicates
            latest_tx_hash = None
            try:
                cursor = db_manager.conn.cursor()
                cursor.execute(f"SELECT tx_hash FROM swaps WHERE source = %s ORDER BY timestamp DESC LIMIT 1", (source_name,))
                result = cursor.fetchone()
                if result:
                    latest_tx_hash = result[0]
                    logger.info(f"Latest {source_name} tx in DB: {latest_tx_hash}")
            except Exception as e:
                logger.warning(f"Could not fetch latest tx_hash for {source_name}: {e}")
            
            # Start fresh from page 1 (latest data) instead of using potentially expired token
            # This ensures we always get the newest data first
            next_page_token = None
            total_processed = 0
            pages_processed = 0
            found_existing_data = False
            max_pages = 10  # Limit pages per sync to avoid infinite pagination
            consecutive_zero_inserts = 0  # Track consecutive pages with no new data

            while True:
                try:
                    # Fetch data
                    data = ingestor.fetch_data(next_page_token=next_page_token)

                    # Handle different response formats
                    if source_name == 'lifi':
                        actions = data.get('data', [])
                    else:
                        actions = data.get('actions', [])
                    
                    if not actions:
                        logger.info(f"No more actions for {source_name}")
                        break
                    
                    # Parse and prepare swap data
                    swap_records = []
                    for action in actions:
                        parsed_swap = ingestor.parse_swap(action)
                        if parsed_swap:
                            # Check if we've reached data we already have
                            if latest_tx_hash and parsed_swap.get('tx_hash') == latest_tx_hash:
                                logger.info(f"Reached existing data at tx {latest_tx_hash}, stopping sync")
                                found_existing_data = True
                                break
                            swap_records.append(parsed_swap)
                    
                    # Stop if we found existing data
                    if found_existing_data:
                        break
                    
                    # Insert into database
                    if swap_records:
                        inserted_count = db_manager.insert_swaps(swap_records)
                        total_processed += inserted_count
                        logger.info(f"Inserted {inserted_count} swaps from page {pages_processed + 1}")

                        # Track consecutive zero inserts (all duplicates)
                        if inserted_count == 0:
                            consecutive_zero_inserts += 1
                            if consecutive_zero_inserts >= 3:
                                logger.info(f"3 consecutive pages with no new data, stopping sync for {source_name}")
                                break
                        else:
                            consecutive_zero_inserts = 0

                        # Track latest data timestamp (most recent transaction)
                        if pages_processed == 0:
                            # First page has the newest data
                            latest_data_ts = swap_records[0].get('timestamp')
                            if not latest_data_ts:
                                # Fallback: find max timestamp in first batch
                                timestamps = [s.get('timestamp') for s in swap_records if s.get('timestamp')]
                                latest_data_ts = max(timestamps) if timestamps else None
                        else:
                            latest_data_ts = None
                    else:
                        consecutive_zero_inserts += 1
                        if consecutive_zero_inserts >= 3:
                            logger.info(f"3 consecutive pages with no new data, stopping sync for {source_name}")
                            break
                        latest_data_ts = None

                    # Update sync status - handle different pagination formats
                    if source_name == 'lifi':
                        next_token = data.get('next') if data.get('hasNext', False) else None
                    else:
                        # THORChain and MayaChain
                        next_token = data.get('nextPageToken') or data.get('meta', {}).get('nextPageToken')

                    update_params = {
                        'next_page_token': next_token,
                        'last_synced_timestamp': datetime.utcnow(),
                        'error_count': 0,
                        'last_error': None
                    }

                    # Only update latest_data_timestamp if we found new data on first page
                    if latest_data_ts:
                        update_params['latest_data_timestamp'] = latest_data_ts

                    db_manager.update_sync_status(source_name, **update_params)
                    
                    if not next_token:
                        break

                    next_page_token = next_token
                    pages_processed += 1

                    # Stop if we've reached max pages
                    if pages_processed >= max_pages:
                        logger.info(f"Reached max pages ({max_pages}) for {source_name}, stopping")
                        break

                    # Add delay between requests
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing page for {source_name}: {e}")
                    db_manager.update_sync_status(
                        source_name,
                        error_count=sync_status.get('error_count', 0) + 1,
                        last_error=str(e)
                    )
                    break
            
            logger.info(f"Completed sync for {source_name}. Processed {total_processed} swaps across {pages_processed} pages")
            
            # Refresh materialized views
            db_manager.execute_query("SELECT refresh_materialized_views()")
            logger.info("Refreshed materialized views")
            
        except Exception as e:
            logger.error(f"Sync failed for {source_name}: {e}")
            if source_name != 'arkham':
                db_manager.update_sync_status(
                    source_name,
                    error_count=sync_status.get('error_count', 0) + 1,
                    last_error=str(e)
                )
    
    def sync_all_sources(self):
        """Sync all active sources in parallel"""
        logger.info("Starting parallel sync for all sources")

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(self.sync_source, src): src for src in self.ingestors.keys()}

            for future in as_completed(futures):
                source = futures[future]
                try:
                    future.result()
                    logger.info(f"✅ {source} sync completed")
                except Exception as e:
                    logger.error(f"❌ {source} sync failed: {e}")

        logger.info("Completed parallel sync for all sources")

def main():
    sync_service = SyncService()
    
    # Schedule sync every N minutes (configurable)
    schedule.every(config.SYNC_INTERVAL_MINUTES).minutes.do(sync_service.sync_all_sources)
    
    # Run initial sync
    sync_service.sync_all_sources()
    
    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()