# main.py
from __future__ import annotations

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
from ingestors.etherscan_ingestor import EtherscanIngestor
from ingestors.router_source_classifier import (
    reclassify_all as reclassify_etherscan_rows,
    sync_attributed_gap_rows,
)
from ingestors.swapkit_senders import backfill_swapkit_rows
from ingestors.chainflip import ChainflipIngestor
from ingestors.near_intents import NearIntentsIngestor
from ingestors.near_intents_accrual import NearIntentsAccrualReader
from ingestors.rpc_fee_ingestor import RpcFeeIngestor
from ingestors.vult_holders import VultHoldersIngestor
from enrichers.enrich_arkham_volumes import VolumeEnricher

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

MIDGARD_RECONCILIATION_SOURCES = frozenset({'thorchain', 'mayachain'})
VOLUME_SYNC_SOURCES = frozenset({'chainflip', 'near-intents', 'near-intents-accrual', 'rpc-fees'})
MAX_PAGES_PER_SYNC = 10
DUPLICATE_PAGE_STOP_LIMIT = 3


def extract_midgard_tx_hash(action: object) -> str | None:
    if not isinstance(action, dict):
        return None

    action_inputs = action.get('in')
    if not isinstance(action_inputs, list) or not action_inputs:
        return None

    first_input = action_inputs[0]
    if not isinstance(first_input, dict):
        return None

    tx_hash = first_input.get('txID')
    return tx_hash if isinstance(tx_hash, str) and tx_hash else None


class SyncService:
    def __init__(self):
        self.ingestors = {
            'etherscan': EtherscanIngestor(),
            'thorchain': THORChainIngestor(),
            'mayachain': MayaChainIngestor(),
            'lifi': LiFiIngestor(),
            'chainflip': ChainflipIngestor(),
            'near-intents': NearIntentsIngestor(),
            'near-intents-accrual': NearIntentsAccrualReader(),
            'rpc-fees': RpcFeeIngestor(),
        }

    def sync_source(self, source_name: str):
        """Sync data from a specific source"""
        logger.info(f"Starting sync for {source_name}")

        sync_status = {}
        try:
            ingestor = self.ingestors[source_name]

            # EtherscanIngestor (replaces Arkham) iterates the configured
            # fee receivers and returns one result per (sub-)source. Each
            # gets its own sync_status row so the SystemStatus widget can
            # show per-aggregator health.
            if source_name == 'etherscan':
                try:
                    results = ingestor.ingest()
                    logger.info(f"Completed Etherscan sync: {results}")
                except Exception as e:
                    logger.error(f"Etherscan orchestrator crashed: {e}")
                    results = [
                        {'source': src, 'inserted': 0, 'error': str(e)}
                        for src, _addr in ingestor.integrators
                    ]
                for r in results:
                    db_manager.update_sync_status(
                        r['source'],
                        next_page_token=None,
                        last_synced_timestamp=datetime.utcnow(),
                        error_count=0 if r['error'] is None else 1,
                        last_error=r['error'],
                    )

                # Router-source classifier: verify each fresh row's tx.to
                # against the known-router set for its tagged protocol;
                # demote non-router transfers to 'other'. Fail-open on API
                # errors — unclassified rows retry on the next sync.
                try:
                    with db_manager.get_connection() as conn:
                        reclassify_etherscan_rows(ingestor.api_key, conn)
                except Exception as e:
                    logger.error(f"Router-source classifier crashed: {e}")

                # After router demotes SKWrap to `other`, re-tag allowlisted senders.
                try:
                    with db_manager.get_connection() as conn:
                        promoted = backfill_swapkit_rows(conn)
                        conn.commit()
                    logger.info(f"SwapKit sender backfill: {promoted} rows promoted")
                except Exception as e:
                    logger.error(f"SwapKit sender backfill crashed: {e}")

                # Attributed swaps without an on-chain fee transfer (fee-less
                # or native-fee) get a synthetic revenue row so they aren't
                # credited nowhere after leaving the lifi series.
                try:
                    with db_manager.get_connection() as conn:
                        gap_rows = sync_attributed_gap_rows(conn)
                    logger.info(f"Attribution gap filler: {gap_rows} synthetic fee rows inserted")
                except Exception as e:
                    logger.error(f"Attribution gap filler crashed: {e}")

                try:
                    logger.info("Running Arkham volume enrichment for missing token/volume fields")
                    enricher = VolumeEnricher()
                    try:
                        enricher.enrich_all_missing_volumes()
                    finally:
                        enricher.close()
                except Exception as e:
                    logger.error(f"Arkham volume enrichment crashed: {e}")
                return

            if source_name in VOLUME_SYNC_SOURCES:
                self._sync_volume_source(source_name, ingestor)
                return

            sync_status = None
            try:
                sync_status = db_manager.get_sync_status(source_name)
            except Exception as e:
                logger.warning(f"Could not fetch sync status for {source_name}: {e}")
            
            if not sync_status:
                logger.info(f"No sync status found for {source_name}, starting fresh")
                sync_status = {}
            
            # Midgard can index actions after newer actions have already been
            # returned. Those sources must rescan a bounded page window rather
            # than treating the latest stored transaction as a complete cursor.
            reconcile_late_actions = source_name in MIDGARD_RECONCILIATION_SOURCES

            # Non-Midgard sources retain the efficient latest-transaction stop.
            latest_tx_hash = None
            if not reconcile_late_actions:
                try:
                    results = db_manager.execute_query(
                        "SELECT tx_hash FROM swaps WHERE source = %s ORDER BY timestamp DESC LIMIT 1",
                        (source_name,),
                        fetch=True
                    )
                    if results:
                        latest_tx_hash = results[0]['tx_hash']
                        logger.info(f"Latest {source_name} tx in DB: {latest_tx_hash}")
                except Exception as e:
                    logger.warning(f"Could not fetch latest tx_hash for {source_name}: {e}")
            
            # Start fresh from page 1 (latest data) instead of using potentially expired token
            # This ensures we always get the newest data first
            next_page_token = None
            total_processed = 0
            pages_processed = 0
            found_existing_data = False
            max_pages = MAX_PAGES_PER_SYNC  # Bound rolling reconciliation work
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

                    # Avoid re-enriching every duplicate in the rolling Midgard
                    # window. A page-level lookup leaves only genuinely unseen
                    # actions for parsing while preserving fail-open behavior.
                    known_tx_hashes = set()
                    if reconcile_late_actions:
                        raw_tx_hashes = [
                            extract_midgard_tx_hash(action)
                            for action in actions
                        ]
                        raw_tx_hashes = [tx_hash for tx_hash in raw_tx_hashes if tx_hash]
                        if raw_tx_hashes:
                            try:
                                existing_rows = db_manager.execute_query(
                                    "SELECT tx_hash FROM swaps WHERE source = %s AND tx_hash = ANY(%s)",
                                    (source_name, raw_tx_hashes),
                                    fetch=True
                                )
                                known_tx_hashes = {row['tx_hash'] for row in existing_rows}
                            except Exception as e:
                                logger.warning(
                                    f"Could not fetch existing transaction hashes for {source_name}: {e}"
                                )
                    
                    # Parse and prepare swap data
                    swap_records = []
                    for action in actions:
                        if reconcile_late_actions:
                            raw_tx_hash = extract_midgard_tx_hash(action)
                            if raw_tx_hash in known_tx_hashes:
                                continue

                        parsed_swap = ingestor.parse_swap(action)
                        if parsed_swap:
                            # Check if we've reached data we already have
                            if latest_tx_hash and parsed_swap.get('tx_hash') == latest_tx_hash:
                                logger.info(f"Reached existing data at tx {latest_tx_hash}, stopping sync")
                                found_existing_data = True
                                break
                            swap_records.append(parsed_swap)

                    # Insert into database
                    if swap_records:
                        inserted_count = db_manager.insert_swaps(swap_records)
                        total_processed += inserted_count
                        logger.info(f"Inserted {inserted_count} swaps from page {pages_processed + 1}")

                        # Track consecutive zero inserts (all duplicates)
                        if inserted_count == 0:
                            consecutive_zero_inserts += 1
                            if (
                                not reconcile_late_actions
                                and consecutive_zero_inserts >= DUPLICATE_PAGE_STOP_LIMIT
                            ):
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
                        if (
                            not reconcile_late_actions
                            and consecutive_zero_inserts >= DUPLICATE_PAGE_STOP_LIMIT
                        ):
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

                    if found_existing_data:
                        break

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
            if source_name != 'etherscan':
                db_manager.update_sync_status(
                    source_name,
                    error_count=sync_status.get('error_count', 0) + 1,
                    last_error=str(e)
                )

    def _sync_volume_source(self, source_name: str, ingestor):
        status = db_manager.get_sync_status(source_name) or {}
        result = ingestor.ingest(status.get('next_page_token'))
        error = result.get('error')
        # Progress made before a failure is real; only the "healthy at" stamp waits for success.
        update = {
            'next_page_token': result.get('next_state'),
            'error_count': 0 if error is None else int(status.get('error_count') or 0) + 1,
            'last_error': error,
        }
        if result.get('latest_ts') is not None:
            update['latest_data_timestamp'] = result['latest_ts']
        if error is None:
            update['last_synced_timestamp'] = datetime.utcnow()
        db_manager.update_sync_status(source_name, **update)
        logger.info(
            f"Completed {source_name} volume sync: inserted={result.get('inserted')} "
            f"pages={result.get('pages')} error={error}"
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

def sync_vult_holders():
    """Sync VULT token holder data (runs daily)"""
    logger.info("Starting VULT holders daily sync")
    try:
        ingestor = VultHoldersIngestor()
        ingestor.ingest()
        logger.info("✅ VULT holders sync completed")
    except Exception as e:
        logger.error(f"❌ VULT holders sync failed: {e}")


def main():
    sync_service = SyncService()

    # Schedule swap data sync every N minutes (configurable)
    schedule.every(config.SYNC_INTERVAL_MINUTES).minutes.do(sync_service.sync_all_sources)

    # Schedule VULT holders sync daily at UTC 00:00
    schedule.every().day.at("00:00").do(sync_vult_holders)

    # Run initial sync for swap data
    sync_service.sync_all_sources()

    # Run initial VULT holders sync if data is stale (more than 24h old)
    try:
        results = db_manager.execute_query(
            "SELECT value FROM vult_holders_metadata WHERE key = 'last_updated'",
            fetch=True
        )
        if results:
            last_updated = datetime.fromisoformat(results[0]['value'].replace('Z', '+00:00'))
            if datetime.now(last_updated.tzinfo) - last_updated > timedelta(hours=24):
                logger.info("VULT holders data is stale (>24h), running initial sync")
                sync_vult_holders()
        else:
            logger.info("No VULT holders data found, running initial sync")
            sync_vult_holders()
    except Exception as e:
        logger.warning(f"Could not check VULT holders staleness: {e}")

    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
