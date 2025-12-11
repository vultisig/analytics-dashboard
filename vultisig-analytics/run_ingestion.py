#!/usr/bin/env python3
"""
Run all ingestors to fetch fresh data with complete fields
"""
import logging
import sys
from database.connection import db_manager
from ingestors.thorchain import THORChainIngestor
from ingestors.mayachain import MayaChainIngestor
from ingestors.lifi import LiFiIngestor
from ingestors.arkham_ingestor import ArkhamIngestor
from utils.fetch_asset_decimals import fetch_and_cache_decimals

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def run_thorchain(limit=50):
    """Run THORChain ingestor"""
    logger.info("=" * 60)
    logger.info("Running THORChain Ingestor")
    logger.info("=" * 60)

    ingestor = THORChainIngestor()
    data = ingestor.fetch_data(limit=limit)
    actions = data.get('actions', [])

    logger.info(f"Fetched {len(actions)} THORChain actions")

    swap_records = []
    for action in actions:
        parsed = ingestor.parse_swap(action)
        if parsed:
            swap_records.append(parsed)

    logger.info(f"Parsed {len(swap_records)} valid swaps")

    if swap_records:
        inserted = db_manager.insert_swaps(swap_records)
        logger.info(f"✓ Inserted {inserted} THORChain swaps")

    return len(swap_records)

def run_mayachain(limit=50):
    """Run MayaChain ingestor"""
    logger.info("=" * 60)
    logger.info("Running MayaChain Ingestor")
    logger.info("=" * 60)

    ingestor = MayaChainIngestor()
    data = ingestor.fetch_data(limit=limit)
    actions = data.get('actions', [])

    logger.info(f"Fetched {len(actions)} MayaChain actions")

    swap_records = []
    for action in actions:
        parsed = ingestor.parse_swap(action)
        if parsed:
            swap_records.append(parsed)

    logger.info(f"Parsed {len(swap_records)} valid swaps")

    if swap_records:
        inserted = db_manager.insert_swaps(swap_records)
        logger.info(f"✓ Inserted {inserted} MayaChain swaps")

    return len(swap_records)

def run_lifi(limit=50):
    """Run LiFi ingestor"""
    logger.info("=" * 60)
    logger.info("Running LiFi Ingestor")
    logger.info("=" * 60)

    ingestor = LiFiIngestor()
    data = ingestor.fetch_data(limit=limit)
    transfers = data.get('data', [])

    logger.info(f"Fetched {len(transfers)} LiFi transfers")

    swap_records = []
    for transfer in transfers:
        parsed = ingestor.parse_swap(transfer)
        if parsed:
            swap_records.append(parsed)

    logger.info(f"Parsed {len(swap_records)} valid swaps")

    if swap_records:
        inserted = db_manager.insert_swaps(swap_records)
        logger.info(f"✓ Inserted {inserted} LiFi swaps")

    return len(swap_records)

def run_arkham():
    """Run Arkham ingestor"""
    logger.info("=" * 60)
    logger.info("Running Arkham Ingestor")
    logger.info("=" * 60)

    ingestor = ArkhamIngestor()
    ingestor.ingest()
    logger.info("✓ Completed Arkham ingestion")

def main():
    """Run all ingestors"""
    logger.info("Starting ingestion of all sources with new schema")
    logger.info("")

    # First, fetch and cache asset decimals
    logger.info("=" * 60)
    logger.info("Fetching Asset Decimals from Midgard Pools API")
    logger.info("=" * 60)
    try:
        count = fetch_and_cache_decimals(db_manager)
        logger.info(f"✓ Cached {count} asset decimals")
    except Exception as e:
        logger.error(f"Error fetching decimals: {e}")

    logger.info("")

    # Run each ingestor
    total_swaps = 0

    try:
        count = run_thorchain(limit=50)
        total_swaps += count
    except Exception as e:
        logger.error(f"THORChain ingestion failed: {e}")

    logger.info("")

    try:
        count = run_mayachain(limit=50)
        total_swaps += count
    except Exception as e:
        logger.error(f"MayaChain ingestion failed: {e}")

    logger.info("")

    try:
        count = run_lifi(limit=50)
        total_swaps += count
    except Exception as e:
        logger.error(f"LiFi ingestion failed: {e}")

    logger.info("")

    try:
        run_arkham()
    except Exception as e:
        logger.error(f"Arkham ingestion failed: {e}")

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"INGESTION COMPLETE - Total new swaps: {total_swaps}")
    logger.info("=" * 60)

if __name__ == '__main__':
    main()
