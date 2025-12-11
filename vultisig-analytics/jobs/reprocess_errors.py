#!/usr/bin/env python3
"""
Background job to reprocess ingestion errors
Retries transactions that failed due to missing prices or other recoverable errors
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')
MAX_RETRY_COUNT = 10

def get_failed_transactions():
    """Get transactions that need reprocessing"""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        cursor.execute("""
            SELECT id, tx_hash, source, error_type, error_message, 
                   retry_count, raw_data, created_at
            FROM ingestion_errors
            WHERE retry_count < %s
            AND error_type = 'missing_price'
            ORDER BY created_at DESC
        """, (MAX_RETRY_COUNT,))
        
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

def reprocess_transaction(error_record):
    """Attempt to reprocess a single transaction"""
    tx_hash = error_record['tx_hash']
    source = error_record['source']
    raw_data = error_record['raw_data']
    
    logger.info(f"Reprocessing {source} transaction: {tx_hash}")
    
    try:
        # Import the appropriate ingestor
        if source == 'thorchain':
            from ingestors.thorchain import THORChainIngestor
            ingestor = THORChainIngestor()
        elif source == 'mayachain':
            from ingestors.mayachain import MayaChainIngestor
            ingestor = MayaChainIngestor()
        else:
            logger.warning(f"Unknown source: {source}, skipping")
            return False
        
        # Parse and ingest the transaction
        parsed = ingestor.parse_swap(raw_data)
        
        if parsed is None:
            # Still failed, will be retried later
            logger.warning(f"Transaction {tx_hash} still failed to parse")
            return False
        
        # If parsing succeeded, insert to database
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO swaps (
                    timestamp, date_only, source, tx_hash, block_height,
                    user_address, in_asset, in_amount, in_amount_usd,
                    out_asset, out_amount, out_amount_usd,
                    total_fee_usd, network_fee_usd, liquidity_fee_usd, affiliate_fee_usd,
                    pool_1, pool_2, is_streaming_swap, swap_slip, volume_tier, platform, raw_data
                ) VALUES (
                    %(timestamp)s, %(date_only)s, %(source)s, %(tx_hash)s, %(block_height)s,
                    %(user_address)s, %(in_asset)s, %(in_amount)s, %(in_amount_usd)s,
                    %(out_asset)s, %(out_amount)s, %(out_amount_usd)s,
                    %(total_fee_usd)s, %(network_fee_usd)s, %(liquidity_fee_usd)s, %(affiliate_fee_usd)s,
                    %(pool_1)s, %(pool_2)s, %(is_streaming_swap)s, %(swap_slip)s, %(volume_tier)s,
                    %(platform)s, %(raw_data)s
                )
                ON CONFLICT (tx_hash) DO UPDATE SET
                    in_amount_usd = EXCLUDED.in_amount_usd,
                    total_fee_usd = EXCLUDED.total_fee_usd,
                    updated_at = NOW()
            """, parsed)
            conn.commit()
            
            # Successfully ingested, delete from error table
            cursor.execute("""
                DELETE FROM ingestion_errors
                WHERE tx_hash = %s AND source = %s
            """, (tx_hash, source))
            conn.commit()
            
            logger.info(f"✅ Successfully reprocessed {tx_hash}")
            return True
            
        finally:
            cursor.close()
            conn.close()
    
    except Exception as e:
        logger.error(f"Error reprocessing {tx_hash}: {e}")
        return False

def update_retry_count(error_id):
    """Increment retry count for a failed attempt"""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            UPDATE ingestion_errors
            SET retry_count = retry_count + 1,
                last_retry_at = NOW()
            WHERE id = %s
        """, (error_id,))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def main():
    logger.info("=== Starting Background Reprocessing Job ===")
    
    # Get failed transactions
    failed_txs = get_failed_transactions()
    total_count = len(failed_txs)
    
    if total_count == 0:
        logger.info("No failed transactions to reprocess")
        return
    
    logger.info(f"Found {total_count} failed transactions to retry")
    
    success_count = 0
    failed_count = 0
    
    for error_record in failed_txs:
        success = reprocess_transaction(error_record)
        
        if success:
            success_count += 1
        else:
            # Update retry count
            update_retry_count(error_record['id'])
            failed_count += 1
    
    logger.info(f"""
=== Reprocessing Summary ===
Total: {total_count}
Succeeded: {success_count}
Failed: {failed_count}
""")

if __name__ == '__main__':
    main()
