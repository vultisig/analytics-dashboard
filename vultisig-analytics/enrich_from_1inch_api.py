#!/usr/bin/env python3
"""
Enrich Arkham DEX aggregator records with swap volume data from 1inch API.
Uses 1inch History API to fetch transaction details including swap amounts.
"""

import os
import sys
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')
ONEINCH_API_KEY = os.getenv('ONEINCH_API_KEY')
ONEINCH_BASE = 'https://api.1inch.dev'

# Chain IDs supported by 1inch
CHAIN_IDS = {
    'Ethereum': 1,
    'Optimism': 10,
    'BSC': 56,
    'Polygon': 137,
    'Base': 8453,
    'Arbitrum': 42161,
    'Avalanche': 43114,
}


class OneInchEnricher:
    def __init__(self):
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable not set")
        if not ONEINCH_API_KEY:
            raise ValueError("ONEINCH_API_KEY environment variable not set")

        self.db = psycopg2.connect(DATABASE_URL)
        self.api_key = ONEINCH_API_KEY

    def fetch_transaction_details(self, tx_hash: str, chain_id: int) -> dict:
        """
        Fetch transaction details from 1inch History API.

        Returns:
            Transaction details including amounts, tokens, etc.
        """
        # Try different API endpoints
        endpoints = [
            f'{ONEINCH_BASE}/history/v2.0/history/{tx_hash}/events',
            f'{ONEINCH_BASE}/tx-gateway/v1.1/{chain_id}/transaction/{tx_hash}',
        ]

        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Accept': 'application/json'
        }

        for endpoint in endpoints:
            try:
                logger.debug(f"Trying endpoint: {endpoint}")
                response = requests.get(endpoint, headers=headers, timeout=30)

                if response.status_code == 200:
                    data = response.json()
                    logger.debug(f"Got response: {data}")
                    return data
                elif response.status_code == 404:
                    logger.debug(f"Transaction not found at {endpoint}")
                    continue
                else:
                    logger.warning(f"API returned {response.status_code}: {response.text}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching from {endpoint}: {e}")
                continue

        return None

    def parse_1inch_transaction(self, tx_data: dict) -> dict:
        """
        Parse 1inch transaction data to extract volume information.

        Returns:
            {
                'amount_in': float,
                'amount_out': float,
                'token_in_symbol': str,
                'token_in_address': str,
                'token_out_symbol': str,
                'token_out_address': str,
                'swap_volume_usd': float
            }
        """
        # The response structure varies by endpoint
        # Handle different response formats

        # Format 1: Direct transaction details
        if 'srcToken' in tx_data:
            return {
                'token_in_address': tx_data.get('srcToken', {}).get('address'),
                'token_in_symbol': tx_data.get('srcToken', {}).get('symbol'),
                'amount_in': float(tx_data.get('srcAmount', 0)) / (10 ** tx_data.get('srcToken', {}).get('decimals', 18)),
                'token_out_address': tx_data.get('dstToken', {}).get('address'),
                'token_out_symbol': tx_data.get('dstToken', {}).get('symbol'),
                'amount_out': float(tx_data.get('dstAmount', 0)) / (10 ** tx_data.get('dstToken', {}).get('decimals', 18)),
                'swap_volume_usd': tx_data.get('srcAmountUsd'),
            }

        # Format 2: Events array
        if 'events' in tx_data and isinstance(tx_data['events'], list):
            for event in tx_data['events']:
                if event.get('type') == 'swap':
                    src_token = event.get('srcToken', {})
                    dst_token = event.get('dstToken', {})
                    return {
                        'token_in_address': src_token.get('address'),
                        'token_in_symbol': src_token.get('symbol'),
                        'amount_in': float(event.get('srcAmount', 0)) / (10 ** src_token.get('decimals', 18)),
                        'token_out_address': dst_token.get('address'),
                        'token_out_symbol': dst_token.get('symbol'),
                        'amount_out': float(event.get('dstAmount', 0)) / (10 ** dst_token.get('decimals', 18)),
                        'swap_volume_usd': event.get('srcAmountUsd'),
                    }

        return None

    def enrich_record(self, record: dict) -> bool:
        """
        Enrich a single Arkham record with volume data from 1inch API.

        Returns:
            True if successfully enriched, False otherwise
        """
        tx_hash = record['tx_hash']
        chain = record['chain']

        chain_id = CHAIN_IDS.get(chain)
        if not chain_id:
            logger.warning(f"Unsupported chain: {chain}")
            return False

        logger.info(f"Enriching {tx_hash} on {chain}")

        # Fetch from 1inch API
        tx_data = self.fetch_transaction_details(tx_hash, chain_id)

        if not tx_data:
            logger.warning(f"Could not fetch transaction data from 1inch API for {tx_hash}")
            return False

        # Parse transaction data
        parsed = self.parse_1inch_transaction(tx_data)

        if not parsed:
            logger.warning(f"Could not parse transaction data for {tx_hash}")
            logger.debug(f"Raw data: {tx_data}")
            return False

        # Update database
        cursor = self.db.cursor()
        cursor.execute("""
            UPDATE dex_aggregator_revenue
            SET
                swap_volume_usd = %s,
                token_in_symbol = %s,
                token_in_address = %s,
                token_out_symbol = %s,
                token_out_address = %s,
                amount_in = %s,
                amount_out = %s,
                volume_data_source = '1inch_api',
                updated_at = NOW()
            WHERE tx_hash = %s
        """, (
            parsed.get('swap_volume_usd'),
            parsed.get('token_in_symbol'),
            parsed.get('token_in_address'),
            parsed.get('token_out_symbol'),
            parsed.get('token_out_address'),
            parsed.get('amount_in'),
            parsed.get('amount_out'),
            tx_hash
        ))

        logger.info(f"✓ Enriched {tx_hash}: {parsed.get('amount_in'):.4f} {parsed.get('token_in_symbol')} → {parsed.get('amount_out'):.4f} {parsed.get('token_out_symbol')} (${parsed.get('swap_volume_usd'):.2f if parsed.get('swap_volume_usd') else 0})")
        return True

    def enrich_all_missing_volumes(self, limit: int = None):
        """
        Enrich all Arkham records that are missing volume data.

        Args:
            limit: Maximum number of records to enrich (None = all)
        """
        cursor = self.db.cursor(cursor_factory=RealDictCursor)

        # Get records without volume data
        query = """
            SELECT tx_hash, chain, timestamp, actual_fee_usd, protocol
            FROM dex_aggregator_revenue
            WHERE swap_volume_usd IS NULL
              AND fee_data_source = 'arkham'
              AND protocol = '1inch'
            ORDER BY timestamp DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        cursor.execute(query)
        records = cursor.fetchall()

        if not records:
            logger.info("No records to enrich!")
            return

        logger.info(f"Found {len(records)} 1inch records to enrich")

        enriched_count = 0
        failed_count = 0

        for i, record in enumerate(records, 1):
            logger.info(f"\n[{i}/{len(records)}] Processing {record['tx_hash']}")

            try:
                success = self.enrich_record(record)

                if success:
                    enriched_count += 1
                else:
                    failed_count += 1

                # Commit every 10 records
                if i % 10 == 0:
                    self.db.commit()
                    logger.info(f"Committed progress: {enriched_count} enriched, {failed_count} failed")

                # Rate limit: 1inch API may have rate limits
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Error enriching {record['tx_hash']}: {e}")
                import traceback
                traceback.print_exc()
                failed_count += 1
                self.db.rollback()

        # Final commit
        self.db.commit()

        logger.info(f"\n{'='*80}")
        logger.info("ENRICHMENT COMPLETE")
        logger.info(f"{'='*80}")
        logger.info(f"Total records processed: {len(records)}")
        logger.info(f"Successfully enriched: {enriched_count}")
        logger.info(f"Failed: {failed_count}")
        if len(records) > 0:
            logger.info(f"Success rate: {enriched_count/len(records)*100:.1f}%")

    def close(self):
        """Close database connection."""
        if self.db:
            self.db.close()


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Enrich Arkham records with 1inch API data')
    parser.add_argument('--limit', type=int, help='Limit number of records to enrich')
    parser.add_argument('--test', action='store_true', help='Test mode: only enrich 5 records')

    args = parser.parse_args()

    try:
        enricher = OneInchEnricher()

        if args.test:
            logger.info("TEST MODE: Enriching 5 records only")
            enricher.enrich_all_missing_volumes(limit=5)
        else:
            enricher.enrich_all_missing_volumes(limit=args.limit)

        enricher.close()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
