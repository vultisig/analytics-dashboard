"""
VULT Token Holder Ingestor
Fetches VULT token holders and THORGuard NFT holders from Moralis API,
calculates tier distribution, and stores in database.

Run daily at UTC 00:00 via cron:
0 0 * * * /path/to/python /path/to/vult_holders.py
"""

import os
import json
import logging
import requests
import psycopg2
from datetime import datetime, timezone
from typing import List, Dict, Set, Optional
from decimal import Decimal
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
MORALIS_API_KEY = os.getenv('MORALIS_API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')
MORALIS_API_BASE = 'https://deep-index.moralis.io/api/v2.2'

# Contract addresses (Ethereum mainnet)
VULT_TOKEN_ADDRESS = '0xb788144df611029c60b859df47e79b7726c4deba'
THORGUARD_NFT_ADDRESS = '0xa98b29a8f5a247802149c268ecf860b8308b7291'

# VULT token has 18 decimals
VULT_DECIMALS = 18

# Tier thresholds (in VULT tokens, not raw units)
TIER_THRESHOLDS = [
    ('Ultimate', 1_000_000),
    ('Diamond', 100_000),
    ('Platinum', 15_000),
    ('Gold', 7_500),
    ('Silver', 3_000),
    ('Bronze', 1_500),
    ('None', 0),
]

# Tier order for boost calculation (index = tier level)
TIER_ORDER = ['None', 'Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond', 'Ultimate']

# THORGuard NFT can boost tier by 1 level, max to Platinum
MAX_BOOST_TIER = 'Platinum'

# Path to blacklist config file
BLACKLIST_CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'blacklist.json')


class VultHoldersIngestor:
    """Ingests VULT token holder data and calculates tier distribution."""

    def __init__(self):
        if not MORALIS_API_KEY:
            raise ValueError("MORALIS_API_KEY environment variable not set")
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable not set")

        self.api_key = MORALIS_API_KEY
        self.database_url = DATABASE_URL
        self.db = None
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'X-API-Key': self.api_key
        })

    def _get_connection(self):
        """Get or refresh database connection."""
        try:
            if self.db and not self.db.closed:
                cursor = self.db.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return self.db
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logger.warning("Database connection lost, reconnecting...")
            if self.db:
                try:
                    self.db.close()
                except:
                    pass

        self.db = psycopg2.connect(self.database_url)
        logger.info("Database connection established")
        return self.db

    def load_blacklist_from_config(self) -> List[Dict]:
        """Load blacklist entries from config file."""
        try:
            with open(BLACKLIST_CONFIG_PATH, 'r') as f:
                config = json.load(f)
                return config.get('blacklist', [])
        except FileNotFoundError:
            logger.warning(f"Blacklist config not found at {BLACKLIST_CONFIG_PATH}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing blacklist config: {e}")
            return []

    def sync_blacklist_to_db(self, blacklist_entries: List[Dict]):
        """Sync blacklist from config file to database."""
        db = self._get_connection()
        cursor = db.cursor()

        for entry in blacklist_entries:
            address = entry.get('address', '').lower()
            description = entry.get('description', '')
            if address:
                cursor.execute("""
                    INSERT INTO vult_holders_blacklist (address, description)
                    VALUES (%s, %s)
                    ON CONFLICT (address) DO UPDATE SET description = EXCLUDED.description
                """, (address, description))

        db.commit()
        cursor.close()
        logger.info(f"Synced {len(blacklist_entries)} blacklist entries from config to database")

    def get_blacklisted_addresses(self) -> Set[str]:
        """Load blacklist from config, sync to DB, and return addresses."""
        # Load from config file
        blacklist_entries = self.load_blacklist_from_config()

        # Sync to database
        if blacklist_entries:
            self.sync_blacklist_to_db(blacklist_entries)

        # Return all addresses from database (includes any manually added)
        db = self._get_connection()
        cursor = db.cursor()
        cursor.execute("SELECT LOWER(address) FROM vult_holders_blacklist")
        blacklist = {row[0] for row in cursor.fetchall()}
        cursor.close()
        logger.info(f"Loaded {len(blacklist)} blacklisted addresses")
        return blacklist

    def fetch_vult_holders(self) -> List[Dict]:
        """
        Fetch all VULT token holders from Moralis API.
        Returns list of {address, balance} dicts.
        """
        holders = []
        cursor = None

        logger.info(f"Fetching VULT token holders from Moralis...")

        while True:
            url = f'{MORALIS_API_BASE}/erc20/{VULT_TOKEN_ADDRESS}/owners'
            params = {'chain': 'eth', 'limit': 100}
            if cursor:
                params['cursor'] = cursor

            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                result = data.get('result', [])
                for holder in result:
                    address = holder.get('owner_address', '').lower()
                    balance_raw = holder.get('balance', '0')
                    # Convert from raw units (18 decimals) to VULT tokens
                    balance = Decimal(balance_raw) / Decimal(10 ** VULT_DECIMALS)
                    holders.append({
                        'address': address,
                        'balance': float(balance)
                    })

                cursor = data.get('cursor')
                logger.info(f"Fetched {len(holders)} VULT holders so far...")

                if not cursor:
                    break

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching VULT holders: {e}")
                break

        logger.info(f"Total VULT holders fetched: {len(holders)}")
        return holders

    def fetch_thorguard_holders(self) -> Set[str]:
        """
        Fetch all THORGuard NFT holders from Moralis API.
        Returns set of addresses (lowercase).
        """
        holders = set()
        cursor = None

        logger.info(f"Fetching THORGuard NFT holders from Moralis...")

        while True:
            url = f'{MORALIS_API_BASE}/nft/{THORGUARD_NFT_ADDRESS}/owners'
            params = {'chain': 'eth', 'limit': 100}
            if cursor:
                params['cursor'] = cursor

            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                result = data.get('result', [])
                for holder in result:
                    address = holder.get('owner_of', '').lower()
                    if address:
                        holders.add(address)

                cursor = data.get('cursor')
                logger.info(f"Fetched {len(holders)} THORGuard holders so far...")

                if not cursor:
                    break

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching THORGuard holders: {e}")
                break

        logger.info(f"Total THORGuard holders fetched: {len(holders)}")
        return holders

    def calculate_base_tier(self, balance: float) -> str:
        """Calculate tier based on VULT balance alone."""
        for tier_name, threshold in TIER_THRESHOLDS:
            if balance >= threshold:
                return tier_name
        return 'None'

    def calculate_effective_tier(self, base_tier: str, has_thorguard: bool) -> str:
        """
        Calculate effective tier after THORGuard NFT boost.
        THORGuard boosts tier by 1 level, max to Platinum.
        """
        if not has_thorguard:
            return base_tier

        base_index = TIER_ORDER.index(base_tier)
        max_boost_index = TIER_ORDER.index(MAX_BOOST_TIER)

        # Boost by 1 level, but cap at Platinum
        boosted_index = min(base_index + 1, max_boost_index)

        # Don't boost beyond Ultimate if somehow at Diamond+
        boosted_index = min(boosted_index, len(TIER_ORDER) - 1)

        return TIER_ORDER[boosted_index]

    def clear_holders_table(self):
        """Clear existing holder data before refresh."""
        db = self._get_connection()
        cursor = db.cursor()
        cursor.execute("TRUNCATE TABLE vult_holders")
        db.commit()
        cursor.close()
        logger.info("Cleared vult_holders table")

    def insert_holders(self, holders: List[Dict], thorguard_holders: Set[str], blacklist: Set[str]):
        """
        Insert holder data into database.
        Filters out blacklisted addresses.
        """
        db = self._get_connection()
        cursor = db.cursor()

        inserted_count = 0
        skipped_count = 0

        for holder in holders:
            address = holder['address']
            balance = holder['balance']

            # Skip blacklisted addresses
            if address in blacklist:
                skipped_count += 1
                continue

            has_thorguard = address in thorguard_holders
            base_tier = self.calculate_base_tier(balance)
            effective_tier = self.calculate_effective_tier(base_tier, has_thorguard)

            cursor.execute("""
                INSERT INTO vult_holders (address, vult_balance, has_thorguard, base_tier, effective_tier, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (address) DO UPDATE SET
                    vult_balance = EXCLUDED.vult_balance,
                    has_thorguard = EXCLUDED.has_thorguard,
                    base_tier = EXCLUDED.base_tier,
                    effective_tier = EXCLUDED.effective_tier,
                    updated_at = NOW()
            """, (address, balance, has_thorguard, base_tier, effective_tier))

            inserted_count += 1

            if inserted_count % 1000 == 0:
                db.commit()
                logger.info(f"Inserted {inserted_count} holders...")

        db.commit()
        cursor.close()
        logger.info(f"Inserted {inserted_count} holders, skipped {skipped_count} blacklisted")

    def update_tier_stats(self):
        """Calculate and update aggregated tier statistics."""
        db = self._get_connection()
        cursor = db.cursor()

        # Calculate stats per tier
        cursor.execute("""
            SELECT
                effective_tier,
                COUNT(*) as holder_count,
                COALESCE(SUM(vult_balance), 0) as total_balance,
                COALESCE(AVG(vult_balance), 0) as avg_balance,
                COUNT(*) FILTER (WHERE has_thorguard AND base_tier != effective_tier) as boosted_count
            FROM vult_holders
            GROUP BY effective_tier
        """)

        tier_stats = {row[0]: {
            'holder_count': row[1],
            'total_balance': float(row[2]),
            'avg_balance': float(row[3]),
            'boosted_count': row[4]
        } for row in cursor.fetchall()}

        # Update tier stats table
        for tier in TIER_ORDER:
            stats = tier_stats.get(tier, {
                'holder_count': 0,
                'total_balance': 0,
                'avg_balance': 0,
                'boosted_count': 0
            })

            cursor.execute("""
                UPDATE vult_tier_stats SET
                    holder_count = %s,
                    total_vult_balance = %s,
                    avg_vult_balance = %s,
                    thorguard_boosted_count = %s,
                    updated_at = NOW()
                WHERE tier = %s
            """, (
                stats['holder_count'],
                stats['total_balance'],
                stats['avg_balance'],
                stats['boosted_count'],
                tier
            ))

        db.commit()
        cursor.close()
        logger.info("Updated tier statistics")

    def update_metadata(self, total_holders: int, total_supply_held: float, thorguard_holders: int):
        """Update metadata table with summary info."""
        db = self._get_connection()
        cursor = db.cursor()

        now = datetime.now(timezone.utc).isoformat()

        updates = [
            ('last_updated', now),
            ('total_holders', str(total_holders)),
            ('total_supply_held', str(total_supply_held)),
            ('thorguard_holders', str(thorguard_holders)),
        ]

        for key, value in updates:
            cursor.execute("""
                UPDATE vult_holders_metadata SET value = %s, updated_at = NOW()
                WHERE key = %s
            """, (value, key))

        db.commit()
        cursor.close()
        logger.info(f"Updated metadata: {total_holders} holders, {total_supply_held:.2f} VULT held")

    def ingest(self):
        """Main ingestion process."""
        try:
            logger.info("Starting VULT holders ingestion")

            # Get blacklisted addresses
            blacklist = self.get_blacklisted_addresses()

            # Fetch data from Moralis
            vult_holders = self.fetch_vult_holders()
            thorguard_holders = self.fetch_thorguard_holders()

            if not vult_holders:
                logger.warning("No VULT holders fetched, aborting")
                return

            # Clear and re-insert (full refresh)
            self.clear_holders_table()

            # Insert holder data
            self.insert_holders(vult_holders, thorguard_holders, blacklist)

            # Calculate aggregated stats
            self.update_tier_stats()

            # Calculate totals for metadata (excluding blacklisted)
            valid_holders = [h for h in vult_holders if h['address'] not in blacklist]
            total_holders = len(valid_holders)
            total_supply_held = sum(h['balance'] for h in valid_holders)
            thorguard_count = len([h for h in valid_holders if h['address'] in thorguard_holders])

            # Update metadata
            self.update_metadata(total_holders, total_supply_held, thorguard_count)

            logger.info("VULT holders ingestion completed successfully")

            # Print summary
            db = self._get_connection()
            cursor = db.cursor()
            cursor.execute("""
                SELECT tier, holder_count, avg_vult_balance, thorguard_boosted_count
                FROM vult_tier_stats
                ORDER BY CASE tier
                    WHEN 'Ultimate' THEN 1
                    WHEN 'Diamond' THEN 2
                    WHEN 'Platinum' THEN 3
                    WHEN 'Gold' THEN 4
                    WHEN 'Silver' THEN 5
                    WHEN 'Bronze' THEN 6
                    WHEN 'None' THEN 7
                END
            """)
            logger.info("\nTier Distribution:")
            for row in cursor.fetchall():
                logger.info(f"  {row[0]}: {row[1]} holders, avg {row[2]:.2f} VULT, {row[3]} boosted")
            cursor.close()

        except Exception as e:
            logger.error(f"Error during ingestion: {e}")
            if self.db:
                self.db.rollback()
            raise
        finally:
            if self.db:
                self.db.close()


def main():
    """Entry point for VULT holders ingestor."""
    ingestor = VultHoldersIngestor()
    ingestor.ingest()


if __name__ == '__main__':
    main()
