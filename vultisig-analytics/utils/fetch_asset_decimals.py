"""
Asset Decimals Fetcher
Fetches and caches asset decimal information from Midgard Pools API
"""

import requests
import logging
from typing import Optional
from database.connection import DatabaseManager

logger = logging.getLogger(__name__)

MIDGARD_POOLS_API = 'https://midgard.ninerealms.com/v2/pools'
MAYA_POOLS_API = 'https://midgard.mayachain.info/v2/pools'


def fetch_and_cache_decimals(db: DatabaseManager) -> int:
    """
    Fetch asset decimals from Midgard Pools API and cache in database

    Returns:
        int: Number of assets cached
    """
    inserted_count = 0

    try:
        # Fetch THORChain pools
        logger.info('Fetching THORChain pool data...')
        response = requests.get(MIDGARD_POOLS_API, timeout=30)
        response.raise_for_status()
        thor_pools = response.json()
        logger.info(f'Fetched {len(thor_pools)} THORChain pools')

        # Fetch MayaChain pools
        logger.info('Fetching MayaChain pool data...')
        response = requests.get(MAYA_POOLS_API, timeout=30)
        response.raise_for_status()
        maya_pools = response.json()
        logger.info(f'Fetched {len(maya_pools)} MayaChain pools')

        all_pools = thor_pools + maya_pools

        for pool in all_pools:
            asset = pool.get('asset')  # e.g., "AVAX.AVAX", "ETH.USDC-0xA0B86..."
            native_decimal = pool.get('nativeDecimal')

            if not asset or native_decimal is None:
                continue

            # Parse asset: "AVAX.AVAX" -> chain="AVAX", symbol="AVAX"
            # "ETH.USDC-0xA0B..." -> chain="ETH", symbol="USDC", address="0xA0B..."
            parts = asset.split('.')
            if len(parts) != 2:
                logger.warning(f'Invalid asset format: {asset}')
                continue

            chain = parts[0]
            symbol_and_address = parts[1]

            # Extract contract address if present
            contract_address = None
            if '-' in symbol_and_address:
                symbol, contract_address = symbol_and_address.split('-', 1)
            else:
                symbol = symbol_and_address

            # Insert into database
            try:
                db.execute_query('''
                    INSERT INTO asset_decimals (asset_symbol, chain, decimal_places, contract_address, full_asset_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (full_asset_id) DO UPDATE
                    SET decimal_places = EXCLUDED.decimal_places,
                        contract_address = EXCLUDED.contract_address,
                        updated_at = NOW()
                ''', (symbol, chain, int(native_decimal), contract_address, asset))
                inserted_count += 1
            except Exception as e:
                logger.error(f'Error inserting {asset}: {e}')

        logger.info(f'Cached {inserted_count} asset decimals')
        return inserted_count

    except requests.RequestException as e:
        logger.error(f'Failed to fetch pool data: {e}')
        return 0
    except Exception as e:
        logger.error(f'Unexpected error in fetch_and_cache_decimals: {e}')
        return 0


def get_asset_decimal(asset: str, db: DatabaseManager) -> int:
    """
    Get decimal places for an asset, fetch from API if not cached

    Args:
        asset: Full asset ID (e.g., "AVAX.AVAX", "ETH.USDC-0xA0B...")
        db: DatabaseManager instance

    Returns:
        int: Number of decimal places (defaults to 8 if unknown)
    """
    try:
        result = db.execute_query(
            'SELECT decimal_places FROM asset_decimals WHERE full_asset_id = %s',
            (asset,),
            fetch='one'
        )

        if result:
            # Handle both dict and list return types
            if isinstance(result, dict):
                return result['decimal_places']
            elif isinstance(result, (list, tuple)) and len(result) > 0:
                # If it's a list, get the first result and access decimal_places
                first_result = result[0]
                if isinstance(first_result, dict):
                    return first_result['decimal_places']
                else:
                    return first_result  # Assume it's the decimal value itself

        # Not in cache, try to fetch fresh pool data
        logger.warning(f'Asset {asset} not in cache, fetching fresh data...')
        fetch_and_cache_decimals(db)

        # Retry query
        result = db.execute_query(
            'SELECT decimal_places FROM asset_decimals WHERE full_asset_id = %s',
            (asset,),
            fetch='one'
        )

        if result:
            if isinstance(result, dict):
                return result['decimal_places']
            elif isinstance(result, (list, tuple)) and len(result) > 0:
                first_result = result[0]
                if isinstance(first_result, dict):
                    return first_result['decimal_places']
                else:
                    return first_result

        # Fallback: use 8 decimals (common for most chains)
        logger.warning(f'No decimal info for {asset}, defaulting to 8')
        return 8

    except Exception as e:
        logger.error(f'Error getting decimal for {asset}: {e}')
        return 8


def convert_amount_with_decimals(raw_amount: str, asset: str, db: DatabaseManager) -> float:
    """
    Convert raw token amount to human-readable value using decimal places

    Args:
        raw_amount: Raw amount string (e.g., "220000000000")
        asset: Full asset ID (e.g., "AVAX.AVAX")
        db: DatabaseManager instance

    Returns:
        float: Converted amount
    """
    try:
        decimals = get_asset_decimal(asset, db)
        amount_int = int(raw_amount)
        converted = amount_int / (10 ** decimals)
        return converted
    except (ValueError, TypeError) as e:
        logger.error(f'Error converting amount {raw_amount} for asset {asset}: {e}')
        return 0.0


if __name__ == '__main__':
    # Test the fetcher
    logging.basicConfig(level=logging.INFO)
    db = DatabaseManager()

    print('Fetching asset decimals from Midgard...')
    count = fetch_and_cache_decimals(db)
    print(f'Successfully cached {count} assets')

    # Test some lookups
    test_assets = ['AVAX.AVAX', 'ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48', 'THOR.RUNE']
    for asset in test_assets:
        decimal = get_asset_decimal(asset, db)
        print(f'{asset}: {decimal} decimals')

        # Test conversion
        raw = '220000000000'
        converted = convert_amount_with_decimals(raw, asset, db)
        print(f'  {raw} raw -> {converted} converted')
