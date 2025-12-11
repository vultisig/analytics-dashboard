"""
Arkham DEX Aggregator Ingestor
Fetches transfer data from Arkham API and ingests into database.
Uses actual fee amounts from Arkham as ground truth.
"""

import os
import logging
import requests
import psycopg2
from datetime import datetime
from typing import List, Dict, Optional
from .protocol_identifier import ProtocolIdentifier

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
ARKHAM_API_KEY = os.getenv('ARKHAM_API_KEY')
ARKHAM_API_BASE = 'https://api.arkhamintelligence.com'
INTEGRATOR_ADDRESS = '0xA4a4f610e89488EB4ECc6c63069f241a54485269'
DATABASE_URL = os.getenv('DATABASE_URL')

# Chain name normalization
CHAIN_MAPPING = {
    'ethereum': 'Ethereum',
    'bsc': 'BSC',
    'binance-smart-chain': 'BSC',
    'polygon': 'Polygon',
    'polygon-pos': 'Polygon',
    'arbitrum_one': 'Arbitrum',
    'arbitrum-one': 'Arbitrum',
    'optimism': 'Optimism',
    'base': 'Base',
    'avalanche': 'Avalanche',
    'blast': 'Blast',
}

class ArkhamIngestor:
    """Ingests DEX aggregator revenue data from Arkham API."""
    
    def __init__(self):
        if not ARKHAM_API_KEY:
            raise ValueError("ARKHAM_API_KEY environment variable not set")

        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable not set")

        self.api_key = ARKHAM_API_KEY
        self.database_url = DATABASE_URL
        self.db = None
        self.protocol_identifier = None

    def _get_connection(self):
        """Get or refresh database connection"""
        try:
            # Test if connection is alive
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

        # Create new connection
        self.db = psycopg2.connect(self.database_url)
        self.protocol_identifier = ProtocolIdentifier(self.db)
        logger.info("Database connection established")
        return self.db
        
    def fetch_all_transfers(self) -> List[Dict]:
        """
        Fetch transfers from Arkham API.
        Only fetches new transfers by checking latest timestamp in database.
        """
        url = f'{ARKHAM_API_BASE}/transfers'
        headers = {
            'Accept': 'application/json',
            'API-Key': self.api_key
        }
        
        # Get latest timestamp from database to avoid re-fetching
        try:
            db = self._get_connection()
            cursor = db.cursor()
            cursor.execute("SELECT MAX(timestamp) FROM dex_aggregator_revenue WHERE fee_data_source = 'arkham'")
            result = cursor.fetchone()
            latest_timestamp = result[0] if result and result[0] else None
            if latest_timestamp:
                logger.info(f"Latest Arkham timestamp in DB: {latest_timestamp}")
            cursor.close()
        except Exception as e:
            logger.warning(f"Could not fetch latest timestamp: {e}")
            latest_timestamp = None
        
        all_transfers = []
        offset = 0
        limit = 1000
        
        logger.info(f"Fetching transfers for integrator {INTEGRATOR_ADDRESS}")
        
        while True:
            params = {
                'base': INTEGRATOR_ADDRESS,
                'limit': limit,
                'offset': offset
            }
            
            try:
                response = requests.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                transfers = data.get('transfers', [])
                
                if not transfers:
                    break
                
                # Filter out transfers we already have
                new_transfers = []
                for transfer in transfers:
                    timestamp_str = transfer.get('blockTimestamp')
                    if timestamp_str and latest_timestamp:
                        try:
                            transfer_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            if transfer_time <= latest_timestamp:
                                # We've reached transfers we already have, stop fetching
                                logger.info(f"Reached existing transfers at {transfer_time}, stopping fetch")
                                return all_transfers
                        except:
                            pass
                    new_transfers.append(transfer)
                
                all_transfers.extend(new_transfers)
                logger.info(f"Fetched {len(new_transfers)} new transfers (offset {offset})")
                
                offset += len(transfers)
                
                # If we got fewer than limit, we're done
                if len(transfers) < limit:
                    break
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching transfers: {e}")
                break
        
        logger.info(f"Total new transfers fetched: {len(all_transfers)}")
        return all_transfers
    
    def normalize_chain(self, chain: str) -> str:
        """Normalize chain name to match our database format."""
        if not chain:
            return 'Unknown'
        return CHAIN_MAPPING.get(chain.lower(), chain.capitalize())
    
    def extract_address(self, addr_obj) -> str:
        """
        Extract address string from Arkham's address object.
        Arkham returns addresses as objects with nested structure.
        """
        if not addr_obj:
            return ''
        if isinstance(addr_obj, str):
            return addr_obj
        if isinstance(addr_obj, dict):
            return addr_obj.get('address', '')
        return ''
    
    def identify_protocol_from_arkham_entity(self, addr_obj) -> Optional[str]:
        """
        Extract protocol name from Arkham's arkhamEntity metadata.
        This is more reliable than address matching.
        """
        if not isinstance(addr_obj, dict):
            return None
        
        entity = addr_obj.get('arkhamEntity')
        if not entity or not isinstance(entity, dict):
            return None
        
        entity_id = entity.get('id', '').lower()
        entity_name = entity.get('name', '').lower()
        
        # Map Arkham entity names to our protocol names
        if '1inch' in entity_id or '1inch' in entity_name:
            return '1inch'
        elif 'paraswap' in entity_id or 'paraswap' in entity_name:
            return 'paraswap'
        elif 'cow' in entity_id or 'cowswap' in entity_name:
            return 'cowswap'
        elif 'matcha' in entity_id or '0x' in entity_name:
            return 'matcha'
        
        return None
    
    def insert_transfer(self, transfer: Dict):
        """
        Insert a single transfer into the database.
        Stores ALL available fields from Arkham API response.

        Args:
            transfer: Transfer data from Arkham API
        """
        try:
            tx_hash = transfer.get('transactionHash')
            if not tx_hash:
                logger.warning(f"Transfer missing transaction hash: {transfer}")
                return

            # Extract addresses from objects
            from_address_obj = transfer.get('fromAddress', {})
            from_address = self.extract_address(from_address_obj)
            to_address = self.extract_address(transfer.get('toAddress', {}))

            # Extract chain and normalize
            chain = self.normalize_chain(transfer.get('chain', ''))

            # Try to identify protocol from Arkham entity first (most reliable)
            protocol = self.identify_protocol_from_arkham_entity(from_address_obj)

            # If not found, use our protocol identifier
            if not protocol:
                protocol = self.protocol_identifier.identify_protocol(tx_hash, from_address, chain)

            # Extract fee data (what Arkham provides directly)
            actual_fee_usd = float(transfer.get('historicalUSD', 0))
            fee_token_symbol = transfer.get('tokenSymbol', '')
            fee_token_address = transfer.get('tokenAddress', '')
            fee_amount_raw = str(transfer.get('unitValue', ''))

            # Parse timestamp
            timestamp = transfer.get('blockTimestamp')
            if timestamp:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            else:
                timestamp = datetime.now()

            block_number = transfer.get('blockNumber')

            # NEW: Extract ALL available token/swap data from Arkham response
            # Note: Arkham may not provide swap details, only fee transfers
            # Token in/out data would need to be enriched from blockchain RPC
            token_in_address = None
            token_out_address = None
            token_in_symbol = None
            token_out_symbol = None
            amount_in = None
            amount_out = None
            swap_volume_usd = None
            volume_data_source = None

            # Some Arkham transfers may include transaction details
            # Check if this is a native token transfer (ETH, BNB, etc.)
            if fee_token_address == 'NATIVE' or not fee_token_address:
                token_in_address = 'NATIVE'
                # For native transfers, use the chain's native token symbol
                chain_native_tokens = {
                    'Ethereum': 'ETH',
                    'BSC': 'BNB',
                    'Polygon': 'MATIC',
                    'Arbitrum': 'ETH',
                    'Optimism': 'ETH',
                    'Base': 'ETH',
                    'Avalanche': 'AVAX',
                    'Blast': 'ETH'
                }
                token_in_symbol = chain_native_tokens.get(chain, '')

            # If Arkham provides unit value and we know the price, calculate amount
            unit_value = transfer.get('unitValue')
            if unit_value and actual_fee_usd > 0:
                try:
                    amount_in = float(unit_value)
                    # swap_volume_usd would be the total swap amount (not fee)
                    # We don't have this from Arkham directly
                except:
                    pass

            # Insert into database (or update if exists)
            db = self._get_connection()
            cursor = db.cursor()
            cursor.execute("""
                INSERT INTO dex_aggregator_revenue (
                    tx_hash, chain, protocol, timestamp,
                    actual_fee_usd, fee_token_symbol, fee_token_address,
                    fee_amount_raw,
                    swap_volume_usd, token_in_symbol, token_in_address,
                    token_out_symbol, token_out_address,
                    amount_in, amount_out,
                    block_number, from_address, to_address,
                    fee_data_source, volume_data_source
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s
               )
                ON CONFLICT (tx_hash) DO UPDATE SET
                    actual_fee_usd = EXCLUDED.actual_fee_usd,
                    protocol = EXCLUDED.protocol,
                    fee_token_symbol = EXCLUDED.fee_token_symbol,
                    token_in_symbol = EXCLUDED.token_in_symbol,
                    token_in_address = EXCLUDED.token_in_address,
                    amount_in = EXCLUDED.amount_in,
                    fee_data_source = EXCLUDED.fee_data_source,
                    volume_data_source = EXCLUDED.volume_data_source,
                    updated_at = NOW()
            """, (
                tx_hash, chain, protocol, timestamp,
                actual_fee_usd, fee_token_symbol, fee_token_address,
                fee_amount_raw,
                swap_volume_usd, token_in_symbol, token_in_address,
                token_out_symbol, token_out_address,
                amount_in, amount_out,
                block_number,
                from_address,
                to_address,
                'arkham',
                volume_data_source
            ))

            logger.debug(f"Inserted/Updated {tx_hash} - {protocol} on {chain}: ${actual_fee_usd:.2f}")

        except Exception as e:
            logger.error(f"Error inserting transfer {transfer.get('transactionHash')}: {e}")
            logger.error(f"Transfer data: {transfer}")
            if self.db:
                self.db.rollback()
    
    def ingest(self):
        """Main ingestion process."""
        try:
            logger.info("Starting Arkham ingestion")

            # Ensure connection is established
            db = self._get_connection()

            # Fetch all transfers
            transfers = self.fetch_all_transfers()

            if not transfers:
                logger.warning("No transfers fetched")
                return

            # Insert each transfer
            for i, transfer in enumerate(transfers, 1):
                self.insert_transfer(transfer)

                # Commit every 100 records and refresh connection
                if i % 100 == 0:
                    db.commit()
                    logger.info(f"Committed {i}/{len(transfers)} transfers")
                    # Refresh connection to prevent staleness
                    db = self._get_connection()

            # Final commit
            db.commit()
            logger.info(f"Completed ingestion of {len(transfers)} transfers")

            # Print stats
            if self.protocol_identifier:
                stats = self.protocol_identifier.get_protocol_stats()
                logger.info("Protocol breakdown:")
                for protocol, count in stats.items():
                    logger.info(f"  {protocol}: {count} transactions")

        except Exception as e:
            logger.error(f"Error during ingestion: {e}")
            if self.db:
                self.db.rollback()
        finally:
            if self.db:
                self.db.close()

def main():
    """Entry point for Arkham ingestor."""
    ingestor = ArkhamIngestor()
    ingestor.ingest()

if __name__ == '__main__':
    main()
