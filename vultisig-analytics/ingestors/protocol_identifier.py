"""
Protocol identifier for DEX aggregator transactions.
Maps transaction hashes and addresses to known DEX aggregators.
"""

import logging
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

# Known DEX aggregator router addresses (lowercase)
KNOWN_ROUTERS = {
    '1inch': [
        '0x1111111254eeb25477b68fb85ed929f73a960582',  # 1inch v5 Router
        '0x111111125421ca6dc452d289314280a0f8842a65',  # 1inch v6 Router (Fusion)
        '0x11111112542d85b3ef69ae05771c2dccff4faa26',  # 1inch v4 Router
    ],
    'paraswap': [
        '0xdef171fe48cf0115b1d80b88dc8eab59176fee57',  # Paraswap Augustus v5
        '0x216b4b4ba9f3e719726886d34a177484278bfcae',  # Paraswap Augustus v6
    ],
    'cowswap': [
        '0x9008d19f58aabd9ed0d60971565aa8510560ab41',  # CoWSwap Settlement
    ],
    'matcha': [
        '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  # 0x Exchange Proxy (Matcha)
    ],
    '0x': [
        '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  # 0x Exchange Proxy
    ],
}

class ProtocolIdentifier:
    """Identifies which DEX aggregator protocol a transaction belongs to."""
    
    def __init__(self, db_connection=None):
        self.db = db_connection
        # Flatten and normalize router addresses for quick lookup
        self.router_to_protocol = {}
        for protocol, addresses in KNOWN_ROUTERS.items():
            for addr in addresses:
                self.router_to_protocol[addr.lower()] = protocol
    
    def identify_by_address(self, from_address: str) -> Optional[str]:
        """
        Identify protocol by the fromAddress (router contract).
        
        Args:
            from_address: The address that sent the fee to integrator
            
        Returns:
            Protocol name or None if not recognized
        """
        if not from_address:
            return None
        
        normalized_addr = from_address.lower()
        return self.router_to_protocol.get(normalized_addr)
    
    def identify_by_1inch_api(self, tx_hash: str) -> Optional[str]:
        """
        Check if transaction exists in our 1inch database.
        
        Args:
            tx_hash: Transaction hash as string
            
        Returns:
            '1inch' if found in database, None otherwise
        """
        if not self.db:
            return None
        
        # Handle case where tx_hash might be passed as dict
        if isinstance(tx_hash, dict):
            tx_hash = tx_hash.get('transactionHash', '')
        
        if not tx_hash or not isinstance(tx_hash, str):
            return None
        
        try:
            cursor = self.db.cursor()
            cursor.execute(
                "SELECT 1 FROM swaps WHERE source = '1inch' AND LOWER(tx_hash) = LOWER(%s) LIMIT 1",
                (tx_hash,)
            )
            result = cursor.fetchone()
            return '1inch' if result else None
        except Exception as e:
            logger.error(f"Error checking 1inch database for {tx_hash}: {e}")
            return None
    
    def identify_protocol(
        self, 
        tx_hash: str, 
        from_address: str, 
        chain: str
    ) -> str:
        """
        Identify which DEX aggregator protocol a transaction belongs to.
        
        Uses multiple identification methods in priority order:
        1. Known router addresses
        2. Cross-reference with 1inch database
        3. Default to 'other'
        
        Args:
            tx_hash: Transaction hash
            from_address: The address that sent tokens to integrator
            chain: Blockchain name
            
        Returns:
            Protocol name: '1inch', 'paraswap', 'cowswap', 'matcha', '0x', or 'other'
        """
        # Method 1: Check known router addresses (fastest)
        protocol = self.identify_by_address(from_address)
        if protocol:
            logger.info(f"Identified {tx_hash} as {protocol} by router address")
            return protocol
        
        # Method 2: Check 1inch database
        protocol = self.identify_by_1inch_api(tx_hash)
        if protocol:
            logger.info(f"Identified {tx_hash} as {protocol} by database lookup")
            return protocol
        
        # Default: Unknown protocol
        logger.debug(f"Could not identify protocol for {tx_hash}, marking as 'other'")
        return 'other'
    
    def get_protocol_stats(self) -> Dict[str, int]:
        """Get count of transactions per protocol from database."""
        if not self.db:
            return {}
        
        try:
            cursor = self.db.cursor()
            cursor.execute("""
                SELECT protocol, COUNT(*) as count
                FROM dex_aggregator_revenue
                GROUP BY protocol
                ORDER BY count DESC
            """)
            return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Error getting protocol stats: {e}")
            return {}
