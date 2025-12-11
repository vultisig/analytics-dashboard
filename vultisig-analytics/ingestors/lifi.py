# ingestors/lifi.py
from typing import Dict, List, Optional
from datetime import datetime, timezone
from .base import BaseIngestor
from config import config
import logging
import json

logger = logging.getLogger(__name__)

class LiFiIngestor(BaseIngestor):
    def __init__(self):
        super().__init__('lifi')
        self.api_url = "https://li.quest/v2/analytics/transfers"

        # Add LiFi API key header for higher rate limits (200 RPM vs 20 RPM)
        if config.LIFI_API_KEY:
            self.session.headers.update({
                'x-lifi-api-key': config.LIFI_API_KEY
            })
            logger.info("LiFi API key configured - using authenticated rate limits (200 RPM)")
        else:
            logger.warning("LiFi API key not found - using public rate limits (20 RPM)")

    def fetch_data(self, next_page_token: str = None, limit: int = 50) -> Dict:
        """Fetch transfer data from LiFi API for all Vultisig integrators"""
        # LiFi supports comma-separated integrators - more efficient
        params = {
            'integrator': 'vultisig-ios,vultisig-android,vultisig-web,vultisig-windows,vultisig-mac',
            'limit': limit
        }

        if next_page_token:
            params['next'] = next_page_token

        return self.make_request(self.api_url, params)

    def parse_swap(self, raw_transfer: Dict) -> Dict:
        """Parse LiFi transfer data into normalized format"""

        def safe_float(value, default=0, max_val=99999999999.99999999):
            """Safely convert to float with overflow protection for NUMERIC(20,8)"""
            try:
                result = float(value or default)
                return min(result, max_val) if result > 0 else max(result, -max_val)
            except (ValueError, TypeError, OverflowError):
                return default

        def safe_int(value, default=0):
            """Safely convert to int"""
            try:
                return int(value or default)
            except (ValueError, TypeError):
                return default

        try:
            # Basic transaction info - use sending timestamp as primary
            sending = raw_transfer.get('sending', {})
            receiving = raw_transfer.get('receiving', {})

            # Convert Unix timestamp to datetime
            timestamp_unix = sending.get('timestamp', 0)
            if timestamp_unix:
                timestamp = datetime.fromtimestamp(timestamp_unix, timezone.utc)
            else:
                timestamp = datetime.now(timezone.utc)

            # Transaction hashes (NEW: store complete transaction IDs)
            tx_hash = sending.get('txHash', '')
            receiving_tx_hash = receiving.get('txHash', '')

            # Use transactionId as primary identifier
            transaction_id = raw_transfer.get('transactionId', tx_hash)

            # User addresses (NEW: store input/output addresses explicitly)
            from_address = raw_transfer.get('fromAddress', '')
            to_address = raw_transfer.get('toAddress', '')
            in_address = from_address  # NEW: explicit in_address
            in_tx_id = tx_hash  # NEW: input transaction ID

            # Sending token data (NEW: store raw amounts)
            sending_token = sending.get('token', {})
            in_asset = f"{sending_token.get('symbol', '')}-{sending_token.get('chainId', '')}"

            # Convert token amounts (considering decimals)
            sending_amount_raw = safe_float(sending.get('amount', 0))
            in_amount_raw = str(int(sending_amount_raw)) if sending_amount_raw else '0'  # NEW: store raw amount string
            sending_decimals = safe_int(sending_token.get('decimals', 18))
            in_amount = sending_amount_raw / (10 ** sending_decimals) if sending_decimals > 0 else sending_amount_raw

            in_amount_usd = safe_float(sending.get('amountUSD', 0))
            in_price_usd = safe_float(sending_token.get('priceUSD', 0))  # NEW: store price

            # Receiving token data (NEW: store output details)
            receiving_token = receiving.get('token', {})
            out_asset = f"{receiving_token.get('symbol', '')}-{receiving_token.get('chainId', '')}"

            receiving_amount_raw = safe_float(receiving.get('amount', 0))
            receiving_decimals = safe_int(receiving_token.get('decimals', 18))
            out_amount = receiving_amount_raw / (10 ** receiving_decimals) if receiving_decimals > 0 else receiving_amount_raw

            out_amount_usd = safe_float(receiving.get('amountUSD', 0))
            out_price_usd = safe_float(receiving_token.get('priceUSD', 0))  # NEW: store output price

            # NEW: Store output transaction details
            out_addresses = [{
                'address': to_address,
                'coins': [{'asset': out_asset, 'amount': str(int(receiving_amount_raw))}],
                'affiliate': False
            }]
            out_tx_ids = [receiving_tx_hash] if receiving_tx_hash else []
            out_heights = [None]  # LiFi doesn't provide block heights

            # Fee calculation
            # Gas fees
            sending_gas_usd = safe_float(sending.get('gasAmountUSD', 0))
            receiving_gas_usd = safe_float(receiving.get('gasAmountUSD', 0))
            network_fee_usd = sending_gas_usd + receiving_gas_usd

            # Integrator fees from included steps
            affiliate_fee_usd = 0
            included_steps = sending.get('includedSteps', [])
            for step in included_steps:
                if step.get('tool') == 'feeCollection':
                    # Calculate fee as difference between fromAmount and toAmount
                    from_amt = safe_float(step.get('fromAmount', 0))
                    to_amt = safe_float(step.get('toAmount', 0))
                    fee_amount_raw = from_amt - to_amt

                    # Convert to USD using token price
                    token_price = safe_float(sending_token.get('priceUSD', 0))
                    if token_price > 0 and sending_decimals > 0:
                        fee_amount_normalized = fee_amount_raw / (10 ** sending_decimals)
                        affiliate_fee_usd += fee_amount_normalized * token_price

            # Bridge/liquidity fees (difference between input and output USD minus gas)
            liquidity_fee_usd = max(0, in_amount_usd - out_amount_usd - affiliate_fee_usd)

            total_fee_usd = network_fee_usd + affiliate_fee_usd + liquidity_fee_usd

            # Additional metadata
            tool = raw_transfer.get('tool', '')
            status = raw_transfer.get('status', '')
            substatus = raw_transfer.get('substatus', '')

            # Chain info
            from_chain = sending_token.get('chainId', '')
            to_chain = receiving_token.get('chainId', '')

            # Block height not available in LiFi API
            block_height = None

            # Volume tier classification
            volume_tier = self.classify_volume_tier(in_amount_usd)

            # For LiFi, we'll store additional bridge-specific data
            bridge_metadata = {
                'tool': tool,
                'status': status,
                'substatus': substatus,
                'from_chain_id': from_chain,
                'to_chain_id': to_chain,
                'receiving_tx_hash': receiving_tx_hash,
                'lifi_explorer_link': raw_transfer.get('lifiExplorerLink', ''),
                'bridge_explorer_link': raw_transfer.get('bridgeExplorerLink', ''),
                'included_steps': len(included_steps)
            }

            # NEW: Store complete metadata and network fees
            metadata_complete = json.dumps(bridge_metadata)
            network_fees_raw = json.dumps([{
                'asset': in_asset,
                'amount': str(int(sending_gas_usd * 1e8))  # Normalize to E8 format
            }])

            # NEW: Extract pools/chains used
            pools_used = [f"{from_chain}-{to_chain}"]

            # NEW: Extract swap status and type
            swap_status = status if status else 'unknown'
            swap_type = 'bridge'  # LiFi transactions are bridge/cross-chain swaps

            # Determine platform from integrator string
            metadata = raw_transfer.get('metadata', {})
            integrator = metadata.get('integrator', '')
            platform = self.get_platform_from_integrator(integrator)

            # NEW: Affiliate addresses - LiFi doesn't use same affiliate model
            affiliate_addresses = []  # Empty for LiFi
            affiliate_fees_bps = []

            # Memo equivalent (integrator info)
            memo = integrator if integrator else ''

            return {
                # Existing fields
                'timestamp': timestamp,
                'date_only': timestamp.date(),
                'source': self.source_name,
                'tx_hash': transaction_id,  # Use transactionId as primary
                'block_height': block_height,
                'user_address': from_address,
                'in_asset': in_asset,
                'in_amount': in_amount,
                'in_amount_usd': in_amount_usd,
                'out_asset': out_asset,
                'out_amount': out_amount,
                'out_amount_usd': out_amount_usd,
                'total_fee_usd': total_fee_usd,
                'network_fee_usd': network_fee_usd,
                'liquidity_fee_usd': liquidity_fee_usd,
                'affiliate_fee_usd': affiliate_fee_usd,
                'pool_1': f"{from_chain}-{to_chain}",  # Bridge route as pool
                'pool_2': None,
                'is_streaming_swap': False,  # Not applicable for bridges
                'swap_slip': None,  # Calculate from price difference if needed
                'volume_tier': volume_tier,
                'platform': platform,
                'raw_data': json.dumps({
                    **raw_transfer,
                    'bridge_metadata': bridge_metadata
                }),

                # NEW FIELDS for complete logging
                'in_address': in_address,
                'in_tx_id': in_tx_id,
                'in_amount_raw': in_amount_raw,
                'out_addresses': json.dumps(out_addresses),
                'out_tx_ids': out_tx_ids,
                'out_heights': out_heights,
                'affiliate_addresses': affiliate_addresses,
                'affiliate_fees_bps': affiliate_fees_bps,
                'metadata_complete': metadata_complete,
                'in_price_usd': in_price_usd,
                'out_price_usd': out_price_usd,
                'network_fees_raw': network_fees_raw,
                'pools_used': pools_used,
                'swap_status': swap_status,
                'swap_type': swap_type,
                'memo': memo
            }

        except Exception as e:
            logger.error(f"Error parsing LiFi transfer: {e}")
            logger.error(f"Raw data: {raw_transfer}")
            return None

    def get_platform_from_integrator(self, integrator: str) -> str:
        """Determine platform from integrator string"""
        if not integrator:
            return 'Unknown'
        
        integrator = integrator.lower()
        if 'ios' in integrator:
            return 'iOS'
        elif 'android' in integrator:
            return 'Android'
        elif 'web' in integrator:
            return 'Web'
        elif 'mac' in integrator:
            return 'Mac'
        elif 'windows' in integrator:
            return 'Windows'
        elif 'vultisig' in integrator:
            # Fallback if specific platform not found but is vultisig
            return 'Unknown' 
        else:
            return 'Other'