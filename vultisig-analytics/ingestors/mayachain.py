# ingestors/mayachain.py
from typing import Dict, List, Optional
from datetime import datetime, timezone
from .base import BaseIngestor
from config import config
import logging
import json

logger = logging.getLogger(__name__)

class MayaChainIngestor(BaseIngestor):
    def __init__(self):
        super().__init__('mayachain')
        # Prioritized list of API endpoints
        self.api_endpoints = [
            config.MAYACHAIN_API_URL,  # Primary: https://midgard.mayachain.info/v2/actions
            "https://midgard-proxy.odindex.io/v2/actions"  # Fallback: Odindex Proxy
        ]
        self.current_endpoint_index = 0

    def fetch_data(self, next_page_token: str = None, limit: int = 50) -> Dict:
        """Fetch swap data from MayaChain API with fallback support"""
        params = {
            'type': 'swap',
            'affiliate': ','.join(config.VULTISIG_AFFILIATES),
            'limit': limit
        }

        if next_page_token:
            params['nextPageToken'] = next_page_token

        # Try endpoints in order, starting from the last successful one
        # We'll try all endpoints once, wrapping around if needed
        start_index = self.current_endpoint_index
        attempts = 0
        total_endpoints = len(self.api_endpoints)
        
        last_exception = None

        while attempts < total_endpoints:
            endpoint = self.api_endpoints[self.current_endpoint_index]
            
            # Construct full URL if the endpoint is just the base URL
            # But here we store full paths in the list for simplicity, 
            # except config.MAYACHAIN_API_URL might be just base.
            # Let's assume config.MAYACHAIN_API_URL is full path or handle it.
            # config.MAYACHAIN_API_URL is likely "https://midgard.mayachain.info/v2/actions" based on usage
            
            try:
                logger.info(f"Attempting MayaChain endpoint: {endpoint}")
                return self.make_request(endpoint, params)
            except Exception as e:
                logger.warning(f"Endpoint {endpoint} failed: {e}")
                last_exception = e
                # Move to next endpoint
                self.current_endpoint_index = (self.current_endpoint_index + 1) % total_endpoints
                attempts += 1
        
        # If all endpoints failed
        logger.error("All MayaChain endpoints failed")
        raise last_exception or Exception("All endpoints failed")

    def parse_swap(self, raw_swap: Dict) -> Dict:
        """Parse MayaChain swap data into normalized format"""

        def safe_float(value, default=0, max_val=1e30):
            """Safely convert to float with overflow protection for NUMERIC(38,18)"""
            try:
                result = float(value or default)
                return min(result, max_val) if result > 0 else max(result, -max_val)
            except (ValueError, TypeError, OverflowError):
                return default

        try:
            # Basic transaction info
            timestamp = self.parse_timestamp(raw_swap.get('date', ''))
            tx_hash = raw_swap['in'][0]['txID']
            block_height = int(raw_swap.get('height', 0)) if raw_swap.get('height') else None

            # Input data (NEW: store complete input details)
            in_data = raw_swap['in'][0]
            in_coin = in_data['coins'][0]
            user_address = in_data.get('address', '')
            in_address = user_address  # NEW: explicit in_address
            in_tx_id = in_data.get('txID', '')  # NEW: input transaction ID
            in_asset = in_coin.get('asset', '')
            in_amount = safe_float(in_coin.get('amount', 0))
            in_amount_raw = in_coin.get('amount', '0')  # NEW: store raw amount string

            # Swap metadata (NEW: store complete metadata)
            swap_meta = raw_swap.get('metadata', {}).get('swap', {})
            metadata_complete = json.dumps(swap_meta)  # NEW: store complete metadata as JSON

            # Parse affiliate data - handle dual affiliates like "VALT/vi"
            affiliate_address = swap_meta.get('affiliateAddress', '')
            memo = swap_meta.get('memo', '')

            # NEW: Parse multiple affiliate addresses and fees
            affiliate_addresses_str = affiliate_address
            affiliate_fee_str = swap_meta.get('affiliateFee', '')

            # Split by "/" for multiple affiliates
            affiliate_addresses = affiliate_addresses_str.split('/') if affiliate_addresses_str else []
            affiliate_fees_bps = [int(f) for f in affiliate_fee_str.split('/') if f.isdigit()] if affiliate_fee_str else []
            
            # Extract Vultisig affiliate info (vi/va/v0)
            vultisig_affiliate_info = self._extract_vultisig_affiliate(affiliate_address, memo)
            
            # Skip if no Vultisig affiliate found
            if not vultisig_affiliate_info:
                logger.debug(f"Skipping transaction {tx_hash}: No Vultisig affiliate found")
                return None
            
            vultisig_code = vultisig_affiliate_info['code']  # vi, va, or v0
            vultisig_bps = vultisig_affiliate_info['bps']    # BPS for our affiliate
            vultisig_address = vultisig_affiliate_info['address']  # Full address for our affiliate

            # NEW: Parse ALL output data (multiple outputs)
            out_data_list = raw_swap.get('out', [])
            out_addresses = []
            out_tx_ids = []
            out_heights = []

            for out_item in out_data_list:
                out_addresses.append({
                    'address': out_item.get('address'),
                    'coins': out_item.get('coins'),
                    'affiliate': out_item.get('affiliate', False)
                })
                out_tx_ids.append(out_item.get('txID', ''))
                # Convert height to int or None
                height = out_item.get('height')
                out_heights.append(int(height) if height is not None else None)

            # Fee data - extract from the Vultisig affiliate output
            fee_data = self._find_vultisig_affiliate_output(raw_swap.get('out', []), vultisig_address)
            if fee_data and fee_data.get('coins'):
                fee_coin = fee_data['coins'][0]
                fee_asset = fee_coin.get('asset', '')
                fee_amount = safe_float(fee_coin.get('amount', 0))
            else:
                # No fee collected for our affiliate
                logger.debug(f"Skipping transaction {tx_hash}: No Vultisig affiliate fee output found")
                return None

            # Output data - for reference (actual swap output)
            out_data = self._find_swap_output(raw_swap.get('out', []), in_asset)
            if out_data and out_data.get('coins'):
                out_coin = out_data['coins'][0]
                out_asset = out_coin.get('asset', '')
                out_amount = safe_float(out_coin.get('amount', 0))
            else:
                out_asset = ''
                out_amount = 0

            # NEW: Store price data separately
            in_price_usd = safe_float(swap_meta.get('inPriceUSD', 0))
            out_price_usd = safe_float(swap_meta.get('outPriceUSD', 0))

            # NEW: Parse network fees
            network_fees_raw = json.dumps(swap_meta.get('networkFees', []))

            # IMPORTANT: MayaChain uses 1e10 for CACAO, 1e8 for other assets
            # inPriceUSD and outPriceUSD are UNIT PRICES (price per token)
            # Calculate USD volume: (amount / decimals) * unit_price

            # Determine decimals for input asset (CACAO uses 1e10)
            if 'CACAO' in in_asset or 'MAYA' in in_asset:
                in_decimals = 1e10
            else:
                in_decimals = 1e8

            # Determine decimals for output asset
            if 'CACAO' in out_asset or 'MAYA' in out_asset:
                out_decimals = 1e10
            else:
                out_decimals = 1e8

            # Calculate volumes
            in_amount_usd = (in_amount / in_decimals) * in_price_usd
            out_amount_usd = (out_amount / out_decimals) * out_price_usd

            # Fees are in CACAO (base units 1e10)
            liquidity_fee = safe_float(swap_meta.get('liquidityFee', 0)) / 1e10
            affiliate_fee = safe_float(swap_meta.get('affiliateFee', 0)) / 1e10
            network_fees = swap_meta.get('networkFees', [])
            network_fee = sum(safe_float(fee.get('amount', 0)) for fee in network_fees) / 1e10

            # Trust Lower Value Logic (Price Sanity Check)
            # If slip is low, Input USD should be close to Output USD.
            # If discrepancy is huge, assume the LOWER value is correct.
            swap_slip_bps = safe_float(swap_meta.get('swapSlip', 0))
            if in_amount_usd > 0 and out_amount_usd > 0 and swap_slip_bps < 500: # < 5% slip
                diff_pct = abs(in_amount_usd - out_amount_usd) / max(in_amount_usd, out_amount_usd)
                if diff_pct > 0.1: # > 10% discrepancy
                    # Trust the LOWER value
                    if in_amount_usd < out_amount_usd:
                        out_amount_usd = in_amount_usd
                    else:
                        in_amount_usd = out_amount_usd

            # Fees
            # Calculate total economic fee (Input Value - Output Value)
            # This captures Liquidity Fee + Network Fee + Affiliate Fee + Slippage

            # Affiliate Fee is usually in basis points (bps) in the metadata
            # Use the Vultisig-specific BPS (handles dual affiliates correctly)
            affiliate_fee_bps = vultisig_bps

            # CRITICAL: MayaChain Midgard returns unreliable affiliate fee amounts (inflated 493x-20,896x)
            # ALWAYS calculate fees from BPS instead: affiliate_fee_usd = (bps / 10000) * volume
            # This is based on analysis in fix_mayachain_fees.py
            affiliate_fee_usd = (affiliate_fee_bps / 10000) * in_amount_usd

            logger.info(f"DEBUG: tx={tx_hash[:16]}..., vultisig_bps={vultisig_bps}, fee_asset={fee_asset}, fee_amount={fee_amount}, affiliate_fee_usd=${affiliate_fee_usd:.2f}")

            # We can estimate liquidity/network fees as the remainder
            liquidity_fee_usd = 0
            network_fee_usd = 0

            # Calculate total economic fee
            total_fee_usd = max(0, in_amount_usd - out_amount_usd)

            # Pool info (NEW: store all pools as array)
            pools = raw_swap.get('pools', [])
            pools_used = pools  # NEW: store complete pools array
            pool_1 = pools[0] if len(pools) > 0 else None
            pool_2 = pools[1] if len(pools) > 1 else None

            # Additional metadata
            is_streaming_swap = swap_meta.get('isStreamingSwap', False)
            swap_slip = safe_float(swap_meta.get('swapSlip', 0)) if swap_meta.get('swapSlip') else None

            # NEW: Extract swap status and type
            swap_status = raw_swap.get('status', 'success')
            swap_type = raw_swap.get('type', 'swap')

            # Classify volume
            volume_tier = self.classify_volume_tier(in_amount_usd)

            # Store ALL affiliate addresses (referrer + Vultisig) for referral tracking
            # This enables tracking of nested/dual affiliates like "VALT/vi" with BPS "10/35"
            affiliate_addresses_to_store = affiliate_addresses if affiliate_addresses else [vultisig_code]
            affiliate_fees_bps_to_store = affiliate_fees_bps if affiliate_fees_bps else [vultisig_bps]

            return {
                # Existing fields
                'timestamp': timestamp,
                'date_only': timestamp.date(),
                'source': self.source_name,
                'tx_hash': tx_hash,
                'block_height': block_height,
                'user_address': user_address,
                'in_asset': in_asset,
                'in_amount': in_amount,
                'in_amount_usd': in_amount_usd,
                'out_asset': fee_asset,  # Use fee_asset for price calculation
                'out_amount': fee_amount,  # Use fee_amount for price calculation
                'out_amount_usd': out_amount_usd,
                'total_fee_usd': total_fee_usd,
                'network_fee_usd': network_fee_usd,
                'liquidity_fee_usd': liquidity_fee_usd,
                'affiliate_fee_usd': affiliate_fee_usd,
                'pool_1': pool_1,
                'pool_2': pool_2,
                'is_streaming_swap': is_streaming_swap,
                'swap_slip': swap_slip,
                'volume_tier': volume_tier,
                'platform': self.get_platform_from_affiliate(vultisig_code),
                'raw_data': json.dumps(raw_swap),

                # NEW FIELDS for complete Midgard logging
                'in_address': in_address,
                'in_tx_id': in_tx_id,
                'in_amount_raw': in_amount_raw,
                'out_addresses': json.dumps(out_addresses),
                'out_tx_ids': out_tx_ids,
                'out_heights': out_heights,
                'affiliate_addresses': affiliate_addresses_to_store,
                'affiliate_fees_bps': affiliate_fees_bps_to_store,
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
            logger.error(f"Error parsing MayaChain swap: {e}")
            logger.error(f"Raw data: {raw_swap}")
            return None

    def _extract_vultisig_affiliate(self, affiliate_address: str, memo: str) -> Optional[Dict]:
        """
        Extract Vultisig affiliate information from affiliate address and memo.
        Handles dual affiliates like "VALT/vi" with memo "10/35".
        Returns: {'code': 'vi', 'bps': 35, 'address': 'thor_address'} or None
        """
        if not affiliate_address:
            return None
        
        # Our affiliate codes
        vultisig_codes = ['vi', 'va', 'v0']
        
        # Split affiliate address by /
        affiliates = affiliate_address.split('/')
        
        # Find which affiliate is ours
        vultisig_index = None
        vultisig_code = None
        for i, aff in enumerate(affiliates):
            aff_lower = aff.lower().strip()
            if aff_lower in vultisig_codes:
                vultisig_index = i
                vultisig_code = aff_lower
                break
        
        if vultisig_index is None:
            return None
        
        # Extract BPS from memo
        # Memo format: "=:e:0xaddress:0/1/0:VALT/vi:10/35"
        # The last part is affiliateCode:bps
        bps_values = []
        if memo:
            parts = memo.split(':')
            if len(parts) >= 6:  # Has affiliate info
                bps_part = parts[-1]  # Last part is BPS like "10/35"
                if '/' in bps_part:
                    bps_values = [int(x) for x in bps_part.split('/') if x.isdigit()]
                elif bps_part.isdigit():
                    bps_values = [int(bps_part)]
        
        # Get the BPS for our affiliate
        if bps_values and vultisig_index < len(bps_values):
            vultisig_bps = bps_values[vultisig_index]
        else:
            # Fallback: if only one BPS value or parsing failed, use it
            vultisig_bps = bps_values[0] if bps_values else 0
        
        return {
            'code': vultisig_code,
            'bps': vultisig_bps,
            'address': affiliates[vultisig_index]  # The exact affiliate code from the address
        }
    
    def _find_vultisig_affiliate_output(self, outputs: List[Dict], vultisig_address: str) -> Optional[Dict]:
        """
        Find the affiliate output for Vultisig.
        In dual affiliate scenarios, there may be multiple affiliate outputs.
        We need to find the one corresponding to our affiliate.
        """
        for output in outputs:
            if output.get('affiliate'):
                return output
        return None
    
    def _find_swap_output(self, outputs: List[Dict], in_asset: str) -> Optional[Dict]:
        """Find the actual swap output (different asset, non-affiliate)"""
        # Find an output with a different asset than input
        for output in outputs:
            if output.get('coins') and len(output['coins']) > 0:
                out_asset = output['coins'][0].get('asset', '')
                if not output.get('affiliate') and out_asset != in_asset:
                    return output

        # Fallback: find first non-affiliate output
        for output in outputs:
            if not output.get('affiliate'):
                return output

        return outputs[0] if outputs else None

