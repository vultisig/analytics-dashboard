# ingestors/thorchain.py
from typing import Dict, List, Optional
from datetime import datetime, timezone
from .base import BaseIngestor
from config import config
import logging
import json

logger = logging.getLogger(__name__)

class THORChainIngestor(BaseIngestor):
    def __init__(self):
        super().__init__('thorchain')
        # List of endpoints to try in order (primary to fallback)
        self.api_endpoints = [
            'https://midgard.ninerealms.com/v2/actions',
            'https://vanaheimex.com/actions',  # Vanaheimex as fallback
            'https://midgard.thorswap.net/v2/actions',
            'https://midgard.thorchain.liquify.com/v2/actions',
        ]
        self.current_endpoint_index = 0
    
    def fetch_data(self, next_page_token: str = None, limit: int = 50) -> Dict:
        """Fetch swap data from THORChain API with endpoint fallback"""
        params = {
            'type': 'swap',
            'affiliate': ','.join(config.VULTISIG_AFFILIATES),
            'limit': limit
        }
        
        if next_page_token:
            params['nextPageToken'] = next_page_token
        
        # Try each endpoint in sequence
        last_error = None
        for i, endpoint in enumerate(self.api_endpoints):
            try:
                logger.info(f"Attempting endpoint {i+1}/{len(self.api_endpoints)}: {endpoint}")
                result = self.make_request(endpoint, params)
                # If successful, remember this endpoint for next time
                if self.current_endpoint_index != i:
                    logger.info(f"Switched to working endpoint: {endpoint}")
                    self.current_endpoint_index = i
                return result
            except Exception as e:
                last_error = e
                logger.warning(f"Endpoint {endpoint} failed: {e}")
                # Continue to next endpoint
                continue
        
        # All endpoints failed
        raise Exception(f"All THORChain endpoints failed. Last error: {last_error}")
    
    def parse_swap(self, raw_swap: Dict) -> Dict:
        """Parse THORChain swap data into normalized format"""
        try:
            # Basic transaction info
            timestamp = self.parse_timestamp(raw_swap.get('date', ''))
            tx_hash = raw_swap['in'][0]['txID']
            block_height = raw_swap.get('height')

            # Input data (NEW: store complete input details)
            in_data = raw_swap['in'][0]
            in_coin = in_data['coins'][0]
            user_address = in_data.get('address', '')
            in_address = user_address  # NEW: explicit in_address
            in_tx_id = in_data.get('txID', '')  # NEW: input transaction ID
            in_asset = in_coin.get('asset', '')
            in_amount = float(in_coin.get('amount', 0))
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
                fee_amount = float(fee_coin.get('amount', 0))
            else:
                # No fee collected for our affiliate
                logger.debug(f"Skipping transaction {tx_hash}: No Vultisig affiliate fee output found")
                return None

            # Output data - for reference (actual swap output)
            out_data = self._find_swap_output(raw_swap.get('out', []), in_asset)
            if out_data and out_data.get('coins'):
                out_coin = out_data['coins'][0]
                out_asset = out_coin.get('asset', '')
                out_amount = float(out_coin.get('amount', 0))
            else:
                out_asset = ''
                out_amount = 0
            # NEW: Store price data separately
            in_price_usd = float(swap_meta.get('inPriceUSD', 0))
            out_price_usd = float(swap_meta.get('outPriceUSD', 0))

            # NEW: Parse network fees
            network_fees_raw = json.dumps(swap_meta.get('networkFees', []))
            
            # Calculate USD volume: (amount / 1e8) * price
            logger.info(f"DEBUG: in_amount={in_amount}, in_price_usd={in_price_usd}")
            in_amount_usd = (in_amount / 1e8) * in_price_usd
            out_amount_usd = (out_amount / 1e8) * out_price_usd
            # Note: THORChain normalizes all amounts to 1e8 (E8) internally.
            # We strictly divide by 1e8 to get human readable amounts, regardless of the asset's native decimals.
            # This applies to both input and output amounts from Midgard /v2/actions. Logic (Price Sanity Check)
            # If slip is low, Input USD should be close to Output USD.
            # If discrepancy is huge, assume the LOWER value is correct.
            swap_slip_bps = float(swap_meta.get('swapSlip', 0)) if swap_meta.get('swapSlip') else 0
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
            total_fee_usd = max(0, in_amount_usd - out_amount_usd)

            # Use the Vultisig-specific BPS (handles dual affiliates correctly)
            affiliate_fee_bps = vultisig_bps

            # NEW: Calculate affiliate fee USD from actual fee amount collected, not percentage
            # The fee is collected in fee_asset (usually THOR.RUNE)
            if fee_asset == 'THOR.RUNE':
                # Get RUNE price - try Midgard first, fallback to swap data
                try:
                    rune_price_usd = self._get_rune_price_from_midgard(timestamp)
                    logger.debug(f"Got RUNE price from Midgard: ${rune_price_usd:.4f}")
                except Exception as e:
                    logger.warning(f"Failed to get RUNE price from Midgard for {tx_hash}: {e}, using fallback")
                    rune_price_usd = self._derive_rune_price_from_pools(raw_swap)
                    if rune_price_usd > 0:
                        logger.debug(f"Derived RUNE price from swap: ${rune_price_usd:.4f}")

                # Calculate USD value of fee collected in RUNE
                affiliate_fee_usd = (fee_amount / 1e8) * rune_price_usd
            elif fee_asset == in_asset:
                # Fee is in input asset
                affiliate_fee_usd = (fee_amount / 1e8) * in_price_usd
            elif fee_asset == out_asset:
                # Fee is in output asset
                affiliate_fee_usd = (fee_amount / 1e8) * out_price_usd
            else:
                # Unknown asset, fallback to percentage calculation
                logger.warning(f"Unknown fee asset {fee_asset} for {tx_hash}, using percentage fallback")
                affiliate_fee_usd = (affiliate_fee_bps / 10000) * in_amount_usd

            logger.info(f"DEBUG: tx={tx_hash[:16]}..., vultisig_bps={vultisig_bps}, fee_asset={fee_asset}, fee_amount={fee_amount}, affiliate_fee_usd=${affiliate_fee_usd:.2f}")

            # We can estimate liquidity/network fees as the remainder, but they are less critical for revenue
            # liquidity_fee_usd = total_fee_usd - affiliate_fee_usd - network_fee_usd (approx)
            liquidity_fee_usd = 0 # Placeholder or derived if needed
            network_fee_usd = 0   # Placeholder or derived if needed
            
            # Pool info (NEW: store all pools as array)
            pools = raw_swap.get('pools', [])
            pools_used = pools  # NEW: store complete pools array
            pool_1 = pools[0] if len(pools) > 0 else None
            pool_2 = pools[1] if len(pools) > 1 else None

            # Additional metadata
            is_streaming_swap = swap_meta.get('isStreamingSwap', False)
            swap_slip = float(swap_meta.get('swapSlip', 0)) if swap_meta.get('swapSlip') else None

            # NEW: Extract swap status and type
            swap_status = raw_swap.get('status', 'success')
            swap_type = raw_swap.get('type', 'swap')

            # Classify volume
            volume_tier = self.classify_volume_tier(in_amount_usd)

            # NEW: Filter Vultisig-specific affiliate addresses
            vultisig_affiliates = [a for a in affiliate_addresses if a.lower() in ['vi', 'va', 'v0']]

            # CRITICAL FIX: Always store the BPS value that was actually used for calculation
            # Previously, empty vultisig_affiliates list would result in NULL storage
            # even though vultisig_bps was used for fee calculation
            affiliate_addresses_to_store = vultisig_affiliates if vultisig_affiliates else [vultisig_code]
            affiliate_fees_bps_to_store = affiliate_fees_bps[:len(vultisig_affiliates)] if vultisig_affiliates and affiliate_fees_bps else [vultisig_bps]

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
            logger.error(f"Error parsing THORChain swap: {e}")
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
        
        Note: THORChain outputs don't explicitly tag which affiliate output belongs to which affiliate.
        For now, we'll use the first affiliate output. If there are multiple affiliates,
        we may need more sophisticated logic based on output addresses.
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

    def _get_rune_price_from_midgard(self, timestamp: datetime) -> float:
        """
        Query Midgard /v2/history/swaps for RUNE price at the given timestamp.

        API: GET /v2/history/swaps?interval=5min&from={timestamp}&count=1
        Returns: runePriceUSD field from the first interval
        """
        from datetime import timezone

        # Get Unix timestamp
        ts_unix = int(timestamp.replace(tzinfo=timezone.utc).timestamp())

        # Use first endpoint, replace /v2/actions with /v2/history/swaps
        base_url = self.api_endpoints[self.current_endpoint_index]
        history_url = base_url.replace('/v2/actions', '/v2/history/swaps')

        # Use 5min interval, from timestamp, count=1 (only these 2 parameters allowed)
        params = {
            'interval': '5min',
            'from': ts_unix,
            'count': 1
        }

        try:
            response = self.make_request(history_url, params)
            intervals = response.get('intervals', [])
            if intervals and len(intervals) > 0:
                rune_price = float(intervals[0].get('runePriceUSD', 0))
                if rune_price > 0:
                    return rune_price
        except Exception as e:
            logger.warning(f"Midgard history API error: {e}")

        raise Exception("No RUNE price data available from Midgard")

    def _derive_rune_price_from_pools(self, raw_swap: Dict) -> float:
        """
        FALLBACK: Calculate RUNE price from pool data in the swap.

        For swaps involving RUNE on one side:
        - If X → RUNE: RUNE price = outPriceUSD
        - If RUNE → X: RUNE price = inPriceUSD
        """
        try:
            # Extract in/out data
            in_data = raw_swap['in'][0]
            in_coin = in_data['coins'][0]
            in_asset = in_coin.get('asset', '')

            metadata = raw_swap.get('metadata', {}).get('swap', {})
            in_price_usd = float(metadata.get('inPriceUSD', 0))
            out_price_usd = float(metadata.get('outPriceUSD', 0))

            # Find RUNE side
            out_data = self._find_swap_output(raw_swap.get('out', []), in_asset)
            if out_data and out_data.get('coins'):
                out_coin = out_data['coins'][0]
                out_asset = out_coin.get('asset', '')

                if in_asset == 'THOR.RUNE' and in_price_usd > 0:
                    return in_price_usd
                elif out_asset == 'THOR.RUNE' and out_price_usd > 0:
                    return out_price_usd
        except Exception as e:
            logger.warning(f"Failed to derive RUNE price from swap: {e}")

        # Last resort: return 0 (will skip this swap)
        return 0