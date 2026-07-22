# tests/test_lifi.py
"""
Unit tests for ingestors/lifi.py (LiFiIngestor).

All fixtures are built on a real LiFi /v2/analytics/transfers response
envelope (2 transfers from a single page). Variant helpers mutate copies
of one canonical transfer to exercise each code path.

Run from the repo root with:
    python3 -m unittest discover -s tests -p 'test_*.py' -v
"""
import copy
import json
import os
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

# Make `ingestors` importable whether the test runs from the repo root
# (CI) or from vultisig-analytics/.
_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from ingestors.lifi import LiFiIngestor  # noqa: E402


# ---------------------------------------------------------------------------
# Real-world fixture: full /v2/analytics/transfers response envelope
# ---------------------------------------------------------------------------

REAL_RESPONSE = {
    "data": [
        # --- Transfer 0: WXPL → USDT0 on Plasma (same-chain swap) ------------
        {
            "transactionId": "0x3517804281be09de759184011dcfadb93ef8c99a60e795db2afa757ebf7cc96d",
            "sending": {
                "txHash": "0xa5a2cc477396bb98ef331cae72a2883c6667ec7637fb3c1f265fdee4d1abcc01",
                "txLink": "https://plasmascan.to/tx/0xa5a2cc477396bb98ef331cae72a2883c6667ec7637fb3c1f265fdee4d1abcc01",
                "token": {
                    "address": "0x6100E367285b01F48D07953803A2d8dCA5D19873",
                    "chainId": 9745,
                    "symbol": "WXPL",
                    "decimals": 18,
                    "name": "Wrapped Plasma",
                    "coinKey": "WXPL",
                    "priceUSD": "0.130765",
                    "tags": [],
                    "verificationStatus": "unverified",
                    "verificationStatusBreakdown": [],
                },
                "chainId": 9745,
                "gasPrice": "1500000000",
                "gasUsed": "334118",
                "gasToken": {
                    "address": "0x0000000000000000000000000000000000000000",
                    "chainId": 9745,
                    "symbol": "XPL",
                    "decimals": 18,
                    "priceUSD": "0.130555",
                },
                "gasAmount": "501177000000000",
                "gasAmountUSD": "0.0001",
                "amountUSD": "1.1769",
                "value": "0",
                "includedSteps": [
                    {
                        "tool": "kyberswap",
                        "toolDetails": {
                            "key": "kyberswap",
                            "name": "Kyberswap",
                            "webUrl": "https://kyberswap.com/",
                        },
                        "fromAmount": "9000000000000000000",
                        "fromToken": {
                            "address": "0x6100E367285b01F48D07953803A2d8dCA5D19873",
                            "chainId": 9745,
                            "symbol": "WXPL",
                            "decimals": 18,
                            "priceUSD": "0.130765",
                        },
                        "toToken": {
                            "address": "0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb",
                            "chainId": 9745,
                            "symbol": "USDT0",
                            "decimals": 6,
                            "priceUSD": "1",
                        },
                        "toAmount": "1176337",
                        "bridgedAmount": None,
                    }
                ],
                "amount": "9000000000000000000",
                "timestamp": 1775911736,
            },
            "receiving": {
                "txHash": "0xa5a2cc477396bb98ef331cae72a2883c6667ec7637fb3c1f265fdee4d1abcc01",
                "txLink": "https://plasmascan.to/tx/0xa5a2cc477396bb98ef331cae72a2883c6667ec7637fb3c1f265fdee4d1abcc01",
                "token": {
                    "address": "0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb",
                    "chainId": 9745,
                    "symbol": "USDT0",
                    "decimals": 6,
                    "priceUSD": "1",
                },
                "chainId": 9745,
                "gasPrice": "1500000000",
                "gasUsed": "334118",
                "gasAmount": "501177000000000",
                "gasAmountUSD": "0.0001",
                "amountUSD": "1.1763",
                "value": "0",
                "amount": "1176337",
                "timestamp": 1775911736,
            },
            "lifiExplorerLink": "https://scan.li.fi/tx/0xa5a2cc477396bb98ef331cae72a2883c6667ec7637fb3c1f265fdee4d1abcc01",
            "fromAddress": "0x6719558682cbb76f7fa6c216c7b51791b163a6b8",
            "toAddress": "0x6719558682cbb76f7fa6c216c7b51791b163a6b8",
            "tool": "kyberswap",
            "status": "DONE",
            "substatus": "COMPLETED",
            "substatusMessage": "The transfer is complete.",
            "metadata": {"integrator": "jumper.exchange"},
            "feeCosts": [],
        },
        # --- Transfer 1: ETH → USDC on Base (same-chain swap) ----------------
        {
            "transactionId": "0xeee2f6d38a6a8a43904d413e1659b2e17168c317fd32cbc6dd3eb78b62b0a349",
            "sending": {
                "txHash": "0x039fd7029b33aad4f49ca7c05f1d7b78f65fc54ba9eb9be2b6f2b486b632586e",
                "txLink": "https://basescan.org/tx/0x039fd7029b33aad4f49ca7c05f1d7b78f65fc54ba9eb9be2b6f2b486b632586e",
                "token": {
                    "address": "0x0000000000000000000000000000000000000000",
                    "chainId": 8453,
                    "symbol": "ETH",
                    "decimals": 18,
                    "priceUSD": "2244.54",
                },
                "chainId": 8453,
                "gasAmountUSD": "0.0044",
                "amountUSD": "17.9563",
                "value": "8000000000000000",
                "includedSteps": [
                    {
                        "tool": "sushiswap",
                        "fromAmount": "8000000000000000",
                        "fromToken": {
                            "chainId": 8453,
                            "symbol": "ETH",
                            "decimals": 18,
                            "priceUSD": "2244.54",
                        },
                        "toToken": {
                            "chainId": 8453,
                            "symbol": "USDC",
                            "decimals": 6,
                            "priceUSD": "0.999906",
                        },
                        "toAmount": "17952020",
                        "bridgedAmount": None,
                    }
                ],
                "amount": "8000000000000000",
                "timestamp": 1775911731,
            },
            "receiving": {
                "txHash": "0x039fd7029b33aad4f49ca7c05f1d7b78f65fc54ba9eb9be2b6f2b486b632586e",
                "token": {
                    "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                    "chainId": 8453,
                    "symbol": "USDC",
                    "decimals": 6,
                    "priceUSD": "0.999906",
                },
                "chainId": 8453,
                "gasAmountUSD": "0.0044",
                "amountUSD": "17.9503",
                "value": "8000000000000000",
                "amount": "17952020",
                "timestamp": 1775911731,
            },
            "lifiExplorerLink": "https://scan.li.fi/tx/0x039fd7029b33aad4f49ca7c05f1d7b78f65fc54ba9eb9be2b6f2b486b632586e",
            "fromAddress": "0xb75bb26e8dbff32c6674d8f0db2750a71924a23e",
            "toAddress": "0xb75bb26e8dbff32c6674d8f0db2750a71924a23e",
            "tool": "sushiswap",
            "status": "DONE",
            "substatus": "COMPLETED",
            "substatusMessage": "The transfer is complete.",
            "metadata": {"integrator": "os-prod"},
            "feeCosts": [],
        },
    ],
    "hasPrevious": False,
    "hasNext": True,
    "next": "W3siJGRhdGUiOiIyMDI2LTA0LTExVDEyOjQ4OjI5WiJ9XQ",
}

# Constants pulled from the canonical transfer (index 0).
REAL_TX_ID = "0x3517804281be09de759184011dcfadb93ef8c99a60e795db2afa757ebf7cc96d"
REAL_TX_HASH = "0xa5a2cc477396bb98ef331cae72a2883c6667ec7637fb3c1f265fdee4d1abcc01"
REAL_FROM_ADDR = "0x6719558682cbb76f7fa6c216c7b51791b163a6b8"

# safe_float inside parse_swap caps at 99999999999.99999999 ≈ 1e11 — any
# base-unit amount with 18-decimal tokens (9e18, 8e15, ...) gets clipped.
CLIPPED_AMOUNT_RAW = "100000000000"
CLIPPED_IN_AMOUNT = 100000000000.0 / 1e18  # 1e-7


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _clone(transfer_index: int = 0):
    """Return an independent deep-copy of one of the real transfers."""
    return copy.deepcopy(REAL_RESPONSE["data"][transfer_index])


def _clone_response():
    """Return an independent deep-copy of the full response envelope."""
    return copy.deepcopy(REAL_RESPONSE)


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestInit(unittest.TestCase):
    def test_source_name_and_api_url(self):
        ing = LiFiIngestor()
        self.assertEqual(ing.source_name, "lifi")
        self.assertEqual(ing.api_url, "https://li.quest/v2/analytics/transfers")

    def test_api_key_header_set_when_configured(self):
        with patch("ingestors.lifi.config") as mock_cfg:
            mock_cfg.LIFI_API_KEY = "test-key-xyz"
            ing = LiFiIngestor()
            self.assertEqual(ing.session.headers.get("x-lifi-api-key"), "test-key-xyz")

    def test_api_key_header_absent_when_not_configured(self):
        with patch("ingestors.lifi.config") as mock_cfg:
            mock_cfg.LIFI_API_KEY = ""
            ing = LiFiIngestor()
            self.assertNotIn("x-lifi-api-key", ing.session.headers)


# ---------------------------------------------------------------------------
# fetch_data
# ---------------------------------------------------------------------------

class TestFetchData(unittest.TestCase):
    def setUp(self):
        self.ing = LiFiIngestor()

    def test_uses_api_url_and_integrator_list(self):
        with patch.object(self.ing, "make_request", return_value={"data": []}) as m:
            result = self.ing.fetch_data(limit=25)
        self.assertEqual(result, {"data": []})
        m.assert_called_once()
        called_url, called_params = m.call_args[0]
        self.assertEqual(called_url, "https://li.quest/v2/analytics/transfers")
        self.assertEqual(called_params["limit"], 25)
        # Integrator string is the comma-joined list of Vultisig platforms.
        integrators = called_params["integrator"].split(",")
        self.assertIn("vultisig-ios", integrators)
        self.assertIn("vultisig-android", integrators)
        self.assertIn("vultisig-web", integrators)
        self.assertIn("vultisig-windows", integrators)
        self.assertIn("vultisig-mac", integrators)
        # SDK-default tag used by desktop apps + browser extension.
        self.assertIn("vultisig-0", integrators)
        self.assertNotIn("next", called_params)

    def test_next_page_token_is_forwarded(self):
        with patch.object(self.ing, "make_request", return_value={"data": []}) as m:
            self.ing.fetch_data(next_page_token=REAL_RESPONSE["next"])
        _, params = m.call_args[0]
        self.assertEqual(params["next"], "W3siJGRhdGUiOiIyMDI2LTA0LTExVDEyOjQ4OjI5WiJ9XQ")

    def test_full_response_envelope_is_returned_verbatim(self):
        """
        fetch_data is a thin pass-through: whatever LiFi returns must
        be handed back unchanged (including hasNext / hasPrevious / next).
        """
        with patch.object(
            self.ing, "make_request", return_value=_clone_response()
        ):
            result = self.ing.fetch_data(limit=2)
        self.assertEqual(len(result["data"]), 2)
        self.assertFalse(result["hasPrevious"])
        self.assertTrue(result["hasNext"])
        self.assertEqual(result["next"], "W3siJGRhdGUiOiIyMDI2LTA0LTExVDEyOjQ4OjI5WiJ9XQ")

    def test_request_exception_propagates(self):
        with patch.object(
            self.ing, "make_request", side_effect=Exception("boom")
        ):
            with self.assertRaises(Exception):
                self.ing.fetch_data()


# ---------------------------------------------------------------------------
# get_platform_from_integrator
# ---------------------------------------------------------------------------

class TestGetPlatformFromIntegrator(unittest.TestCase):
    def setUp(self):
        self.ing = LiFiIngestor()

    def test_ios(self):
        self.assertEqual(self.ing.get_platform_from_integrator("vultisig-ios"), "iOS")

    def test_android(self):
        self.assertEqual(
            self.ing.get_platform_from_integrator("vultisig-android"), "Android"
        )

    def test_web(self):
        self.assertEqual(self.ing.get_platform_from_integrator("vultisig-web"), "Web")

    def test_mac(self):
        self.assertEqual(self.ing.get_platform_from_integrator("vultisig-mac"), "Mac")

    def test_windows(self):
        self.assertEqual(
            self.ing.get_platform_from_integrator("vultisig-windows"), "Windows"
        )

    def test_vultisig_0_returns_desktop_extension(self):
        self.assertEqual(
            self.ing.get_platform_from_integrator("vultisig-0"), "Desktop/Extension"
        )

    def test_bare_vultisig_returns_unknown(self):
        self.assertEqual(self.ing.get_platform_from_integrator("vultisig"), "Unknown")

    def test_empty_string_returns_unknown(self):
        self.assertEqual(self.ing.get_platform_from_integrator(""), "Unknown")

    def test_none_returns_unknown(self):
        self.assertEqual(self.ing.get_platform_from_integrator(None), "Unknown")

    def test_non_vultisig_returns_other(self):
        self.assertEqual(
            self.ing.get_platform_from_integrator("jumper.exchange"), "Other"
        )
        self.assertEqual(self.ing.get_platform_from_integrator("os-prod"), "Other")

    def test_case_insensitive(self):
        self.assertEqual(
            self.ing.get_platform_from_integrator("VULTISIG-IOS"), "iOS"
        )


# ---------------------------------------------------------------------------
# parse_swap — the two real transfers
# ---------------------------------------------------------------------------

class TestParseSwapRealTransfers(unittest.TestCase):
    def setUp(self):
        self.ing = LiFiIngestor()

    def test_transfer_0_wxpl_to_usdt0_parses(self):
        """Transfer 0: WXPL → USDT0 on Plasma (same-chain swap, jumper.exchange)."""
        result = self.ing.parse_swap(_clone(0))

        self.assertIsNotNone(result)
        self.assertEqual(result["source"], "lifi")

        # tx_hash uses transactionId as the primary identifier
        self.assertEqual(result["tx_hash"], REAL_TX_ID)
        self.assertEqual(result["in_tx_id"], REAL_TX_HASH)
        self.assertIsNone(result["block_height"])

        # Addresses
        self.assertEqual(result["user_address"], REAL_FROM_ADDR)
        self.assertEqual(result["in_address"], REAL_FROM_ADDR)

        # Assets: "{symbol}-{chainId}"
        self.assertEqual(result["in_asset"], "WXPL-9745")
        self.assertEqual(result["out_asset"], "USDT0-9745")

        # safe_float clips 9e18 base units down to 1e11; in_amount then
        # becomes 1e11 / 1e18 = 1e-7. This matches production behavior.
        self.assertEqual(result["in_amount_raw"], CLIPPED_AMOUNT_RAW)
        self.assertAlmostEqual(result["in_amount"], CLIPPED_IN_AMOUNT)

        # Receiving amount (1176337 base / 1e6) = 1.176337 (not clipped)
        self.assertAlmostEqual(result["out_amount"], 1.176337, places=8)

        # USD values preserved verbatim
        self.assertAlmostEqual(result["in_amount_usd"], 1.1769, places=6)
        self.assertAlmostEqual(result["out_amount_usd"], 1.1763, places=6)
        self.assertAlmostEqual(result["in_price_usd"], 0.130765, places=6)
        self.assertAlmostEqual(result["out_price_usd"], 1.0, places=6)

        # Fees: network = send_gas + recv_gas = $0.0001 + $0.0001 = $0.0002
        # affiliate = 0 (no feeCollection step), liquidity = max(0, in - out - aff) = $0.0006
        self.assertAlmostEqual(result["network_fee_usd"], 0.0002, places=6)
        self.assertEqual(result["affiliate_fee_usd"], 0)
        self.assertAlmostEqual(result["liquidity_fee_usd"], 0.0006, places=5)
        self.assertAlmostEqual(result["total_fee_usd"], 0.0008, places=5)

        # Pool = "from_chain-to_chain"
        self.assertEqual(result["pool_1"], "9745-9745")
        self.assertIsNone(result["pool_2"])
        self.assertEqual(result["pools_used"], ["9745-9745"])

        # Flags / metadata
        self.assertFalse(result["is_streaming_swap"])
        self.assertIsNone(result["swap_slip"])
        self.assertEqual(result["volume_tier"], "<=$100")
        self.assertEqual(result["swap_type"], "bridge")
        self.assertEqual(result["swap_status"], "DONE")
        self.assertEqual(result["memo"], "jumper.exchange")
        # jumper.exchange doesn't match any vultisig platform → 'Other'
        self.assertEqual(result["platform"], "Other")

        # LiFi doesn't produce any affiliate addresses
        self.assertEqual(result["affiliate_addresses"], [])
        self.assertEqual(result["affiliate_fees_bps"], [])

        # Timestamp: unix 1775911736 → 2026-04-11 (roughly)
        self.assertIsInstance(result["timestamp"], datetime)
        self.assertEqual(
            result["timestamp"],
            datetime.fromtimestamp(1775911736, timezone.utc),
        )
        self.assertEqual(result["date_only"], result["timestamp"].date())

        # Output addresses (single non-affiliate output)
        out_addresses = json.loads(result["out_addresses"])
        self.assertEqual(len(out_addresses), 1)
        self.assertEqual(out_addresses[0]["address"], REAL_FROM_ADDR)
        self.assertEqual(out_addresses[0]["coins"][0]["asset"], "USDT0-9745")
        self.assertEqual(out_addresses[0]["coins"][0]["amount"], "1176337")
        self.assertFalse(out_addresses[0]["affiliate"])

        self.assertEqual(result["out_tx_ids"], [REAL_TX_HASH])
        self.assertEqual(result["out_heights"], [None])  # LiFi has no heights

        # network_fees_raw: [{asset: in_asset, amount: int(gas_usd * 1e8)}]
        # $0.0001 * 1e8 = 10000
        nf = json.loads(result["network_fees_raw"])
        self.assertEqual(nf, [{"asset": "WXPL-9745", "amount": "10000"}])

        # metadata_complete preserves the bridge metadata
        md = json.loads(result["metadata_complete"])
        self.assertEqual(md["tool"], "kyberswap")
        self.assertEqual(md["status"], "DONE")
        self.assertEqual(md["substatus"], "COMPLETED")
        self.assertEqual(md["from_chain_id"], 9745)
        self.assertEqual(md["to_chain_id"], 9745)
        self.assertEqual(md["receiving_tx_hash"], REAL_TX_HASH)
        self.assertEqual(
            md["lifi_explorer_link"],
            "https://scan.li.fi/tx/0xa5a2cc477396bb98ef331cae72a2883c6667ec7637fb3c1f265fdee4d1abcc01",
        )
        self.assertEqual(md["included_steps"], 1)

        # raw_data round-trips with bridge_metadata injected
        rd = json.loads(result["raw_data"])
        self.assertIn("bridge_metadata", rd)
        self.assertEqual(rd["bridge_metadata"]["tool"], "kyberswap")
        self.assertEqual(rd["transactionId"], REAL_TX_ID)

    def test_transfer_1_eth_to_usdc_parses(self):
        """Transfer 1: ETH → USDC on Base (same-chain swap, os-prod)."""
        result = self.ing.parse_swap(_clone(1))
        self.assertIsNotNone(result)

        self.assertEqual(
            result["tx_hash"],
            "0xeee2f6d38a6a8a43904d413e1659b2e17168c317fd32cbc6dd3eb78b62b0a349",
        )
        self.assertEqual(
            result["user_address"], "0xb75bb26e8dbff32c6674d8f0db2750a71924a23e"
        )
        self.assertEqual(result["in_asset"], "ETH-8453")
        self.assertEqual(result["out_asset"], "USDC-8453")
        self.assertEqual(result["pool_1"], "8453-8453")
        self.assertEqual(result["pools_used"], ["8453-8453"])

        # 8e15 base ETH is also > 1e11 cap → clipped to 1e-7 just like transfer 0.
        self.assertEqual(result["in_amount_raw"], CLIPPED_AMOUNT_RAW)
        self.assertAlmostEqual(result["in_amount"], CLIPPED_IN_AMOUNT)

        # Receiving 17952020 / 1e6 = 17.95202 (not clipped)
        self.assertAlmostEqual(result["out_amount"], 17.95202, places=8)

        self.assertAlmostEqual(result["in_amount_usd"], 17.9563, places=6)
        self.assertAlmostEqual(result["out_amount_usd"], 17.9503, places=6)
        self.assertAlmostEqual(result["in_price_usd"], 2244.54, places=4)
        self.assertAlmostEqual(result["out_price_usd"], 0.999906, places=6)

        # network = 0.0044 + 0.0044 = 0.0088; affiliate = 0; liq = 17.9563 - 17.9503 - 0 = 0.006
        self.assertAlmostEqual(result["network_fee_usd"], 0.0088, places=6)
        self.assertEqual(result["affiliate_fee_usd"], 0)
        self.assertAlmostEqual(result["liquidity_fee_usd"], 0.006, places=4)
        self.assertAlmostEqual(result["total_fee_usd"], 0.0148, places=4)

        # network_fees_raw: 0.0044 * 1e8 = 440000
        nf = json.loads(result["network_fees_raw"])
        self.assertEqual(nf, [{"asset": "ETH-8453", "amount": "440000"}])

        # Volume tier still '<=$100' for ~$18
        self.assertEqual(result["volume_tier"], "<=$100")

        # os-prod → 'Other'
        self.assertEqual(result["platform"], "Other")
        self.assertEqual(result["memo"], "os-prod")

        # Timestamp: unix 1775911731 → matches upstream
        self.assertEqual(
            result["timestamp"],
            datetime.fromtimestamp(1775911731, timezone.utc),
        )

        md = json.loads(result["metadata_complete"])
        self.assertEqual(md["tool"], "sushiswap")
        self.assertEqual(md["included_steps"], 1)

    def test_every_real_transfer_parses_successfully(self):
        """Both real transfers must parse without returning None."""
        for i, transfer in enumerate(REAL_RESPONSE["data"]):
            with self.subTest(transfer=i):
                self.assertIsNotNone(
                    self.ing.parse_swap(copy.deepcopy(transfer))
                )


# ---------------------------------------------------------------------------
# parse_swap — variants / edge cases
# ---------------------------------------------------------------------------

class TestParseSwapVariants(unittest.TestCase):
    def setUp(self):
        self.ing = LiFiIngestor()

    def test_fee_collection_step_produces_affiliate_fee(self):
        """
        A 'feeCollection' tool in includedSteps means the user paid an
        integrator fee. affiliate_fee_usd should equal:
            (fromAmount - toAmount) / 10**decimals * priceUSD
        """
        raw = _clone(0)
        raw["sending"]["includedSteps"] = [
            {
                "tool": "feeCollection",
                # Use small amounts well below safe_float's 1e11 cap.
                "fromAmount": "1000000",   # 1 USDC-ish (6 decimals)
                "toAmount": "990000",      # 0.99 USDC
                "fromToken": {"symbol": "X", "decimals": 6, "priceUSD": "1"},
                "toToken": {"symbol": "X", "decimals": 6, "priceUSD": "1"},
            }
        ]
        # Update the outer sending token to match (decimals=6, price=1) so the
        # affiliate-fee math uses the priceUSD we expect.
        raw["sending"]["token"]["decimals"] = 6
        raw["sending"]["token"]["priceUSD"] = "1"
        raw["sending"]["amount"] = "1000000"

        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        # fee = (1000000 - 990000) / 1e6 * 1 = 0.01 USD
        self.assertAlmostEqual(result["affiliate_fee_usd"], 0.01, places=6)

    def test_non_fee_collection_steps_do_not_add_affiliate_fee(self):
        """Only 'feeCollection' tool contributes to affiliate_fee_usd."""
        raw = _clone(0)
        raw["sending"]["includedSteps"] = [
            {
                "tool": "kyberswap",
                "fromAmount": "1000000",
                "toAmount": "500000",
                "fromToken": {"symbol": "X", "decimals": 6, "priceUSD": "1"},
            }
        ]
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["affiliate_fee_usd"], 0)

    def test_fee_costs_fee_split_takes_precedence_over_included_steps(self):
        """
        When feeCosts is present, affiliate_fee_usd comes from
        feeSplit.integratorFee priced in the fee's own token — NOT from the
        includedSteps delta (which is ~0 on most bridge routes).
        """
        raw = _clone(0)
        raw["sending"]["includedSteps"] = [
            {
                "tool": "feeCollection",
                "fromAmount": "1000000",
                "toAmount": "1000000",  # zero delta — legacy path would yield 0
            }
        ]
        # Real-world shape: Jul 22 2026 HYPE swap, integrator fee 1.58920204 HYPE @ $58.58.
        raw["feeCosts"] = [
            {
                "name": "LIFI Fixed Fee",
                "token": {"symbol": "HYPE", "decimals": 18, "priceUSD": "58.58"},
                "amount": "2913537073590000000",
                "feeSplit": {
                    "lifiFee": "1324335033450000000",
                    "integratorFee": "1589202040140000000",
                },
            }
        ]
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result["affiliate_fee_usd"], 93.095, places=2)

    def test_fee_costs_sums_multiple_entries(self):
        fee_costs = [
            {
                "token": {"symbol": "USDC", "decimals": 6, "priceUSD": "1"},
                "feeSplit": {"integratorFee": "250000"},
            },
            {
                "token": {"symbol": "WETH", "decimals": 18, "priceUSD": "2000"},
                "feeSplit": {"integratorFee": "1000000000000000"},  # 0.001 WETH
            },
        ]
        usd = LiFiIngestor.integrator_fee_usd_from_fee_costs(fee_costs)
        self.assertAlmostEqual(usd, 0.25 + 2.0, places=6)

    def test_fee_costs_tolerates_missing_split_and_bad_values(self):
        fee_costs = [
            {"token": {"decimals": 6, "priceUSD": "1"}},  # no feeSplit
            {"token": {"decimals": 6, "priceUSD": "1"}, "feeSplit": {"integratorFee": "not-a-number"}},
            {"token": None, "feeSplit": {"integratorFee": "1000000000000000000"}},  # defaults: 18 dec, price 0
        ]
        self.assertEqual(LiFiIngestor.integrator_fee_usd_from_fee_costs(fee_costs), 0)

    def test_empty_fee_costs_falls_back_to_included_steps(self):
        """feeCosts: [] (older rows) must keep using the includedSteps delta."""
        raw = _clone(0)
        raw["feeCosts"] = []
        raw["sending"]["includedSteps"] = [
            {"tool": "feeCollection", "fromAmount": "1000000", "toAmount": "990000"}
        ]
        raw["sending"]["token"]["decimals"] = 6
        raw["sending"]["token"]["priceUSD"] = "1"
        raw["sending"]["amount"] = "1000000"
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result["affiliate_fee_usd"], 0.01, places=6)

    def test_missing_timestamp_falls_back_to_now(self):
        raw = _clone(0)
        raw["sending"]["timestamp"] = 0
        before = datetime.now(timezone.utc)
        result = self.ing.parse_swap(raw)
        after = datetime.now(timezone.utc)
        self.assertIsNotNone(result)
        self.assertGreaterEqual(result["timestamp"], before)
        self.assertLessEqual(result["timestamp"], after)

    def test_zero_decimals_does_not_divide(self):
        """When token decimals=0, in_amount should equal the raw amount."""
        raw = _clone(0)
        raw["sending"]["token"]["decimals"] = 0
        raw["sending"]["amount"] = "5000"
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["in_amount"], 5000.0)

    def test_missing_transaction_id_falls_back_to_tx_hash(self):
        raw = _clone(0)
        del raw["transactionId"]
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["tx_hash"], REAL_TX_HASH)

    def test_missing_receiving_tx_hash_means_empty_out_tx_ids(self):
        raw = _clone(0)
        raw["receiving"]["txHash"] = ""
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["out_tx_ids"], [])

    def test_ios_integrator_sets_platform(self):
        raw = _clone(0)
        raw["metadata"]["integrator"] = "vultisig-ios"
        result = self.ing.parse_swap(raw)
        self.assertEqual(result["platform"], "iOS")
        self.assertEqual(result["memo"], "vultisig-ios")

    def test_android_integrator_sets_platform(self):
        raw = _clone(0)
        raw["metadata"]["integrator"] = "vultisig-android"
        self.assertEqual(self.ing.parse_swap(raw)["platform"], "Android")

    def test_web_integrator_sets_platform(self):
        raw = _clone(0)
        raw["metadata"]["integrator"] = "vultisig-web"
        self.assertEqual(self.ing.parse_swap(raw)["platform"], "Web")

    def test_cross_chain_bridge_pool_format(self):
        """Set different from/to chain IDs — pool_1 should reflect the route."""
        raw = _clone(0)
        raw["sending"]["token"]["chainId"] = 1       # Ethereum
        raw["receiving"]["token"]["chainId"] = 137   # Polygon
        result = self.ing.parse_swap(raw)
        self.assertEqual(result["pool_1"], "1-137")
        self.assertEqual(result["pools_used"], ["1-137"])
        # bridge_metadata records both sides
        md = json.loads(result["metadata_complete"])
        self.assertEqual(md["from_chain_id"], 1)
        self.assertEqual(md["to_chain_id"], 137)

    def test_liquidity_fee_clamped_at_zero_when_out_exceeds_in(self):
        """If out_usd > in_usd, liquidity_fee_usd must be 0 (max(0, ...))."""
        raw = _clone(0)
        raw["sending"]["amountUSD"] = "10"
        raw["receiving"]["amountUSD"] = "100"
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["liquidity_fee_usd"], 0)

    def test_malformed_transfer_returns_none(self):
        """Passing something without sending/receiving keys is survivable."""
        result = self.ing.parse_swap({})
        # With empty dict, sending=={} and receiving=={} → still parses with defaults.
        self.assertIsNotNone(result)
        self.assertEqual(result["in_asset"], "-")
        self.assertEqual(result["in_amount"], 0)
        self.assertEqual(result["in_amount_usd"], 0)

    def test_garbage_amount_handled_by_safe_float(self):
        raw = _clone(0)
        raw["sending"]["amount"] = "not-a-number"
        raw["sending"]["amountUSD"] = "also-garbage"
        result = self.ing.parse_swap(raw)
        self.assertIsNotNone(result)
        self.assertEqual(result["in_amount"], 0)
        self.assertEqual(result["in_amount_usd"], 0)

    def test_volume_tier_classifies_correctly_for_larger_transfer(self):
        raw = _clone(0)
        raw["sending"]["amountUSD"] = "1500"
        result = self.ing.parse_swap(raw)
        self.assertEqual(result["volume_tier"], "1000-5000")

    def test_missing_metadata_integrator_returns_unknown(self):
        raw = _clone(0)
        raw["metadata"] = {}
        result = self.ing.parse_swap(raw)
        self.assertEqual(result["platform"], "Unknown")
        self.assertEqual(result["memo"], "")


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# parse_swap — tool capture + aggregator attribution
# ---------------------------------------------------------------------------

class TestToolAttribution(unittest.TestCase):
    def setUp(self):
        self.ing = LiFiIngestor()

    def test_tool_stored_as_column(self):
        result = self.ing.parse_swap(_clone(0))
        self.assertEqual(result["tool"], "kyberswap")

    def test_unattributed_tool_keeps_lifi_source(self):
        result = self.ing.parse_swap(_clone(0))
        self.assertEqual(result["source"], "lifi")

    def test_attributed_tool_relabels_source(self):
        """1inch-executed LI.FI swaps are credited to 1inch; the relabel keeps
        them out of the lifi series (source != '1inch' filters)."""
        raw = _clone(0)
        raw["tool"] = "1inch"
        result = self.ing.parse_swap(raw)
        self.assertEqual(result["source"], "1inch")
        self.assertEqual(result["tool"], "1inch")

    def test_missing_tool_stored_as_none(self):
        raw = _clone(0)
        raw.pop("tool", None)
        result = self.ing.parse_swap(raw)
        self.assertIsNone(result["tool"])
        self.assertEqual(result["source"], "lifi")


