"""Unit tests for the Chainflip SwapKit volume ingestor.

Fixtures are live explorer records captured 2026-09-01 from the three
per-app affiliate accounts.
"""
import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

if "psycopg2" not in sys.modules:
    _mock_pg = MagicMock()
    _mock_pg.OperationalError = type("OperationalError", (Exception,), {})
    _mock_pg.InterfaceError = type("InterfaceError", (Exception,), {})
    sys.modules["psycopg2"] = _mock_pg

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from config import config  # noqa: E402
from ingestors.chainflip import (  # noqa: E402
    CHAINFLIP_TOOL,
    allowlisted_graphql_url,
    amount_from_raw,
    ChainflipIngestor,
)

WINDOWS_ACCOUNT = "cFLTzujZfsG2mdaQ4MJRZ36uD4Y2U5y7sLhEccS7N2gfqQPqj"
WINDOWS_SWAP = {
    "type": "AFFILIATE",
    "swapRequest": {
        "nativeId": "1758291",
        "sourceAsset": "Usdc",
        "destinationAsset": "Sol",
        "sourceChain": "Ethereum",
        "destinationChain": "Solana",
        "sourceAddress": None,
        "destinationAddress": "A3eQFss2TmtNQQYAFcK5LN6JEVq8uRFGdXeAQEzrg2tG",
        "requestedBlockTimestamp": "2026-08-27T03:25:12.000000+00:00",
        "completedBlockTimestamp": "2026-08-27T03:28:30.000000+00:00",
        "depositAmount": "223860000000",
        "depositValueUsd": "223816.29",
        "totalBrokerCommissionBps": 65,
        "egress": {"amount": "2182867581203", "valueUsd": "221929.3669998693"},
        "transactionRefsBySwapRequestId": {
            "nodes": [{"ref": "0x3c29e58ec14b51393e8b6890bac2264d53d9bbb20dd0f953aa77220261d81b61"}]
        },
        "swapsBySwapRequestId": {
            "nodes": [{"swapInputValueUsd": "223816.29", "swapOutputValueUsd": "221929.37"}]
        },
    },
}
FLIP_SWAP = {
    "type": "AFFILIATE",
    "swapRequest": {
        "nativeId": "1713550",
        "sourceAsset": "Usdc",
        "destinationAsset": "Flip",
        "sourceChain": "Ethereum",
        "destinationChain": "Ethereum",
        "sourceAddress": None,
        "destinationAddress": "0x27248b275cf67f689a0b0a83a91412f601333038",
        "requestedBlockTimestamp": "2026-08-10T00:18:12.000000+00:00",
        "completedBlockTimestamp": "2026-08-10T00:21:48.000000+00:00",
        "depositAmount": "6962667400",
        "depositValueUsd": "6960.3434031472",
        "totalBrokerCommissionBps": 40,
        "egress": None,
        "transactionRefsBySwapRequestId": {"nodes": []},
        "swapsBySwapRequestId": {"nodes": [{"swapInputValueUsd": "198.86", "swapOutputValueUsd": "6901.10"}]},
    },
}


def _page(nodes, has_next=False, end_cursor=None):
    return {
        "account": {
            "swaps": {
                "nodes": nodes,
                "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
            }
        }
    }


class TestChainflipParse(unittest.TestCase):
    def setUp(self):
        self.ingestor = ChainflipIngestor()

    def test_completed_swap_is_swapkit_volume_only(self):
        row = self.ingestor.parse_swap(WINDOWS_SWAP, "Web")
        self.assertEqual(row["source"], "swapkit")
        self.assertEqual(row["tool"], CHAINFLIP_TOOL)
        self.assertEqual(row["affiliate_fee_usd"], 0.0)
        self.assertEqual(row["platform"], "Web")
        self.assertEqual(row["in_amount_usd"], 223816.29)
        self.assertAlmostEqual(row["in_amount"], 223860.0)
        self.assertEqual(row["volume_tier"], "100000-250000")

    def test_row_key_is_native_id_and_ref_lands_in_in_tx_id(self):
        row = self.ingestor.parse_swap(WINDOWS_SWAP, "Web")
        self.assertEqual(row["tx_hash"], "cf-1758291")
        self.assertEqual(
            row["in_tx_id"],
            "0x3c29e58ec14b51393e8b6890bac2264d53d9bbb20dd0f953aa77220261d81b61",
        )

    def test_missing_ref_keeps_native_id_key(self):
        row = self.ingestor.parse_swap(FLIP_SWAP, "Web")
        self.assertEqual(row["tx_hash"], "cf-1713550")
        self.assertEqual(row["in_tx_id"], "cf-1713550")

    def test_egress_fills_out_amount(self):
        row = self.ingestor.parse_swap(WINDOWS_SWAP, "Web")
        self.assertAlmostEqual(row["out_amount"], 2182.867581203)
        self.assertAlmostEqual(row["out_amount_usd"], 221929.3669998693)

    def test_missing_egress_falls_back_to_leg_output(self):
        row = self.ingestor.parse_swap(FLIP_SWAP, "Web")
        self.assertEqual(row["out_amount"], 0.0)
        self.assertAlmostEqual(row["out_amount_usd"], 6901.10)

    def test_incomplete_swap_skipped(self):
        raw = {"type": "AFFILIATE", "swapRequest": {"nativeId": "1", "completedBlockTimestamp": None}}
        self.assertIsNone(self.ingestor.parse_swap(raw, "Web"))

    def test_unreadable_timestamp_skipped_not_substituted(self):
        raw = {"type": "AFFILIATE", "swapRequest": dict(WINDOWS_SWAP["swapRequest"], completedBlockTimestamp="garbage")}
        self.assertIsNone(self.ingestor.parse_swap(raw, "Web"))

    def test_usdc_decimals(self):
        self.assertAlmostEqual(amount_from_raw("1000000", "Usdc"), 1.0)

    def test_graphql_url_allowlisted(self):
        self.assertEqual(
            allowlisted_graphql_url(config.CHAINFLIP_GRAPHQL_URL),
            "https://explorer-service-processor.chainflip.io/graphql",
        )
        with self.assertRaises(ValueError):
            allowlisted_graphql_url("https://evil.example/graphql")


class TestChainflipAccounts(unittest.TestCase):
    def test_three_per_app_accounts_from_swapkit(self):
        platforms = dict(config.CHAINFLIP_AFFILIATE_ACCOUNTS)
        self.assertEqual(set(platforms), {"iOS", "Android", "Web"})
        self.assertEqual(platforms["Web"], WINDOWS_ACCOUNT)
        self.assertEqual(len({acct for _, acct in config.CHAINFLIP_AFFILIATE_ACCOUNTS}), 3)


class TestChainflipIngest(unittest.TestCase):
    def test_ingest_walks_every_account_with_its_platform(self):
        ingestor = ChainflipIngestor()
        ingestor.accounts = (("iOS", "cF-ios"), ("Web", WINDOWS_ACCOUNT))
        pages = {"cF-ios": _page([FLIP_SWAP]), WINDOWS_ACCOUNT: _page([WINDOWS_SWAP])}
        with patch.object(
            ingestor, "fetch_page", side_effect=lambda account, after=None: pages[account]
        ), patch.object(ingestor, "_insert_swaps", side_effect=len) as insert:
            result = ingestor.ingest(None)
        self.assertIsNone(result["error"])
        self.assertEqual(result["source"], "chainflip")
        self.assertEqual(result["inserted"], 2)
        platforms = [call.args[0][0]["platform"] for call in insert.call_args_list]
        self.assertEqual(platforms, ["iOS", "Web"])
        state = json.loads(result["next_state"])
        self.assertEqual(state["done"], {"iOS": True, "Web": True})

    def test_missing_account_fails_that_feed_only(self):
        ingestor = ChainflipIngestor()
        ingestor.accounts = (("iOS", "cF-missing"), ("Web", WINDOWS_ACCOUNT))
        pages = {"cF-missing": {"account": None}, WINDOWS_ACCOUNT: _page([WINDOWS_SWAP])}
        with patch.object(
            ingestor, "fetch_page", side_effect=lambda account, after=None: pages[account]
        ), patch.object(ingestor, "_insert_swaps", side_effect=len):
            result = ingestor.ingest(None)
        self.assertIn("iOS: Chainflip affiliate account not found", result["error"])
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(json.loads(result["next_state"])["done"], {"Web": True})


if __name__ == "__main__":
    unittest.main()
