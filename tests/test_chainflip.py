"""Unit tests for the Chainflip SwapKit volume ingestor."""
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
COMPLETED = {
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
        "transactionRefsBySwapRequestId": {
            "nodes": [{"ref": "0x930e49786ac75863d92e87a63e8d4d31c1292e506bedee2846e1c8d545e7f22b"}]
        },
        "swapsBySwapRequestId": {"nodes": [{"swapInputValueUsd": "198.86"}]},
    },
}


class TestChainflipParse(unittest.TestCase):
    def setUp(self):
        self.ingestor = ChainflipIngestor()

    def test_completed_swap_is_swapkit_volume_only(self):
        row = self.ingestor.parse_swap(COMPLETED)
        self.assertIsNotNone(row)
        self.assertEqual(row["source"], "swapkit")
        self.assertEqual(row["tool"], CHAINFLIP_TOOL)
        self.assertEqual(row["affiliate_fee_usd"], 0.0)
        self.assertEqual(row["platform"], "All")
        self.assertEqual(row["in_amount_usd"], 6960.3434031472)
        self.assertAlmostEqual(row["in_amount"], 6962.6674)
        self.assertEqual(row["volume_tier"], "5000-10000")

    def test_incomplete_swap_skipped(self):
        raw = {
            "type": "AFFILIATE",
            "swapRequest": {
                "nativeId": "1",
                "completedBlockTimestamp": None,
                "depositValueUsd": "10",
            },
        }
        self.assertIsNone(self.ingestor.parse_swap(raw))

    def test_usdc_decimals(self):
        self.assertAlmostEqual(amount_from_raw("1000000", "Usdc"), 1.0)

    def test_graphql_url_allowlisted(self):
        self.assertEqual(
            allowlisted_graphql_url(config.CHAINFLIP_GRAPHQL_URL),
            "https://explorer-service-processor.chainflip.io/graphql",
        )
        with self.assertRaises(ValueError):
            allowlisted_graphql_url("https://evil.example/graphql")


class TestChainflipIngest(unittest.TestCase):
    def test_ingest_inserts_parsed_rows(self):
        ingestor = ChainflipIngestor()
        page = {
            "account": {
                "swaps": {
                    "nodes": [COMPLETED],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }
        with patch.object(ingestor, "fetch_page", return_value=page), patch.object(
            ingestor, "_insert_swaps", return_value=1
        ) as insert:
            result = ingestor.ingest()
        self.assertIsNone(result["error"])
        self.assertEqual(result["source"], "chainflip")
        self.assertEqual(result["inserted"], 1)
        inserted = insert.call_args[0][0]
        self.assertEqual(inserted[0]["source"], "swapkit")
        self.assertEqual(inserted[0]["affiliate_fee_usd"], 0.0)
