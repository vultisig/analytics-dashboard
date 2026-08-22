"""Unit tests for the Near-Intents SwapKit volume ingestor."""
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
from ingestors.near_intents import (  # noqa: E402
    JWT_MISSING,
    NEAR_TOOL,
    NearIntentsIngestor,
    allowlisted_near_url,
)
SUCCESS_TX = {
    "originAsset": "nep141:eth-0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.omft.near",
    "destinationAsset": "nep141:wrap.near",
    "depositAddress": "GDJ4JZXZELZD737NVFORH4PSSQDWFDZTKW3AIDKHYQG23ZXBPDGGQBJK",
    "depositMemo": "12345",
    "recipient": "somebody.near",
    "status": "SUCCESS",
    "createdAt": "2025-12-31T12:00:00.000Z",
    "createdAtTimestamp": 1767182400,
    "intentHashes": "GnGk38hvi92tTWDYMMS8CWYnVT4fixmfBrnqSErCDMTu",
    "amountInFormatted": "22.130108",
    "amountOutFormatted": "22.113697",
    "amountIn": "22130108",
    "amountInUsd": "22.1272",
    "amountOut": "22113697",
    "amountOutUsd": "22.1108",
    "originChainTxHashes": [
        "0x9bcff372aee89b648c922b850573b22387c31d693079f5e37cd255814e2d615a"
    ],
    "senders": ["0x1234567890abcdef1234567890abcdef12345678"],
    "appFees": [{"fee": 50, "recipient": "vultisigswapkit.near"}],
}


class TestNearParse(unittest.TestCase):
    def setUp(self):
        self.ingestor = NearIntentsIngestor()

    def test_success_is_swapkit_volume_only(self):
        row = self.ingestor.parse_swap(SUCCESS_TX)
        self.assertIsNotNone(row)
        self.assertEqual(row["source"], "swapkit")
        self.assertEqual(row["tool"], NEAR_TOOL)
        self.assertEqual(row["affiliate_fee_usd"], 0.0)
        self.assertEqual(row["platform"], "All")
        self.assertEqual(row["in_amount_usd"], 22.1272)
        self.assertEqual(
            row["tx_hash"],
            "0x9bcff372aee89b648c922b850573b22387c31d693079f5e37cd255814e2d615a",
        )
        self.assertEqual(row["volume_tier"], "<=$100")

    def test_failed_status_skipped(self):
        raw = dict(SUCCESS_TX, status="FAILED")
        self.assertIsNone(self.ingestor.parse_swap(raw))

    def test_url_allowlisted(self):
        self.assertEqual(
            allowlisted_near_url(config.NEAR_INTENTS_API_URL),
            "https://explorer.near-intents.org/api/v0/transactions",
        )
        with self.assertRaises(ValueError):
            allowlisted_near_url("https://evil.example/api")


class TestNearFailOpen(unittest.TestCase):
    def test_missing_jwt_skips_without_request(self):
        ingestor = NearIntentsIngestor()
        ingestor.jwt = ""
        with patch.object(ingestor, "fetch_page") as fetch:
            result = ingestor.ingest()
        fetch.assert_not_called()
        self.assertEqual(result["error"], JWT_MISSING)
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["source"], "near-intents")
