# tests/test_enricher_pricing.py
"""Unit tests for VolumeEnricher.resolve_fee_usd (symbol and contract pricing)."""
import os
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

if "psycopg2" not in sys.modules:
    _mock_pg = MagicMock()
    _mock_pg.OperationalError = type("OperationalError", (Exception,), {})
    _mock_pg.InterfaceError = type("InterfaceError", (Exception,), {})
    sys.modules["psycopg2"] = _mock_pg
if "psycopg2.extras" not in sys.modules:
    sys.modules["psycopg2.extras"] = MagicMock()
try:
    import aiohttp  # noqa: F401  (price_fetcher imports it; the tests mock the fetcher)
except ImportError:
    sys.modules["aiohttp"] = MagicMock()

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from enrichers import enrich_arkham_volumes as eav  # noqa: E402

TS = datetime(2025, 12, 29, 10, 0, tzinfo=timezone.utc)
SYRUP = "0x643c4e15d7d62ad0abec4a9bd4b001aa3ef52d66"


def _enricher(coingecko_price=None):
    enricher = eav.VolumeEnricher.__new__(eav.VolumeEnricher)
    enricher.price_fetcher = MagicMock()
    enricher.price_fetcher.get_historical_price.return_value = coingecko_price
    return enricher


def _llama(price, confidence=0.99, coin=f"ethereum:{SYRUP}"):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"coins": {coin: {"price": price, "confidence": confidence}}}
    return resp


def _record(**kw):
    base = {"chain": "Ethereum", "timestamp": TS, "actual_fee_usd": 0,
            "fee_token_symbol": "SYRUP", "fee_token_address": SYRUP, "fee_amount_raw": "489"}
    base.update(kw)
    return base


class TestResolveFeeUsd(unittest.TestCase):
    @patch.object(eav.requests, "get")
    def test_mapped_symbol_uses_coingecko(self, mock_get):
        enricher = _enricher(coingecko_price=1.0)
        fee = enricher.resolve_fee_usd(_record(fee_token_symbol="USDC", fee_amount_raw="12.5"))
        self.assertAlmostEqual(fee, 12.5)
        mock_get.assert_not_called()

    @patch.object(eav.requests, "get")
    def test_unmapped_token_priced_by_contract(self, mock_get):
        mock_get.return_value = _llama(0.3537876)
        fee = _enricher().resolve_fee_usd(_record())
        self.assertAlmostEqual(fee, 489 * 0.3537876, places=6)
        url = mock_get.call_args[0][0]
        self.assertIn(f"/{int(TS.timestamp())}/ethereum:{SYRUP}", url)

    @patch.object(eav.requests, "get")
    def test_low_confidence_price_rejected(self, mock_get):
        mock_get.return_value = _llama(5.0, confidence=0.5)
        self.assertEqual(_enricher().resolve_fee_usd(_record()), 0.0)

    @patch.object(eav.requests, "get")
    def test_llama_timeout_is_transient(self, mock_get):
        import requests as req
        mock_get.side_effect = req.exceptions.Timeout("slow")
        with self.assertRaises(eav.TransientPriceError):
            _enricher().resolve_fee_usd(_record())

    @patch.object(eav.requests, "get")
    def test_llama_404_is_definitive_zero(self, mock_get):
        import requests as req
        resp = MagicMock(status_code=404)
        mock_get.return_value.raise_for_status.side_effect = req.exceptions.HTTPError(response=resp)
        self.assertEqual(_enricher().resolve_fee_usd(_record()), 0.0)

    @patch.object(eav.requests, "get")
    def test_transient_failure_leaves_row_unstamped(self, mock_get):
        import requests as req
        resp = MagicMock(status_code=503)
        mock_get.return_value.raise_for_status.side_effect = req.exceptions.HTTPError(response=resp)
        enricher = _enricher()
        enricher.apply_updates = MagicMock()
        enricher.extract_swap_path = MagicMock(return_value={})
        row = _record(tx_hash="0xabc", swap_volume_usd=None, token_in_symbol=None, token_out_symbol=None)
        self.assertFalse(enricher.enrich_record(row))
        enricher.apply_updates.assert_not_called()

    @patch.object(eav.requests, "get")
    def test_native_fee_has_no_contract_lookup(self, mock_get):
        enricher = _enricher(coingecko_price=None)
        fee = enricher.resolve_fee_usd(_record(fee_token_symbol="ETH", fee_token_address="", fee_amount_raw="0.05"))
        self.assertEqual(fee, 0.0)
        mock_get.assert_not_called()

    @patch.object(eav.requests, "get")
    def test_hyperevm_native_symbol_mapped(self, mock_get):
        enricher = _enricher(coingecko_price=40.0)
        fee = enricher.resolve_fee_usd(_record(chain="HyperEVM", fee_token_symbol="HYPE",
                                               fee_token_address="", fee_amount_raw="0.25"))
        self.assertAlmostEqual(fee, 10.0)
        enricher.price_fetcher.get_historical_price.assert_called_with("hyperliquid", TS.date())

    def test_existing_fee_kept(self):
        self.assertEqual(_enricher().resolve_fee_usd(_record(actual_fee_usd=7.5)), 7.5)


if __name__ == "__main__":
    unittest.main()
