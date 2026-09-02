"""Unattributed fee-wallet inflow summary."""
import os
import sys
import unittest

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from fee_wallet_gap import (  # noqa: E402
    DEFAULT_WINDOW_DAYS,
    MAX_WINDOW_DAYS,
    TOP_SENDERS,
    UNATTRIBUTED_QUERY,
    clamp_window_days,
    query_params,
    summarize_unattributed,
)

THORCHAIN_ROUTER = "0xd37bbe5744d730a1d98d8dc97c42f0ca46ad7146"


class TestWindow(unittest.TestCase):
    def test_defaults_and_clamps(self):
        self.assertEqual(clamp_window_days(None), DEFAULT_WINDOW_DAYS)
        self.assertEqual(clamp_window_days("abc"), DEFAULT_WINDOW_DAYS)
        self.assertEqual(clamp_window_days("0"), 1)
        self.assertEqual(clamp_window_days("9999"), MAX_WINDOW_DAYS)
        self.assertEqual(clamp_window_days("7"), 7)

    def test_query_is_parameterized_on_other_rows_only(self):
        self.assertIn("protocol = %s", UNATTRIBUTED_QUERY)
        self.assertIn("fee_data_source = 'etherscan'", UNATTRIBUTED_QUERY)
        stables, protocol, days = query_params(30)
        self.assertEqual(protocol, "other")
        self.assertIn("USDC", stables)
        self.assertEqual(days, 30)


class TestSummary(unittest.TestCase):
    def test_totals_and_top_senders(self):
        rows = [
            {"from_address": f"0x{i:040x}", "transfers": i + 1, "stable_usd": 100.0 * (10 - i)}
            for i in range(TOP_SENDERS + 2)
        ]
        rows[0]["from_address"] = THORCHAIN_ROUTER
        summary = summarize_unattributed(rows, 30)
        self.assertEqual(summary["days"], 30)
        self.assertEqual(summary["transfers"], sum(r["transfers"] for r in rows))
        self.assertEqual(summary["stable_usd"], sum(r["stable_usd"] for r in rows))
        self.assertEqual(len(summary["top_senders"]), TOP_SENDERS)
        self.assertEqual(summary["top_senders"][0]["from_address"], "0xd37bbe57…7146")
        self.assertNotIn(THORCHAIN_ROUTER, str(summary))

    def test_empty_window(self):
        self.assertEqual(
            summarize_unattributed([], 7),
            {"days": 7, "transfers": 0, "stable_usd": 0.0, "top_senders": []},
        )


if __name__ == "__main__":
    unittest.main()
