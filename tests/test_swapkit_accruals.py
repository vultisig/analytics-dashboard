"""Latest accrual snapshot shaping."""
import os
import sys
import unittest
from datetime import datetime, timezone

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from swapkit_accruals import LATEST_ACCRUALS_QUERY, query_params, summarize_accruals  # noqa: E402

SNAPSHOT = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
USDC = "nep141:eth-0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.omft.near"


def _row(platform, token, usd):
    return {"snapshot_at": SNAPSHOT, "platform": platform, "token_id": token, "amount_raw": 1, "amount_usd": usd}


class TestSummary(unittest.TestCase):
    def test_per_platform_stables_and_unpriced_counts(self):
        rows = [
            _row("Android", USDC, 978.744617),
            _row("Android", "nep141:sol.omft.near", None),
            _row("Web", USDC, 210.735664),
            _row("Web", "nep141:doge.omft.near", None),
            _row("Web", "nep141:zec.omft.near", None),
            _row("iOS", USDC, 785.248817),
        ]
        summary = summarize_accruals("near-intents", rows, lambda ts: ts.isoformat())
        self.assertEqual(summary["provider"], "near-intents")
        self.assertEqual(summary["snapshot_at"], "2026-09-01T12:00:00+00:00")
        self.assertEqual(summary["stable_usd"], 1974.73)
        self.assertEqual(
            summary["platforms"],
            [
                {"platform": "Android", "stable_usd": 978.74, "unpriced_tokens": 1},
                {"platform": "Web", "stable_usd": 210.74, "unpriced_tokens": 2},
                {"platform": "iOS", "stable_usd": 785.25, "unpriced_tokens": 0},
            ],
        )

    def test_empty_snapshot(self):
        summary = summarize_accruals("near-intents", [], lambda ts: ts)
        self.assertEqual(summary, {"provider": "near-intents", "snapshot_at": None, "stable_usd": 0.0, "platforms": []})

    def test_query_pins_latest_snapshot_of_the_provider(self):
        self.assertIn("MAX(snapshot_at)", LATEST_ACCRUALS_QUERY)
        self.assertEqual(LATEST_ACCRUALS_QUERY.count("%s"), 2)
        self.assertEqual(query_params("near-intents"), ("near-intents", "near-intents"))


if __name__ == "__main__":
    unittest.main()
