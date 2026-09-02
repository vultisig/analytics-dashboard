"""Sender-ownership re-verification plan over fee-wallet rows."""
import os
import sys
import unittest
from unittest.mock import MagicMock

if "psycopg2" not in sys.modules:
    _mock_pg = MagicMock()
    _mock_pg.OperationalError = type("OperationalError", (Exception,), {})
    _mock_pg.InterfaceError = type("InterfaceError", (Exception,), {})
    sys.modules["psycopg2"] = _mock_pg
if "requests" not in sys.modules:
    sys.modules["requests"] = MagicMock()

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from jobs.reverify_fee_wallet_senders import (  # noqa: E402
    REVENUE_PROTOCOLS,
    SENDER_OWNER_STAMP,
    apply_sender_ownership,
    plan_sender_ownership,
)
from ingestors.router_source_classifier import iter_never_router_checked_rows  # noqa: E402

THORCHAIN_ROUTER = "0xd37bbe5744d730a1d98d8dc97c42f0ca46ad7146"
ASGARD_VAULT = "0xf5e10380213880111522dd0efd3dbb45b9f62bcc"
SKWRAP = "0x9025b8ff35ca44f7018c3a37fe0f69e63dbb0743"
KYBER_ROUTER = "0x6131b5fae19ea4f9d964eac0408e4408b66337b5"
LIFI_DIAMOND = "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae"


class TestPlan(unittest.TestCase):
    def test_owned_senders_move_and_unknown_senders_stay_for_router_check(self):
        rows = [
            (1, THORCHAIN_ROUTER, "kyberswap"),
            (2, ASGARD_VAULT, "kyberswap"),
            (3, SKWRAP, "kyberswap"),
            (4, KYBER_ROUTER, "kyberswap"),
            (5, LIFI_DIAMOND, "kyberswap"),
            (6, SKWRAP, "swapkit"),
        ]
        self.assertEqual(plan_sender_ownership(rows), {"other": [1, 2], "swapkit": [3]})

    def test_only_revenue_protocols_are_candidates(self):
        self.assertEqual(REVENUE_PROTOCOLS, ("1inch", "kyberswap"))


class TestApply(unittest.TestCase):
    def test_writes_one_update_per_owner_and_commits(self):
        db = MagicMock()
        cur = MagicMock()
        cur.rowcount = 2
        db.cursor.return_value = cur

        moved = apply_sender_ownership(db, {"other": [1, 2], "swapkit": [3]})

        self.assertEqual(moved, 4)
        (other_sql, other_params), (swapkit_sql, swapkit_params) = [c.args for c in cur.execute.call_args_list]
        self.assertEqual(other_params, ("other", SENDER_OWNER_STAMP, [1, 2]))
        self.assertNotIn("swap_volume_usd", other_sql)
        self.assertEqual(swapkit_params, ("swapkit", "unpriced", [3]))
        self.assertIn("swap_volume_usd = NULL", swapkit_sql)
        db.commit.assert_called_once()


class TestNeverCheckedScope(unittest.TestCase):
    def test_scope_ignores_ingestor_and_skips_verified_rows(self):
        db = MagicMock()
        cur = MagicMock()
        cur.__iter__.return_value = iter([])
        db.cursor.return_value = cur

        list(iter_never_router_checked_rows(db))

        sql, params = cur.execute.call_args.args
        self.assertNotIn("fee_data_source", sql)
        self.assertIn("volume_data_source IS DISTINCT FROM %s", sql)
        self.assertIn("lifi_attribution", sql)
        self.assertEqual(params, ("router_check",))


if __name__ == "__main__":
    unittest.main()
