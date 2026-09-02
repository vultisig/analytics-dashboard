"""Unit tests for SwapKit sender classification."""
import os
import sys
import unittest
from unittest.mock import MagicMock

if "psycopg2" not in sys.modules:
    _mock_pg = MagicMock()
    _mock_pg.OperationalError = type("OperationalError", (Exception,), {})
    _mock_pg.InterfaceError = type("InterfaceError", (Exception,), {})
    sys.modules["psycopg2"] = _mock_pg

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from config import config  # noqa: E402
from ingestors.swapkit_senders import (  # noqa: E402
    BACKFILL_SOURCE_PROTOCOLS,
    all_swapkit_senders,
    backfill_swapkit_rows,
    is_swapkit_fee_sender,
    is_swapkit_payout_sender,
    plan_swapkit_enrichment,
    protocol_for_sender,
)


THORCHAIN_ROUTER = "0xD37BbE5744D730a1d98d8DC97c42F0Ca46aD7146"
ASGARD_VAULT = "0xF5E10380213880111522Dd0EFD3DbB45B9F62Bcc"
SKWRAP = "0x9025B8ff35Ca44f7018C3a37FE0f69e63DBb0743"
PAYOUT_FLASHNET = "0xf70da97812CB96acDF810712Aa562db8dfA3dbEF"
PAYOUT_NEAR_A = "0x2CfF890f0378a11913B6129B2E97417a2c302680"
PAYOUT_NEAR_B = "0x8443e89848Ef39017184C42171388674C551FF9A"
KYBER_ROUTER = "0x6131b5fae19ea4f9d964eac0408e4408b66337b5"
UNKNOWN = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"


class TestProtocolForSender(unittest.TestCase):
    def test_skwrap_is_swapkit_fee(self):
        self.assertEqual(protocol_for_sender(SKWRAP), "swapkit")
        self.assertTrue(is_swapkit_fee_sender(SKWRAP))
        self.assertFalse(is_swapkit_payout_sender(SKWRAP))

    def test_payout_wallets_are_swapkit_payout(self):
        for addr in (PAYOUT_FLASHNET, PAYOUT_NEAR_A, PAYOUT_NEAR_B):
            self.assertEqual(protocol_for_sender(addr), "swapkit")
            self.assertTrue(is_swapkit_payout_sender(addr))
            self.assertFalse(is_swapkit_fee_sender(addr))

    def test_mixed_case_matches(self):
        self.assertEqual(protocol_for_sender(SKWRAP.lower()), "swapkit")
        self.assertEqual(protocol_for_sender(SKWRAP.upper()), "swapkit")

    def test_unknown_and_empty_are_none(self):
        self.assertIsNone(protocol_for_sender(UNKNOWN))
        self.assertIsNone(protocol_for_sender(KYBER_ROUTER))
        self.assertIsNone(protocol_for_sender(""))
        self.assertIsNone(protocol_for_sender(None))

    def test_midgard_owned_settlement_is_other_not_a_provider(self):
        for addr in (THORCHAIN_ROUTER, ASGARD_VAULT):
            self.assertEqual(protocol_for_sender(addr), "other")
            self.assertFalse(is_swapkit_fee_sender(addr))
            self.assertFalse(is_swapkit_payout_sender(addr))

    def test_no_sender_is_claimed_by_two_owners(self):
        swapkit = config.SWAPKIT_FEE_SENDERS | config.SWAPKIT_PAYOUT_SENDERS
        self.assertEqual(swapkit & config.MIDGARD_OWNED_FEE_WALLET_SENDERS, frozenset())
        for owner_set in (swapkit, config.MIDGARD_OWNED_FEE_WALLET_SENDERS):
            for addr in owner_set:
                self.assertEqual(addr, addr.lower())

    def test_config_sets_match_issue_addresses(self):
        self.assertIn(SKWRAP.lower(), config.SWAPKIT_FEE_SENDERS)
        self.assertEqual(len(config.SWAPKIT_FEE_SENDERS), 1)
        self.assertEqual(len(config.SWAPKIT_PAYOUT_SENDERS), 3)
        self.assertIn("swapkit", config.DEX_REVENUE_PROVIDERS)
        self.assertNotIn("swapkit", config.ARKHAM_PROVIDERS)


class TestBackfill(unittest.TestCase):
    def test_updates_other_and_kyberswap_only(self):
        db = MagicMock()
        cur = MagicMock()
        cur.rowcount = 4
        db.cursor.return_value = cur

        updated = backfill_swapkit_rows(db)

        self.assertEqual(updated, 4)
        sql, params = cur.execute.call_args[0]
        self.assertIn("LOWER(from_address) IN %s", sql)
        self.assertIn("swap_volume_usd = NULL", sql)
        self.assertIn("WHEN volume_data_source = 'estimated' THEN %s", sql)
        self.assertEqual(params[0], "swapkit")
        self.assertEqual(params[1], "unpriced")
        self.assertEqual(set(params[2]), set(all_swapkit_senders()))
        self.assertEqual(set(params[3]), set(BACKFILL_SOURCE_PROTOCOLS))
        self.assertNotIn("1inch", params[3])


class TestPlanEnrichment(unittest.TestCase):
    def test_skwrap_prices_fee_never_derives_volume(self):
        updates, stamp = plan_swapkit_enrichment(SKWRAP, 20.0, 0)
        self.assertEqual(updates, {"actual_fee_usd": 20.0})
        self.assertEqual(stamp, "unpriced")

    def test_payout_prices_fee_without_volume(self):
        updates, stamp = plan_swapkit_enrichment(PAYOUT_NEAR_A, 2056.0, 0)
        self.assertEqual(updates, {"actual_fee_usd": 2056.0})
        self.assertEqual(stamp, "payout")

    def test_existing_fee_not_rewritten(self):
        updates, stamp = plan_swapkit_enrichment(SKWRAP, 20.0, 20.0)
        self.assertEqual(updates, {})
        self.assertEqual(stamp, "unpriced")


if __name__ == "__main__":
    unittest.main()
