"""Keyless Near-Intents accrual snapshots per app.

Balances are the live `intents.near` reads from 2026-09-01.
"""
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

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from config import config  # noqa: E402
from ingestors.near_intents_accrual import (  # noqa: E402
    PROVIDER,
    SYNC_SOURCE,
    TOKEN_PAGE_LIMIT,
    NearIntentsAccrualReader,
    build_accrual_rows,
    snapshot_bucket,
    stable_usd,
)

USDC_ETH = "nep141:eth-0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.omft.near"
DOGE = "nep141:doge.omft.near"
WEB_BALANCES = [(DOGE, "800000000"), (USDC_ETH, "210735664"), ("nep141:zec.omft.near", "0")]
SNAPSHOT = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)


class TestRows(unittest.TestCase):
    def test_stables_priced_others_raw_zero_dropped(self):
        rows = build_accrual_rows(SNAPSHOT, "Web", WEB_BALANCES)
        self.assertEqual([r["token_id"] for r in rows], [DOGE, USDC_ETH])
        usdc = rows[1]
        self.assertEqual(usdc["platform"], "Web")
        self.assertEqual(usdc["provider"], PROVIDER)
        self.assertEqual(usdc["amount_raw"], 210735664)
        self.assertAlmostEqual(usdc["amount_usd"], 210.735664)
        self.assertIsNone(rows[0]["amount_usd"])

    def test_unknown_token_is_unpriced(self):
        self.assertIsNone(stable_usd("nep245:v2_1.omni.hot.tg:1117_", 6581617))
        self.assertAlmostEqual(stable_usd(USDC_ETH, 785248817), 785.248817)


class TestAccounts(unittest.TestCase):
    def test_three_per_app_implicit_accounts(self):
        platforms = dict(config.NEAR_INTENTS_APP_ACCOUNTS)
        self.assertEqual(set(platforms), {"iOS", "Android", "Web"})
        for implicit in platforms.values():
            self.assertEqual(len(implicit), 64)
            int(implicit, 16)


class TestIngest(unittest.TestCase):
    def test_snapshot_written_per_app_and_failures_isolated(self):
        reader = NearIntentsAccrualReader()
        reader.accounts = (("iOS", "ios-acct"), ("Web", "web-acct"))

        def balances(account):
            if account == "ios-acct":
                raise RuntimeError("rpc timeout")
            return WEB_BALANCES

        with patch.object(reader, "fetch_balances", side_effect=balances), patch.object(
            reader, "_insert_accruals", side_effect=len
        ) as insert:
            result = reader.ingest(None)

        self.assertEqual(result["source"], SYNC_SOURCE)
        self.assertEqual(result["inserted"], 2)
        self.assertEqual(result["error"], "iOS: rpc timeout")
        self.assertIsNone(result["next_state"])
        rows = insert.call_args.args[0]
        self.assertEqual({r["platform"] for r in rows}, {"Web"})
        self.assertEqual(len({r["snapshot_at"] for r in rows}), 1)

    def test_view_call_decodes_result_bytes(self):
        reader = NearIntentsAccrualReader()
        response = MagicMock()
        response.json.return_value = {"result": {"result": list(b'[{"token_id": "x"}]')}}
        with patch.object(reader.session, "post", return_value=response) as post:
            tokens = reader.call_view("mt_tokens_for_owner", {"account_id": "a"})
        self.assertEqual(tokens, [{"token_id": "x"}])
        self.assertEqual(post.call_args.args[0], config.NEAR_RPC_URL)
        body = post.call_args.kwargs["json"]
        self.assertEqual(body["params"]["account_id"], "intents.near")
        self.assertEqual(body["params"]["method_name"], "mt_tokens_for_owner")

    def test_token_enumeration_pages_until_short_page(self):
        reader = NearIntentsAccrualReader()
        full = [{"token_id": f"t{i}"} for i in range(TOKEN_PAGE_LIMIT)]
        tail = [{"token_id": "last"}]
        with patch.object(reader, "call_view", side_effect=[full, tail]) as view:
            token_ids = reader.fetch_token_ids("acct")
        self.assertEqual(len(token_ids), TOKEN_PAGE_LIMIT + 1)
        self.assertEqual(view.call_args_list[1].args[1]["from_index"], str(TOKEN_PAGE_LIMIT))

    def test_snapshot_buckets_to_the_hour(self):
        now = datetime(2026, 9, 1, 23, 23, 37, tzinfo=timezone.utc)
        self.assertEqual(snapshot_bucket(now), datetime(2026, 9, 1, 23, 0, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
