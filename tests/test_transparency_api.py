"""Contract tests for the transparency page's public API."""
import os
import sys
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch


_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

import api_server  # noqa: E402
from chain_reader import ChainReaderError, LockedPosition  # noqa: E402
from config import config  # noqa: E402


TRADE_HASH = "0x1111111111111111111111111111111111111111111111111111111111111111"
LAST_SUCCESSFUL_SYNC = datetime(2026, 7, 13, 12, 30, tzinfo=timezone.utc)
POSITION = LockedPosition(
    token_id=1_195_906,
    token0=config.USDC_ADDRESS,
    token1=config.VULT_ADDRESS,
    fee=10_000,
    tick_lower=260_000,
    tick_upper=280_000,
    liquidity=1_000_000,
    amount0=Decimal("1000"),
    amount1=Decimal("5000"),
)


class TestTransparencyApi(unittest.TestCase):
    def setUp(self):
        api_server.rate_limit_store.clear()
        self.reader = MagicMock()
        self.reader.get_fee_treasury_balances.return_value = {
            "VULT": Decimal("3004339"),
            "USDC": Decimal("15984"),
            "ETH": Decimal("1.66"),
        }
        self.reader.get_spot_price.return_value = Decimal("0.105")
        self.reader.get_locked_positions.return_value = [POSITION]
        self.db = patch.object(api_server.db_manager, "execute_query", side_effect=self._query_result)
        self.reader_patch = patch.object(api_server, "transparency_reader", self.reader)
        self.db.start()
        self.reader_patch.start()
        self.client = api_server.app.test_client()

    def tearDown(self):
        self.reader_patch.stop()
        self.db.stop()

    def _query_result(self, query, *_args, **_kwargs):
        if "FROM sync_status" in query:
            return [{"last_successful_sync": LAST_SUCCESSFUL_SYNC}]
        if "FROM buyback_trades" in query:
            return [{
                "date": date(2026, 7, 1),
                "tx_hash": TRADE_HASH,
                "usdc_spent": Decimal("1000"),
                "vult_bought": Decimal("10000"),
                "price": Decimal("0.1"),
            }]
        if "date_trunc('month'" in query:
            if "FROM swaps" in query:
                return [{"date": "2026-07-01", "fees": Decimal("100")}]
            return [{"date": "2026-07-01", "fees": Decimal("20")}]
        if "FROM swaps" in query:
            return [{"fees": Decimal("1000")}]
        return [{"fees": Decimal("200")}]

    def test_summary_reports_the_pipeline_and_supply_ledger(self):
        response = self.client.get("/api/transparency/summary")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["fees"]["allTimeUsd"], 1200.0)
        self.assertEqual(body["buybacks"]["vultBought"], 10000.0)
        self.assertEqual(body["locked"]["positionCount"], 1)
        self.assertEqual(body["locked"]["percentOfSupply"], 0.005)
        supply = body["supply"]
        self.assertEqual(
            set(supply),
            {"totalVult", "circulatingVult", "protocolLockedVult", "treasuryUnallocatedVult"},
        )
        self.assertEqual(supply["circulatingVult"], 96_990_661.0)
        self.assertEqual(
            supply["circulatingVult"] + supply["protocolLockedVult"] + supply["treasuryUnallocatedVult"],
            supply["totalVult"],
        )
        self.assertEqual(body["fees"]["lastSuccessfulSync"], LAST_SUCCESSFUL_SYNC.isoformat())
        self.assertEqual(body["buybacks"]["lastSuccessfulSync"], LAST_SUCCESSFUL_SYNC.isoformat())

    def test_treasury_returns_live_receipt_balances_and_monthly_fees(self):
        response = self.client.get("/api/transparency/treasury")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["address"], config.FEE_TREASURY_ADDRESS)
        self.assertEqual(body["balances"]["VULT"], 3004339.0)
        self.assertEqual(body["monthlyInflow"], [{"date": "2026-07-01", "feesUsd": 120.0}])

    def test_buybacks_return_transaction_receipts_and_all_time_totals(self):
        response = self.client.get("/api/transparency/buybacks")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["walletAddress"], config.BUYBACK_WALLET_ADDRESS)
        self.assertEqual(body["trades"][0]["txHash"], TRADE_HASH)
        self.assertEqual(body["summary"]["usdcSpent"], 1000.0)
        self.assertEqual(body["summary"]["averagePrice"], 0.1)
        self.assertEqual(body["summary"]["lastSuccessfulSync"], LAST_SUCCESSFUL_SYNC.isoformat())
        buyback_query = next(
            call.args[0]
            for call in api_server.db_manager.execute_query.call_args_list
            if "FROM buyback_trades" in call.args[0]
        )
        self.assertIn("ORDER BY block_number DESC, tx_hash DESC", buyback_query)

    def test_locked_returns_verified_position_composition_and_receipt_addresses(self):
        response = self.client.get("/api/transparency/locked")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        position = body["positions"][0]
        self.assertEqual(body["ownerAddress"], config.DEAD_ADDRESS)
        self.assertEqual(position["tokenId"], POSITION.token_id)
        self.assertEqual(position["composition"]["VULT"], 5000.0)
        self.assertEqual(position["composition"]["USDC"], 1000.0)
        self.assertEqual(position["valueUsd"], 1525.0)

    def test_chain_read_failure_returns_an_honest_unavailable_response(self):
        self.reader.get_spot_price.side_effect = ChainReaderError("RPC unavailable")

        response = self.client.get("/api/transparency/locked")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["error"], "Transparency chain data unavailable")


if __name__ == "__main__":
    unittest.main()
