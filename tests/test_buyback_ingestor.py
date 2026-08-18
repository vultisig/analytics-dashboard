"""Unit tests for the Etherscan-backed VULT buyback ingestor."""
import os
import sys
import unittest
from contextlib import ExitStack
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch


if "psycopg2" not in sys.modules:
    _mock_pg = MagicMock()
    _mock_pg.OperationalError = type("OperationalError", (Exception,), {})
    _mock_pg.InterfaceError = type("InterfaceError", (Exception,), {})
    sys.modules["psycopg2"] = _mock_pg

if "psycopg2.extras" not in sys.modules:
    sys.modules["psycopg2.extras"] = MagicMock()

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from config import config  # noqa: E402


TX_HASH = "0x1111111111111111111111111111111111111111111111111111111111111111"
OTHER_HASH = "0x2222222222222222222222222222222222222222222222222222222222222222"


def _transfer(**overrides):
    transfer = {
        "hash": TX_HASH,
        "blockNumber": "123456",
        "timeStamp": "1750377600",
        "from": "0x1111111111111111111111111111111111111111",
        "to": config.BUYBACK_WALLET_ADDRESS,
        "contractAddress": config.VULT_ADDRESS,
        "tokenDecimal": "18",
        "value": "2000000000000000000",
    }
    transfer.update(overrides)
    return transfer


VULT_IN = _transfer()
USDC_OUT = _transfer(
    from_address=config.BUYBACK_WALLET_ADDRESS,
    to="0x2222222222222222222222222222222222222222",
    contractAddress=config.USDC_ADDRESS,
    tokenDecimal="6",
    value="500000",
)
USDC_OUT["from"] = USDC_OUT.pop("from_address")


def _make_ingestor():
    from ingestors import buyback_ingestor as mod

    mod.ETHERSCAN_API_KEY = "test-key"
    mod.DATABASE_URL = "postgresql://test"
    ingestor = mod.BuybackIngestor()
    connection = MagicMock()
    connection.closed = False
    ingestor.db = connection
    return ingestor, connection


class TestBuildBuybackTrades(unittest.TestCase):
    def test_pairs_usdc_out_with_vult_in_by_transaction_hash(self):
        from ingestors.buyback_ingestor import build_buyback_trades

        trades = build_buyback_trades([VULT_IN, USDC_OUT])

        self.assertEqual(len(trades), 1)
        trade = trades[0]
        self.assertEqual(trade.date, date(2025, 6, 20))
        self.assertEqual(trade.tx_hash, TX_HASH)
        self.assertEqual(trade.usdc_spent, Decimal("0.5"))
        self.assertEqual(trade.vult_bought, Decimal("2"))
        self.assertEqual(trade.price, Decimal("0.25"))

    def test_ignores_unpaired_or_wrong_direction_transfers(self):
        from ingestors.buyback_ingestor import build_buyback_trades

        wrong_direction = _transfer(
            hash=OTHER_HASH,
            from_address=config.BUYBACK_WALLET_ADDRESS,
            to="0x3333333333333333333333333333333333333333",
        )
        wrong_direction["from"] = wrong_direction.pop("from_address")

        self.assertEqual(build_buyback_trades([VULT_IN, wrong_direction]), [])


class TestBuybackIngestor(unittest.TestCase):
    def setUp(self):
        self.ingestor, self.connection = _make_ingestor()
        self.cursor = self.connection.cursor.return_value
        self.cursor.fetchone.return_value = (None,)
        self.cursor.rowcount = 1

    @patch("ingestors.buyback_ingestor.execute_values")
    @patch("ingestors.buyback_ingestor.requests.get")
    def test_ingest_creates_schema_and_uses_tx_hash_conflict_key(self, mock_get, mock_values):
        response = MagicMock()
        response.json.return_value = {"status": "1", "result": [VULT_IN, USDC_OUT]}
        mock_get.return_value = response

        result = self.ingestor.ingest()

        self.assertEqual(result["inserted"], 1)
        self.assertIsNone(result["error"])
        schema_sql = next(
            call.args[0]
            for call in self.cursor.execute.call_args_list
            if "CREATE TABLE IF NOT EXISTS buyback_trades" in call.args[0]
        )
        self.assertIn("CREATE TABLE IF NOT EXISTS buyback_trades", schema_sql)
        insert_sql = mock_values.call_args.args[1]
        self.assertIn("ON CONFLICT (tx_hash) DO NOTHING", insert_sql)
        self.assertEqual(mock_values.call_args.args[2][0][1], TX_HASH)

    @patch("ingestors.buyback_ingestor.execute_values")
    def test_insert_trades_counts_all_batches(self, mock_values):
        from ingestors import buyback_ingestor as module
        from ingestors.buyback_ingestor import build_buyback_trades

        trades = build_buyback_trades([VULT_IN, USDC_OUT]) * 2
        with patch.object(module, "INSERT_PAGE_SIZE", 1):
            inserted = self.ingestor._insert_trades(trades)

        self.assertEqual(inserted, 2)
        self.assertEqual(mock_values.call_count, 2)

    @patch("ingestors.buyback_ingestor.requests.get")
    def test_rechecks_the_latest_block_for_late_indexed_transfers(self, mock_get):
        self.cursor.fetchone.return_value = (123456,)
        response = MagicMock()
        response.json.return_value = {"status": "0", "result": []}
        mock_get.return_value = response

        self.ingestor._fetch_transfers()

        self.assertEqual(mock_get.call_args.kwargs["params"]["startblock"], 123456)

    @patch("ingestors.buyback_ingestor.requests.get")
    def test_no_transactions_response_is_not_a_sync_error(self, mock_get):
        response = MagicMock()
        response.json.return_value = {
            "status": "0",
            "message": "No transactions found",
            "result": "No transactions found",
        }
        mock_get.return_value = response

        transfers, error = self.ingestor._fetch_transfers()

        self.assertEqual(transfers, [])
        self.assertIsNone(error)

    @patch("ingestors.buyback_ingestor.requests.get")
    def test_notok_rate_limit_response_is_a_sync_error(self, mock_get):
        response = MagicMock()
        response.json.return_value = {
            "status": "0",
            "message": "NOTOK",
            "result": "Max rate limit reached",
        }
        mock_get.return_value = response

        transfers, error = self.ingestor._fetch_transfers()

        self.assertEqual(transfers, [])
        self.assertIn("Max rate limit", error)


class TestSyncWiring(unittest.TestCase):
    def test_etherscan_sync_runs_buyback_ingestion_sequentially(self):
        with patch("logging.basicConfig"):
            import main

        etherscan = MagicMock()
        etherscan.integrators = []
        etherscan.ingest.return_value = []
        buybacks = MagicMock()
        buybacks.ingest.return_value = {"inserted": 1, "error": None}

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "EtherscanIngestor", return_value=etherscan))
            stack.enter_context(patch.object(main, "BuybackIngestor", return_value=buybacks))
            stack.enter_context(patch.object(main, "THORChainIngestor"))
            stack.enter_context(patch.object(main, "MayaChainIngestor"))
            stack.enter_context(patch.object(main, "LiFiIngestor"))
            stack.enter_context(patch.object(main.db_manager, "get_connection"))
            update = stack.enter_context(patch.object(main.db_manager, "update_sync_status"))
            stack.enter_context(patch.object(main, "reclassify_etherscan_rows"))
            stack.enter_context(patch.object(main, "sync_attributed_gap_rows", return_value=0))
            stack.enter_context(patch.object(main, "VolumeEnricher"))
            main.SyncService().sync_source("etherscan")

        buybacks.ingest.assert_called_once()
        self.assertIn("buybacks", [call.args[0] for call in update.call_args_list])


if __name__ == "__main__":
    unittest.main()
