"""Fee-wallet receipts crawled from a chain RPC (Robinhood, Optimism, Base, Avalanche).

Fixture is the live 2026-09-01 KyberSwap fee transfer on Robinhood Chain
(tx 0x2048ee83…, 0.121 UP from the Kyber router to the fee wallet).
"""
import json
import os
import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

if "psycopg2" not in sys.modules:
    _mock_pg = MagicMock()
    _mock_pg.OperationalError = type("OperationalError", (Exception,), {})
    _mock_pg.InterfaceError = type("InterfaceError", (Exception,), {})
    sys.modules["psycopg2"] = _mock_pg
    sys.modules["psycopg2.extras"] = MagicMock()

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from config import config  # noqa: E402
from ingestors.rpc_fee_ingestor import (  # noqa: E402
    INSERT_COLUMNS,
    SYNC_SOURCE,
    RpcFeeIngestor,
    build_row,
    classify_protocol,
    decode_string,
    topic_address,
)

FEE_WALLET = "0x8E247a480449c84a5fDD25974A8501f3EFa4ABb9"
KYBER_ROUTER = "0x6131b5fae19ea4f9d964eac0408e4408b66337b5"
THORCHAIN_ROUTER = "0xd37bbe5744d730a1d98d8dc97c42f0ca46ad7146"
UP_TOKEN = "0x57c0e45cb534413d1c20a4240955d6bb250bb4f1"
KYBER_LOG = {
    "address": UP_TOKEN,
    "topics": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        topic_address(KYBER_ROUTER),
        topic_address(FEE_WALLET),
    ],
    "data": hex(121160459545731658),
    "blockNumber": hex(51900000),
    "transactionHash": "0x2048ee83f3876151780aa99ce479cf102794db06e372897a1c583e6c68d471ab",
}
TS = datetime(2026, 9, 1, 18, 3, 18, tzinfo=timezone.utc)
ROBINHOOD = (4663, "Robinhood", "https://rpc.example", 800_000, 60_000_000)
AVALANCHE = (43114, "Avalanche", "https://rpc.example", 2_000, 5_800_000)


def _ingestor(chains):
    ingestor = RpcFeeIngestor()
    ingestor.chains = chains
    ingestor.integrators = (("kyberswap", FEE_WALLET),)
    return ingestor


class TestClassify(unittest.TestCase):
    def test_router_match_keeps_receiver_provider(self):
        self.assertEqual(classify_protocol("kyberswap", KYBER_ROUTER, KYBER_ROUTER), "kyberswap")

    def test_foreign_router_never_inherits_receiver_provider(self):
        self.assertEqual(classify_protocol("kyberswap", "0x1111111254eeb25477b68fb85ed929f73a960582", "0xabc"), "other")

    def test_midgard_sender_is_other_even_without_router(self):
        self.assertEqual(classify_protocol("kyberswap", THORCHAIN_ROUTER, THORCHAIN_ROUTER), "other")

    def test_null_tx_to_is_other(self):
        self.assertEqual(classify_protocol("kyberswap", None, "0xabc"), "other")


class TestRow(unittest.TestCase):
    def test_row_matches_insert_columns_and_is_pre_classified(self):
        row = build_row(KYBER_LOG, "kyberswap", "Robinhood", TS, KYBER_ROUTER, ("UP", 18))
        named = dict(zip(INSERT_COLUMNS, row))
        self.assertEqual(len(row), len(INSERT_COLUMNS))
        self.assertEqual(named["protocol"], "kyberswap")
        self.assertEqual(named["chain"], "Robinhood")
        self.assertEqual(named["fee_token_symbol"], "UP")
        self.assertEqual(named["fee_amount_raw"], "0.121160459545731658")
        self.assertEqual(named["amount_in"], Decimal("0.121160459545731658"))
        self.assertEqual(named["from_address"], KYBER_ROUTER)
        self.assertEqual(named["to_address"], FEE_WALLET.lower())
        self.assertEqual(named["block_number"], 51900000)
        self.assertEqual(named["fee_data_source"], "rpc")
        self.assertEqual(named["volume_data_source"], "router_check")
        self.assertEqual(named["actual_fee_usd"], 0)

    def test_decode_symbol_string_and_bytes32(self):
        dynamic = "0x" + "20".rjust(64, "0") + "2".rjust(64, "0") + "5550".ljust(64, "0")
        self.assertEqual(decode_string(dynamic), "UP")
        self.assertEqual(decode_string("0x" + "504f4e53".ljust(64, "0")), "PONS")


class TestIngest(unittest.TestCase):
    def test_first_run_looks_back_when_db_has_nothing(self):
        ingestor = _ingestor((ROBINHOOD,))
        rpc = MagicMock()
        rpc.latest_block.return_value = 52_135_581
        rpc.transfer_logs.return_value = [KYBER_LOG]
        rpc.block_timestamp.return_value = TS
        rpc.token_meta.return_value = ("UP", 18)
        rpc.tx_to.return_value = KYBER_ROUTER
        with patch.object(ingestor, "client", return_value=rpc), patch.object(
            ingestor, "_last_block_in_db", return_value=None
        ), patch.object(ingestor, "_insert_rows", side_effect=len) as insert:
            result = ingestor.ingest(None)

        start = 52_135_581 - 60_000_000 + 1
        first_stop = start + 800_000 - 1
        self.assertEqual(rpc.transfer_logs.call_args_list[0].args, (FEE_WALLET, start, first_stop))
        self.assertEqual(len(rpc.transfer_logs.call_args_list), config.RPC_FEE_MAX_WINDOWS_PER_SYNC)
        self.assertEqual(result["source"], SYNC_SOURCE)
        self.assertEqual(result["latest_ts"], TS)
        expected_stop = start + 800_000 * config.RPC_FEE_MAX_WINDOWS_PER_SYNC - 1
        self.assertEqual(json.loads(result["next_state"]), {"last_block": {"Robinhood": expected_stop}})
        self.assertEqual(dict(zip(INSERT_COLUMNS, insert.call_args.args[0][0]))["protocol"], "kyberswap")

    def test_first_run_resumes_after_rows_an_earlier_crawl_stored(self):
        ingestor = _ingestor((AVALANCHE,))
        rpc = MagicMock()
        rpc.latest_block.return_value = 94_241_898
        rpc.transfer_logs.return_value = []
        with patch.object(ingestor, "client", return_value=rpc), patch.object(
            ingestor, "_last_block_in_db", return_value=94_230_000
        ), patch.object(ingestor, "_insert_rows", side_effect=len):
            result = ingestor.ingest(None)
        self.assertEqual(rpc.transfer_logs.call_args_list[0].args, (FEE_WALLET, 94_230_001, 94_232_000))
        self.assertEqual(json.loads(result["next_state"]), {"last_block": {"Avalanche": 94_241_898}})

    def test_persisted_state_wins_and_windows_stop_at_head(self):
        ingestor = _ingestor((ROBINHOOD,))
        rpc = MagicMock()
        rpc.latest_block.return_value = 1_000_500
        rpc.transfer_logs.return_value = []
        with patch.object(ingestor, "client", return_value=rpc), patch.object(
            ingestor, "_last_block_in_db", side_effect=AssertionError("must not hit the DB")
        ), patch.object(ingestor, "_insert_rows", side_effect=len):
            result = ingestor.ingest('{"last_block": {"Robinhood": 1000000}}')
        rpc.transfer_logs.assert_called_once_with(FEE_WALLET, 1_000_001, 1_000_500)
        self.assertEqual(json.loads(result["next_state"]), {"last_block": {"Robinhood": 1_000_500}})
        self.assertIsNone(result["error"])

    def test_rpc_failure_keeps_previous_block_and_reports(self):
        ingestor = _ingestor((ROBINHOOD,))
        rpc = MagicMock()
        rpc.latest_block.side_effect = RuntimeError("RPC eth_blockNumber rate-limited")
        with patch.object(ingestor, "client", return_value=rpc):
            result = ingestor.ingest('{"last_block": {"Robinhood": 42}}')
        self.assertIn("Robinhood: RPC eth_blockNumber rate-limited", result["error"])
        self.assertEqual(json.loads(result["next_state"]), {"last_block": {"Robinhood": 42}})


class TestConfig(unittest.TestCase):
    def test_rpc_chains_are_the_ones_the_etherscan_free_plan_cannot_serve(self):
        from ingestors.etherscan_ingestor import DEFAULT_CHAINS
        rpc_ids = {chain[0] for chain in config.RPC_FEE_CHAINS}
        self.assertEqual(rpc_ids, {4663, 10, 8453, 43114})
        etherscan_ids = {chain_id for chain_id, _ in DEFAULT_CHAINS}
        self.assertNotIn(4663, etherscan_ids)
        for chain in config.RPC_FEE_CHAINS:
            self.assertEqual(len(chain), 5)
            self.assertGreater(chain[4], chain[3])


if __name__ == "__main__":
    unittest.main()
