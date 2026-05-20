# tests/test_etherscan_ingestor.py
"""
Unit tests for ingestors/etherscan_ingestor.py (EtherscanIngestor).

Covers the failure modes that bit us with Arkham (silent 402 swallowed as
"no new data") plus the happy path of multi-chain ingest.
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Mock psycopg2 before importing the ingestor (it may not be installed in CI).
if "psycopg2" not in sys.modules:
    _mock_pg = MagicMock()
    _mock_pg.OperationalError = type("OperationalError", (Exception,), {})
    _mock_pg.InterfaceError = type("InterfaceError", (Exception,), {})
    sys.modules["psycopg2"] = _mock_pg

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)


# Sample Etherscan V2 `tokentx` response (Ethereum mainnet, VULT fee token
# arriving at the KyberSwap fee receiver). Truncated to the fields the
# ingestor actually reads.
SAMPLE_TRANSFER = {
    "blockNumber": "25138175",
    "timeStamp": "1779300887",
    "hash": "0x1a1c86f026f29df5cf863c44bb187ac6ade5863149812a50bd59b430e78efa8c",
    "from": "0x6131b5fae19ea4f9d964eac0408e4408b66337b5",
    "to": "0x8e247a480449c84a5fdd25974a8501f3efa4abb9",
    "contractAddress": "0xb788144df611029c60b859df47e79b7726c4deba",
    "tokenSymbol": "VULT",
    "tokenDecimal": "18",
    "value": "149354384523928218",
}

# Same shape but TO a different address — should be filtered out by
# insert_transfer (defends against `tokentx?address=X` returning both
# FROM-X and TO-X transfers).
SAMPLE_OUTBOUND = {
    "blockNumber": "25138200",
    "timeStamp": "1779300999",
    "hash": "0xbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeef",
    "from": "0x8e247a480449c84a5fdd25974a8501f3efa4abb9",
    "to": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    "contractAddress": "0xb788144df611029c60b859df47e79b7726c4deba",
    "tokenSymbol": "VULT",
    "tokenDecimal": "18",
    "value": "1000000000000000000",
}

RECEIVER = "0x8E247a480449c84a5fDD25974A8501f3EFa4ABb9"


def _make_ingestor():
    """Build an EtherscanIngestor with all I/O mocked out."""
    with patch.dict(os.environ, {
        "ETHERSCAN_API_KEY": "test-key-12345",
        "DATABASE_URL": "postgresql://test",
    }):
        # Mock the actual DB connection (psycopg2.connect is already a mock).
        from ingestors.etherscan_ingestor import EtherscanIngestor

        ing = EtherscanIngestor(
            integrators=[("kyberswap", RECEIVER)],
            chains=[(1, "Ethereum")],
        )
        mock_conn = MagicMock()
        mock_conn.closed = False
        ing.db = mock_conn
        return ing, mock_conn


class TestFetchChainPage(unittest.TestCase):
    def setUp(self):
        self.ing, _ = _make_ingestor()

    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_status_1_returns_transfers(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "1", "result": [SAMPLE_TRANSFER]}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        transfers, err = self.ing.fetch_chain_page(1, RECEIVER, 0, 1)
        self.assertEqual(len(transfers), 1)
        self.assertIsNone(err)

    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_no_transactions_found_is_clean_empty(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "0", "message": "No transactions found", "result": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        transfers, err = self.ing.fetch_chain_page(56, RECEIVER, 0, 1)
        self.assertEqual(transfers, [])
        self.assertIsNone(err, "empty-chain result should NOT bubble as error")

    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_402_or_other_status_returns_error(self, mock_get):
        """The Arkham silent-failure regression. A non-OK API response must
        surface as an error so sync_status reflects truth."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "0", "message": "Max rate limit reached", "result": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        transfers, err = self.ing.fetch_chain_page(1, RECEIVER, 0, 1)
        self.assertEqual(transfers, [])
        self.assertIsNotNone(err)
        self.assertIn("Max rate limit", err)

    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_http_exception_returns_error(self, mock_get):
        import requests as req
        mock_get.side_effect = req.exceptions.Timeout("timed out")

        transfers, err = self.ing.fetch_chain_page(1, RECEIVER, 0, 1)
        self.assertEqual(transfers, [])
        self.assertIsNotNone(err)
        self.assertIn("HTTP error", err)


class TestInsertTransfer(unittest.TestCase):
    def setUp(self):
        self.ing, self.mock_conn = _make_ingestor()
        self.mock_cursor = self.mock_conn.cursor.return_value
        self.mock_cursor.rowcount = 1

    def test_inserts_inbound_transfer(self):
        ok = self.ing.insert_transfer(SAMPLE_TRANSFER, "kyberswap", "Ethereum", RECEIVER)
        self.assertTrue(ok)
        # Verify an INSERT call landed
        calls = [c for c in self.mock_cursor.execute.call_args_list if "INSERT" in c[0][0]]
        self.assertEqual(len(calls), 1)
        params = calls[0][0][1]
        self.assertEqual(params[0], SAMPLE_TRANSFER["hash"])  # tx_hash
        self.assertEqual(params[1], "Ethereum")                # chain
        self.assertEqual(params[2], "kyberswap")               # protocol
        self.assertEqual(params[5], "VULT")                    # fee_token_symbol
        self.assertEqual(params[18], "etherscan")              # fee_data_source

    def test_filters_outbound_transfer(self):
        """Transfer FROM the receiver address (someone moving fee tokens out)
        must be dropped — we only care about deposits."""
        ok = self.ing.insert_transfer(SAMPLE_OUTBOUND, "kyberswap", "Ethereum", RECEIVER)
        self.assertFalse(ok)
        # No INSERT should have been executed
        for c in self.mock_cursor.execute.call_args_list:
            self.assertNotIn("INSERT", c[0][0])

    def test_missing_hash_skips(self):
        bad = {**SAMPLE_TRANSFER}
        del bad["hash"]
        ok = self.ing.insert_transfer(bad, "kyberswap", "Ethereum", RECEIVER)
        self.assertFalse(ok)

    def test_decimal_scaling(self):
        """fee_amount_raw should be the decimal-scaled value, not the wei int."""
        self.ing.insert_transfer(SAMPLE_TRANSFER, "kyberswap", "Ethereum", RECEIVER)
        calls = [c for c in self.mock_cursor.execute.call_args_list if "INSERT" in c[0][0]]
        params = calls[0][0][1]
        amount_raw_str = params[7]   # fee_amount_raw
        amount_in = params[13]       # amount_in
        # 149354384523928218 / 1e18 = 0.149354384523928218
        self.assertAlmostEqual(float(amount_raw_str), 0.149354384523928218, places=12)
        self.assertAlmostEqual(amount_in, 0.149354384523928218, places=12)


class TestIngestChain(unittest.TestCase):
    def setUp(self):
        self.ing, self.mock_conn = _make_ingestor()
        self.mock_conn.cursor.return_value.fetchone.return_value = (None,)  # no prior cursor
        self.mock_conn.cursor.return_value.rowcount = 1

    @patch("ingestors.etherscan_ingestor.time.sleep", lambda _x: None)
    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_paginates_until_short_page(self, mock_get):
        # Page 1: full PAGE_SIZE (1000). Page 2: 1 transfer. Expect 2 calls.
        from ingestors.etherscan_ingestor import PAGE_SIZE
        page1 = [SAMPLE_TRANSFER for _ in range(PAGE_SIZE)]
        page2 = [SAMPLE_TRANSFER]
        resp1, resp2 = MagicMock(), MagicMock()
        resp1.json.return_value = {"status": "1", "result": page1}
        resp2.json.return_value = {"status": "1", "result": page2}
        mock_get.side_effect = [resp1, resp2]

        inserted, err = self.ing.ingest_chain("kyberswap", RECEIVER, 1, "Ethereum")
        self.assertIsNone(err)
        self.assertEqual(mock_get.call_count, 2)

    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_stops_on_first_error(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = {"status": "0", "message": "Invalid API Key", "result": []}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp

        inserted, err = self.ing.ingest_chain("kyberswap", RECEIVER, 1, "Ethereum")
        self.assertEqual(inserted, 0)
        self.assertIsNotNone(err)
        self.assertIn("Invalid API Key", err)


class TestIngest(unittest.TestCase):
    def setUp(self):
        self.ing, self.mock_conn = _make_ingestor()
        self.mock_conn.cursor.return_value.fetchone.return_value = (None,)
        self.mock_conn.cursor.return_value.rowcount = 1

    @patch("ingestors.etherscan_ingestor.time.sleep", lambda _x: None)
    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_per_source_results(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = {"status": "1", "result": [SAMPLE_TRANSFER]}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp

        results = self.ing.ingest()
        self.assertEqual(len(results), 1)
        r = results[0]
        self.assertEqual(r["source"], "kyberswap")
        self.assertIsNone(r["error"])

    @patch("ingestors.etherscan_ingestor.time.sleep", lambda _x: None)
    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_error_propagates_to_result(self, mock_get):
        """When the API fails, the result must contain an error string so the
        orchestrator can write it to sync_status — the exact regression that
        let Arkham's 402 stay invisible for 7 months."""
        resp = MagicMock()
        resp.json.return_value = {"status": "0", "message": "trial expired", "result": []}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp

        results = self.ing.ingest()
        self.assertIsNotNone(results[0]["error"])
        self.assertIn("trial expired", results[0]["error"])


if __name__ == "__main__":
    unittest.main()
