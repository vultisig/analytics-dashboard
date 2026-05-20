# tests/test_etherscan_ingestor.py
"""Unit tests for EtherscanIngestor."""
import os
import sys
import unittest
from decimal import Decimal
from unittest.mock import patch, MagicMock

# Mock psycopg2 before importing the ingestor (CI may not have it installed).
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


# Sample Etherscan V2 `tokentx` record: VULT fee landing on the KyberSwap
# fee receiver (the reference tx from the PR description).
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

# Same shape but outbound (FROM the receiver) — must be filtered out.
SAMPLE_OUTBOUND = {
    **SAMPLE_TRANSFER,
    "hash": "0xbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeef",
    "from": "0x8e247a480449c84a5fdd25974a8501f3efa4abb9",
    "to": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
}

RECEIVER = "0x8E247a480449c84a5fDD25974A8501f3EFa4ABb9"


def _make_ingestor():
    """Build an EtherscanIngestor with module-level config patched (the
    module reads ETHERSCAN_API_KEY / DATABASE_URL at import time so an
    os.environ.patch is too late)."""
    from ingestors import etherscan_ingestor as mod
    mod.ETHERSCAN_API_KEY = "test-key-12345"
    mod.DATABASE_URL = "postgresql://test"
    ing = mod.EtherscanIngestor(
        integrators=[("kyberswap", RECEIVER)],
        chains=[(1, "Ethereum")],
    )
    mock_conn = MagicMock()
    mock_conn.closed = False
    ing.db = mock_conn
    return ing, mock_conn


# ---------------------------------------------------------------------------
# _build_row — pure transform, no I/O
# ---------------------------------------------------------------------------

class TestBuildRow(unittest.TestCase):
    def test_inbound_transfer_builds_full_row(self):
        from ingestors.etherscan_ingestor import _build_row, INSERT_COLUMNS
        row = _build_row(SAMPLE_TRANSFER, "kyberswap", "Ethereum", RECEIVER)
        self.assertIsNotNone(row)
        self.assertEqual(len(row), len(INSERT_COLUMNS))
        named = dict(zip(INSERT_COLUMNS, row))
        self.assertEqual(named['tx_hash'], SAMPLE_TRANSFER['hash'])
        self.assertEqual(named['chain'], 'Ethereum')
        self.assertEqual(named['protocol'], 'kyberswap')
        self.assertEqual(named['fee_token_symbol'], 'VULT')
        self.assertEqual(named['fee_data_source'], 'etherscan')
        # actual_fee_usd is 0 as a placeholder — column is NOT NULL on the
        # existing schema; the enricher pass overwrites with the real value.
        self.assertEqual(named['actual_fee_usd'], 0)

    def test_outbound_transfer_filtered(self):
        from ingestors.etherscan_ingestor import _build_row
        self.assertIsNone(_build_row(SAMPLE_OUTBOUND, "kyberswap", "Ethereum", RECEIVER))

    def test_missing_hash_filtered(self):
        from ingestors.etherscan_ingestor import _build_row
        bad = {**SAMPLE_TRANSFER}
        del bad["hash"]
        self.assertIsNone(_build_row(bad, "kyberswap", "Ethereum", RECEIVER))

    def test_decimal_precision_preserved(self):
        """18-decimal tokens with large raw values must not lose precision."""
        from ingestors.etherscan_ingestor import _build_row, INSERT_COLUMNS
        row = _build_row(SAMPLE_TRANSFER, "kyberswap", "Ethereum", RECEIVER)
        named = dict(zip(INSERT_COLUMNS, row))
        # 149354384523928218 / 1e18 — full 18 fractional digits preserved.
        self.assertEqual(named['fee_amount_raw'], '0.149354384523928218')
        self.assertEqual(named['amount_in'], Decimal('0.149354384523928218'))

    def test_zero_decimals(self):
        """Tokens with 0 decimals (NFTs, some stablecoins) → raw int amount."""
        from ingestors.etherscan_ingestor import _build_row, INSERT_COLUMNS
        t = {**SAMPLE_TRANSFER, "tokenDecimal": "0", "value": "42"}
        row = _build_row(t, "kyberswap", "Ethereum", RECEIVER)
        named = dict(zip(INSERT_COLUMNS, row))
        self.assertEqual(named['amount_in'], Decimal('42'))


# ---------------------------------------------------------------------------
# fetch_chain_page — the silent-failure regression suite
# ---------------------------------------------------------------------------

class TestFetchChainPage(unittest.TestCase):
    def setUp(self):
        self.ing, _ = _make_ingestor()

    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_status_1_returns_transfers(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = {"status": "1", "result": [SAMPLE_TRANSFER]}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp
        transfers, err = self.ing.fetch_chain_page(1, RECEIVER, 0, 1)
        self.assertEqual(len(transfers), 1)
        self.assertIsNone(err)

    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_empty_chain_no_transactions_found(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = {"status": "0", "message": "No transactions found", "result": []}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp
        transfers, err = self.ing.fetch_chain_page(56, RECEIVER, 0, 1)
        self.assertEqual(transfers, [])
        self.assertIsNone(err)

    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_empty_chain_notok_variant(self, mock_get):
        """Empirical Etherscan V2 variant: status=0, message='NOTOK', result
        also 'NOTOK' (string). Observed live on BSC/Optimism/Base/Avalanche
        for the kyber fee receiver. Must NOT bubble as an error or the sync
        treats every empty chain as a failure (1.0.9 production regression)."""
        resp = MagicMock()
        resp.json.return_value = {"status": "0", "message": "NOTOK", "result": "NOTOK"}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp
        transfers, err = self.ing.fetch_chain_page(56, RECEIVER, 0, 1)
        self.assertEqual(transfers, [])
        self.assertIsNone(err)

    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_real_failure_propagates_error(self, mock_get):
        """Arkham 402 silent-failure regression: specific error messages
        (Invalid API Key, Max rate limit) must surface as errors, never as
        clean empty results."""
        resp = MagicMock()
        resp.json.return_value = {"status": "0", "message": "Invalid API Key", "result": "Invalid API Key"}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp
        transfers, err = self.ing.fetch_chain_page(1, RECEIVER, 0, 1)
        self.assertEqual(transfers, [])
        self.assertIsNotNone(err)
        self.assertIn("Invalid API Key", err)

    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_rate_limit_propagates_error(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = {"status": "0", "message": "Max rate limit reached", "result": "Max rate limit reached"}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp
        _, err = self.ing.fetch_chain_page(1, RECEIVER, 0, 1)
        self.assertIsNotNone(err)
        self.assertIn("Max rate limit", err)

    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_http_exception_returns_error(self, mock_get):
        import requests as req
        mock_get.side_effect = req.exceptions.Timeout("timed out")
        _, err = self.ing.fetch_chain_page(1, RECEIVER, 0, 1)
        self.assertIsNotNone(err)
        self.assertIn("HTTP error", err)


# ---------------------------------------------------------------------------
# ingest_chain / ingest — orchestration
# ---------------------------------------------------------------------------

class TestIngestChain(unittest.TestCase):
    def setUp(self):
        self.ing, self.mock_conn = _make_ingestor()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_conn.cursor.return_value
        self.mock_conn.cursor.return_value.fetchone.return_value = (None,)
        self.mock_conn.cursor.return_value.rowcount = 0  # set by tests as needed

    @patch("ingestors.etherscan_ingestor.execute_values")
    @patch("ingestors.etherscan_ingestor.time.sleep", lambda _x: None)
    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_paginates_until_short_page(self, mock_get, mock_execute_values):
        from ingestors.etherscan_ingestor import PAGE_SIZE
        page1 = [SAMPLE_TRANSFER] * PAGE_SIZE
        page2 = [SAMPLE_TRANSFER]
        r1, r2 = MagicMock(), MagicMock()
        r1.json.return_value = {"status": "1", "result": page1}
        r2.json.return_value = {"status": "1", "result": page2}
        mock_get.side_effect = [r1, r2]

        # cursor.rowcount represents inserts per page batch
        self.mock_conn.cursor.return_value.rowcount = PAGE_SIZE
        _, err = self.ing.ingest_chain("kyberswap", RECEIVER, 1, "Ethereum")
        self.assertIsNone(err)
        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(mock_execute_values.call_count, 2)

    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_stops_on_first_error(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = {"status": "0", "message": "Invalid API Key", "result": "NOTOK"}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp
        inserted, err = self.ing.ingest_chain("kyberswap", RECEIVER, 1, "Ethereum")
        self.assertEqual(inserted, 0)
        self.assertIn("Invalid API Key", err)


class TestIngest(unittest.TestCase):
    def setUp(self):
        self.ing, self.mock_conn = _make_ingestor()
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_conn.cursor.return_value
        self.mock_conn.cursor.return_value.fetchone.return_value = (None,)
        self.mock_conn.cursor.return_value.rowcount = 1

    @patch("ingestors.etherscan_ingestor.execute_values")
    @patch("ingestors.etherscan_ingestor.time.sleep", lambda _x: None)
    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_per_source_results(self, mock_get, _mock_ev):
        resp = MagicMock()
        resp.json.return_value = {"status": "1", "result": [SAMPLE_TRANSFER]}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp
        results = self.ing.ingest()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["source"], "kyberswap")
        self.assertIsNone(results[0]["error"])

    @patch("ingestors.etherscan_ingestor.time.sleep", lambda _x: None)
    @patch("ingestors.etherscan_ingestor.requests.get")
    def test_error_propagates_to_result(self, mock_get):
        """The exact regression that hid Arkham's 402: an API failure must
        leave a non-None error string on the per-source result so the
        orchestrator can write sync_status.last_error."""
        resp = MagicMock()
        resp.json.return_value = {"status": "0", "message": "trial expired", "result": "NOTOK"}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp
        results = self.ing.ingest()
        self.assertIsNotNone(results[0]["error"])
        self.assertIn("trial expired", results[0]["error"])


if __name__ == "__main__":
    unittest.main()
