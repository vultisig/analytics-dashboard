# tests/test_router_source_classifier.py
"""Unit tests for the router-source classifier."""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

if "psycopg2" not in sys.modules:
    _mock_pg = MagicMock()
    _mock_pg.OperationalError = type("OperationalError", (Exception,), {})
    _mock_pg.InterfaceError = type("InterfaceError", (Exception,), {})
    sys.modules["psycopg2"] = _mock_pg

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from ingestors import router_source_classifier as rsc  # noqa: E402


KYBER_V2_ROUTER = '0x6131b5fae19ea4f9d964eac0408e4408b66337b5'
ONEINCH_V5_ROUTER = '0x1111111254eeb25477b68fb85ed929f73a960582'
THORCHAIN_ROUTER = '0xd37bbe5744d730a1d98d8dc97c42f0ca46ad7146'
TREASURY_EOA = '0x890b0a47d192857d41f3e50fa6338dc47944b9fc'
LIFI_DIAMOND = rsc.config.LIFI_DIAMOND_ADDRESS

from datetime import datetime, timedelta  # noqa: E402
OLD_TS = datetime.utcnow() - timedelta(days=30)
FRESH_TS = datetime.utcnow()


def _mock_get(to_addr):
    """Build a requests.get mock that returns a tx with the given `to`."""
    resp = MagicMock()
    resp.json.return_value = {'jsonrpc': '2.0', 'id': 1, 'result': {'to': to_addr}}
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# fetch_tx_to
# ---------------------------------------------------------------------------

class TestFetchTxTo(unittest.TestCase):
    @patch.object(rsc.requests, 'get')
    def test_ok_returns_lowercased_to(self, mock_get):
        mock_get.return_value = _mock_get(KYBER_V2_ROUTER.upper())  # mixed case
        to, err = rsc.fetch_tx_to('key', 1, '0xabc')
        self.assertEqual(to, KYBER_V2_ROUTER)
        self.assertIsNone(err)

    @patch.object(rsc.requests, 'get')
    def test_contract_creation_returns_empty_string(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = {'jsonrpc': '2.0', 'id': 1, 'result': {'to': None}}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp
        to, err = rsc.fetch_tx_to('key', 1, '0xabc')
        self.assertEqual(to, '')
        self.assertIsNone(err)

    @patch.object(rsc.requests, 'get')
    def test_http_exception_returns_error(self, mock_get):
        import requests as req
        mock_get.side_effect = req.exceptions.Timeout('timeout')
        to, err = rsc.fetch_tx_to('key', 1, '0xabc')
        self.assertIsNone(to)
        self.assertIn('HTTP error', err)

    @patch.object(rsc.requests, 'get')
    def test_rpc_error_envelope_returns_error(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = {'jsonrpc': '2.0', 'id': 1, 'error': {'code': -32600, 'message': 'invalid'}}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp
        to, err = rsc.fetch_tx_to('key', 1, '0xabc')
        self.assertIsNone(to)
        self.assertIn('Etherscan error', err)

    @patch.object(rsc.requests, 'get')
    def test_missing_result_returns_error(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = {'status': '0', 'message': 'NOTOK'}
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp
        to, err = rsc.fetch_tx_to('key', 1, '0xabc')
        self.assertIsNone(to)
        self.assertIn('unexpected', err)


# ---------------------------------------------------------------------------
# classify_row
# ---------------------------------------------------------------------------

class TestClassifyRow(unittest.TestCase):
    @patch.object(rsc.requests, 'get')
    def test_router_tx_kept(self, mock_get):
        mock_get.return_value = _mock_get(KYBER_V2_ROUTER)
        final, _to, err = rsc.classify_row('key', 1, '0xabc', 'kyberswap')
        self.assertEqual(final, 'kyberswap')
        self.assertIsNone(err)

    @patch.object(rsc.requests, 'get')
    def test_non_router_demoted(self, mock_get):
        # Treasury EOA → tx.to is the EOA itself (not a router)
        mock_get.return_value = _mock_get(TREASURY_EOA)
        final, _to, err = rsc.classify_row('key', 1, '0xabc', 'kyberswap')
        self.assertEqual(final, 'other')
        self.assertIsNone(err)

    @patch.object(rsc.requests, 'get')
    def test_thorchain_router_demoted_for_kyber_source(self, mock_get):
        """The receiver also catches THORChain affiliate fees — they must
        not be tagged as kyberswap (already captured by the thorchain ingestor)."""
        mock_get.return_value = _mock_get(THORCHAIN_ROUTER)
        final, _to, err = rsc.classify_row('key', 1, '0xabc', 'kyberswap')
        self.assertEqual(final, 'other')
        self.assertIsNone(err)

    @patch.object(rsc.requests, 'get')
    def test_oneinch_router_kept_for_oneinch_source(self, mock_get):
        mock_get.return_value = _mock_get(ONEINCH_V5_ROUTER)
        final, _to, err = rsc.classify_row('key', 1, '0xabc', '1inch')
        self.assertEqual(final, '1inch')
        self.assertIsNone(err)

    @patch.object(rsc.requests, 'get')
    def test_cross_protocol_router_demoted(self, mock_get):
        """1inch router on a kyberswap-tagged row → demote (defensible to be
        strict; cross-protocol attribution would require calldata decode)."""
        mock_get.return_value = _mock_get(ONEINCH_V5_ROUTER)
        final, _to, err = rsc.classify_row('key', 1, '0xabc', 'kyberswap')
        self.assertEqual(final, 'other')

    @patch.object(rsc.requests, 'get')
    def test_api_error_returns_none(self, mock_get):
        """The Arkham silent-failure invariant: an API error must surface as
        an inability to classify, never a forced demote."""
        import requests as req
        mock_get.side_effect = req.exceptions.Timeout('timeout')
        final, _to, err = rsc.classify_row('key', 1, '0xabc', 'kyberswap')
        self.assertIsNone(final)
        self.assertIsNotNone(err)


# ---------------------------------------------------------------------------
# reclassify_all — orchestration
# ---------------------------------------------------------------------------

class TestReclassifyAll(unittest.TestCase):
    def _mock_db(self, rows):
        db = MagicMock()
        # Named server-side cursor for streaming
        streaming_cursor = MagicMock()
        streaming_cursor.__iter__.return_value = iter(rows)
        # Update cursors (no name arg)
        update_cursor = MagicMock()

        # db.cursor(name=...) → streaming; db.cursor() → update
        def cursor_factory(*args, **kwargs):
            if kwargs.get('name'):
                return streaming_cursor
            return update_cursor

        db.cursor.side_effect = cursor_factory
        return db, update_cursor

    @patch.object(rsc, 'time')  # silence sleep
    @patch.object(rsc.requests, 'get')
    def test_kept_and_demoted_counts(self, mock_get, _mock_time):
        rows = [
            (1, '0xkeep', 'Ethereum', 'kyberswap', OLD_TS),
            (2, '0xdrop', 'Ethereum', 'kyberswap', OLD_TS),
        ]
        # First call returns kyber router; second returns treasury EOA
        mock_get.side_effect = [_mock_get(KYBER_V2_ROUTER), _mock_get(TREASURY_EOA)]
        db, update_cursor = self._mock_db(rows)

        counts = rsc.reclassify_all('key', db)

        self.assertEqual(counts['kept'], 1)
        self.assertEqual(counts['demoted'], 1)
        self.assertEqual(counts['skipped_error'], 0)
        # Two UPDATE statements should have landed
        self.assertEqual(update_cursor.execute.call_count, 2)

    @patch.object(rsc, 'time')
    @patch.object(rsc.requests, 'get')
    def test_api_error_does_not_touch_row(self, mock_get, _mock_time):
        """Fail-open invariant: a row whose classification API call errors
        must NOT be UPDATEd. It stays unclassified and retries next cycle."""
        import requests as req
        rows = [(1, '0xerr', 'Ethereum', 'kyberswap', OLD_TS)]
        mock_get.side_effect = req.exceptions.ConnectionError('boom')
        db, update_cursor = self._mock_db(rows)

        counts = rsc.reclassify_all('key', db)

        self.assertEqual(counts['skipped_error'], 1)
        self.assertEqual(counts['kept'], 0)
        self.assertEqual(counts['demoted'], 0)
        update_cursor.execute.assert_not_called()

    @patch.object(rsc, 'time')
    @patch.object(rsc.requests, 'get')
    def test_unknown_chain_skipped_without_api_call(self, mock_get, _mock_time):
        rows = [(1, '0xabc', 'Solana', 'kyberswap', OLD_TS)]  # not in CHAIN_TO_ID
        db, update_cursor = self._mock_db(rows)
        counts = rsc.reclassify_all('key', db)
        self.assertEqual(counts['skipped_unknown_chain'], 1)
        mock_get.assert_not_called()
        update_cursor.execute.assert_not_called()


# ---------------------------------------------------------------------------
# reclassify_all — LiFi Diamond attribution
# ---------------------------------------------------------------------------

ONEINCH_SWAP = {
    'tool': '1inch',
    'in_amount_usd': 1234.5,
    'affiliate_fee_usd': 6.17,
    'in_asset': 'VULT-1',
    'out_asset': 'USDC-1',
    'in_amount': 2000.0,
}


class TestLifiDiamondAttribution(TestReclassifyAll):
    @patch.object(rsc, 'time')
    @patch.object(rsc.requests, 'get')
    def test_oneinch_tool_attributed_with_liquest_values(self, mock_get, _mock_time):
        rows = [(1, '0xfee', 'Ethereum', '1inch', OLD_TS)]
        mock_get.return_value = _mock_get(LIFI_DIAMOND)
        db, update_cursor = self._mock_db(rows)

        with patch.object(rsc, 'fetch_lifi_swap', return_value=dict(ONEINCH_SWAP)):
            counts = rsc.reclassify_all('key', db)

        self.assertEqual(counts['attributed'], 1)
        self.assertEqual(counts['demoted'], 0)
        params = update_cursor.execute.call_args[0][1]
        self.assertEqual(params[0], '1inch')      # protocol
        self.assertEqual(params[1], 1234.5)       # swap_volume_usd
        self.assertEqual(params[2], 6.17)         # actual_fee_usd
        self.assertEqual(params[3], 'VULT')       # token_in_symbol
        self.assertEqual(params[4], 'USDC')       # token_out_symbol

    @patch.object(rsc, 'time')
    @patch.object(rsc.requests, 'get')
    def test_unattributed_tool_demoted(self, mock_get, _mock_time):
        rows = [(1, '0xfee', 'Ethereum', '1inch', OLD_TS)]
        mock_get.return_value = _mock_get(LIFI_DIAMOND)
        db, update_cursor = self._mock_db(rows)

        swap = dict(ONEINCH_SWAP, tool='sushiswap')
        with patch.object(rsc, 'fetch_lifi_swap', return_value=swap):
            counts = rsc.reclassify_all('key', db)

        self.assertEqual(counts['demoted'], 1)
        self.assertEqual(counts['attributed'], 0)

    @patch.object(rsc, 'time')
    @patch.object(rsc.requests, 'get')
    def test_missing_swap_defers_fresh_row(self, mock_get, _mock_time):
        """The lifi sync runs in parallel — a fee row may land before its
        li.quest swap. Fresh rows must wait, not get demoted."""
        rows = [(1, '0xfee', 'Ethereum', '1inch', FRESH_TS)]
        mock_get.return_value = _mock_get(LIFI_DIAMOND)
        db, update_cursor = self._mock_db(rows)

        with patch.object(rsc, 'fetch_lifi_swap', return_value=None):
            counts = rsc.reclassify_all('key', db)

        self.assertEqual(counts['deferred'], 1)
        update_cursor.execute.assert_not_called()

    @patch.object(rsc, 'time')
    @patch.object(rsc.requests, 'get')
    def test_missing_swap_demotes_after_grace(self, mock_get, _mock_time):
        rows = [(1, '0xfee', 'Ethereum', '1inch', OLD_TS)]
        mock_get.return_value = _mock_get(LIFI_DIAMOND)
        db, update_cursor = self._mock_db(rows)

        with patch.object(rsc, 'fetch_lifi_swap', return_value=None):
            counts = rsc.reclassify_all('key', db)

        self.assertEqual(counts['demoted'], 1)


class TestAssetSymbol(unittest.TestCase):
    def test_strips_chain_suffix(self):
        self.assertEqual(rsc._asset_symbol('USDC-1'), 'USDC')

    def test_handles_empty(self):
        self.assertIsNone(rsc._asset_symbol(''))
        self.assertIsNone(rsc._asset_symbol(None))
        self.assertIsNone(rsc._asset_symbol('-1'))



if __name__ == '__main__':
    unittest.main()
