# tests/test_arkham_ingestor.py
"""
Unit tests for ingestors/arkham_ingestor.py (ArkhamIngestor).

Fixtures are built from a real Arkham /transfers API response.
Three representative transfers cover: no entity / zero USD, LiFi entity on BSC,
and native-ETH transfer on Base with null tokenAddress.

Run from the repo root with:
    python3 -m unittest discover -s tests -p 'test_*.py' -v
"""
import copy
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock, call

# Mock psycopg2 before any ingestor import (it may not be installed in CI).
if "psycopg2" not in sys.modules:
    _mock_pg = MagicMock()
    _mock_pg.OperationalError = type("OperationalError", (Exception,), {})
    _mock_pg.InterfaceError = type("InterfaceError", (Exception,), {})
    sys.modules["psycopg2"] = _mock_pg

# Make `ingestors` importable whether the test runs from the repo root
# (CI) or from vultisig-analytics/.
_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)
 

# Transfer 0: Base chain, no arkhamEntity, historicalUSD = 0 (DCC token)
TRANSFER_NO_ENTITY = {
    "id": "0x1ba1eaec67eab2e1fcb32dfeb5aa97f1e3de1df42df59a8586630ea9cb9a30b5_488",
    "transactionHash": "0x1ba1eaec67eab2e1fcb32dfeb5aa97f1e3de1df42df59a8586630ea9cb9a30b5",
    "fromAddress": {
        "address": "0x98c4E3Bb85EB0782508b72CCd08B5268b12c695a",
        "chain": "base",
        "isUserAddress": False,
        "contract": False,
    },
    "fromIsContract": False,
    "toAddress": {
        "address": "0xA4a4f610e89488EB4ECc6c63069f241a54485269",
        "chain": "base",
        "isUserAddress": False,
        "contract": False,
    },
    "toIsContract": False,
    "tokenAddress": "0x0852b1A0C9D7838a3B089ECade0B18df576b3AdB",
    "type": "",
    "blockTimestamp": "2026-04-13T07:47:29Z",
    "blockNumber": 44638551,
    "blockHash": "0x494f3eca022920182db4f83e0d6899a314e71d5b046e98fff06494fbf0847b3e",
    "tokenName": "Domain Chain Coin",
    "tokenSymbol": "DCC",
    "tokenDecimals": 18,
    "unitValue": 17.93423125,
    "tokenId": None,
    "historicalUSD": 0,
    "chain": "base",
}

# Transfer 1: BSC chain, LiFi arkhamEntity, small ETH fee
TRANSFER_LIFI_BSC = {
    "id": "0xce1e15a364fc7af65b4fd1f979909513dbe07db55b6402e7ebcc300ca7473830_90",
    "transactionHash": "0xce1e15a364fc7af65b4fd1f979909513dbe07db55b6402e7ebcc300ca7473830",
    "fromAddress": {
        "address": "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
        "chain": "bsc",
        "arkhamEntity": {
            "name": "LiFi",
            "note": "",
            "id": "li-fi",
            "type": "dex-aggregator",
            "service": None,
            "addresses": None,
            "website": "https://li.fi",
        },
        "arkhamLabel": {
            "name": "LiFiDiamond",
            "address": "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
            "chainType": "evm",
        },
        "isUserAddress": False,
        "contract": True,
    },
    "fromIsContract": True,
    "toAddress": {
        "address": "0xA4a4f610e89488EB4ECc6c63069f241a54485269",
        "chain": "bsc",
        "isUserAddress": False,
        "contract": False,
    },
    "toIsContract": False,
    "tokenAddress": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",
    "type": "",
    "blockTimestamp": "2026-04-12T19:47:47Z",
    "blockNumber": 92161629,
    "blockHash": "0xe33919e4d45669bd4be0678fab0ac53d472ba775f26afe9043513ddbbaea96bd",
    "tokenName": "Ethereum Token",
    "tokenSymbol": "ETH",
    "tokenDecimals": 18,
    "unitValue": 0.000004025,
    "tokenId": "binance-peg-weth",
    "historicalUSD": 0.00887637275,
    "chain": "bsc",
}

# Transfer 2: Base chain, native ETH (tokenAddress = null), FeeForwarder label
TRANSFER_NATIVE_ETH = {
    "id": "call_0xb1cc63cb48c30ea0940764777a756a0afdaa55df5133e5e9fedd36d2884cb03b_0,0,1",
    "transactionHash": "0xb1cc63cb48c30ea0940764777a756a0afdaa55df5133e5e9fedd36d2884cb03b",
    "fromAddress": {
        "address": "0xC18D9E84b8687A2645447A61e52c455Dac1675e1",
        "chain": "base",
        "arkhamLabel": {
            "name": "FeeForwarder",
            "address": "0xC18D9E84b8687A2645447A61e52c455Dac1675e1",
            "chainType": "evm",
        },
        "isUserAddress": False,
        "contract": True,
    },
    "fromIsContract": True,
    "toAddress": {
        "address": "0xA4a4f610e89488EB4ECc6c63069f241a54485269",
        "chain": "base",
        "isUserAddress": False,
        "contract": False,
    },
    "toIsContract": False,
    "tokenAddress": None,
    "type": "",
    "blockTimestamp": "2026-04-12T07:10:39Z",
    "blockNumber": 44594246,
    "blockHash": "0x761ddd65cf04e7d6076ec0e8bd1b207ad0cbdd2e6f18bb866fa3b855e80794ba",
    "tokenName": "Ethereum",
    "tokenSymbol": "ETH",
    "tokenDecimals": 18,
    "unitValue": 0.0000039,
    "tokenId": "ethereum",
    "historicalUSD": 0.008634482999999998,
    "chain": "base",
}


def _clone(transfer):
    """Return an independent deep-copy of a fixture transfer."""
    return copy.deepcopy(transfer)


# ---------------------------------------------------------------------------
# Helpers to construct an ArkhamIngestor without real env vars / DB
# ---------------------------------------------------------------------------

def _make_ingestor():
    """
    Instantiate ArkhamIngestor with env vars patched and DB mocked.
    Returns (ingestor, mock_db_connection).
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.closed = False
    mock_cursor.fetchone.return_value = (None,)

    with patch.dict(os.environ, {
        "ARKHAM_API_KEY": "test-key-12345",
        "DATABASE_URL": "postgresql://test:test@localhost/test",
    }):
        # Reload module-level constants so they pick up the patched env
        import importlib
        import ingestors.arkham_ingestor as mod
        mod.ARKHAM_API_KEY = "test-key-12345"
        mod.DATABASE_URL = "postgresql://test:test@localhost/test"

        ingestor = mod.ArkhamIngestor()
        # Inject mock connection so _get_connection doesn't hit a real DB
        ingestor.db = mock_conn
        ingestor.protocol_identifier = MagicMock()
        ingestor.protocol_identifier.identify_protocol.return_value = "other"

    return ingestor, mock_conn


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

class TestInit(unittest.TestCase):
    def test_raises_without_arkham_api_key(self):
        import ingestors.arkham_ingestor as mod
        orig_key, orig_db = mod.ARKHAM_API_KEY, mod.DATABASE_URL
        try:
            mod.ARKHAM_API_KEY = None
            mod.DATABASE_URL = "postgresql://x"
            with self.assertRaises(ValueError) as ctx:
                mod.ArkhamIngestor()
            self.assertIn("ARKHAM_API_KEY", str(ctx.exception))
        finally:
            mod.ARKHAM_API_KEY, mod.DATABASE_URL = orig_key, orig_db

    def test_raises_without_database_url(self):
        import ingestors.arkham_ingestor as mod
        orig_key, orig_db = mod.ARKHAM_API_KEY, mod.DATABASE_URL
        try:
            mod.ARKHAM_API_KEY = "some-key"
            mod.DATABASE_URL = None
            with self.assertRaises(ValueError) as ctx:
                mod.ArkhamIngestor()
            self.assertIn("DATABASE_URL", str(ctx.exception))
        finally:
            mod.ARKHAM_API_KEY, mod.DATABASE_URL = orig_key, orig_db


# ---------------------------------------------------------------------------
# normalize_chain
# ---------------------------------------------------------------------------

class TestNormalizeChain(unittest.TestCase):
    def setUp(self):
        self.ing, _ = _make_ingestor()

    def test_known_chains(self):
        cases = {
            "ethereum": "Ethereum",
            "bsc": "BSC",
            "binance-smart-chain": "BSC",
            "polygon": "Polygon",
            "polygon-pos": "Polygon",
            "arbitrum_one": "Arbitrum",
            "arbitrum-one": "Arbitrum",
            "optimism": "Optimism",
            "base": "Base",
            "avalanche": "Avalanche",
            "blast": "Blast",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(self.ing.normalize_chain(raw), expected)

    def test_case_insensitive(self):
        self.assertEqual(self.ing.normalize_chain("ETHEREUM"), "Ethereum")
        self.assertEqual(self.ing.normalize_chain("BSC"), "BSC")
        self.assertEqual(self.ing.normalize_chain("Base"), "Base")

    def test_unknown_chain_capitalized(self):
        self.assertEqual(self.ing.normalize_chain("solana"), "Solana")
        self.assertEqual(self.ing.normalize_chain("fantom"), "Fantom")

    def test_empty_string_returns_unknown(self):
        self.assertEqual(self.ing.normalize_chain(""), "Unknown")

    def test_none_returns_unknown(self):
        self.assertEqual(self.ing.normalize_chain(None), "Unknown")


# ---------------------------------------------------------------------------
# extract_address
# ---------------------------------------------------------------------------

class TestExtractAddress(unittest.TestCase):
    def setUp(self):
        self.ing, _ = _make_ingestor()

    def test_dict_with_address_key(self):
        self.assertEqual(
            self.ing.extract_address({"address": "0xABC", "chain": "base"}),
            "0xABC",
        )

    def test_plain_string(self):
        self.assertEqual(self.ing.extract_address("0xDEF"), "0xDEF")

    def test_none_returns_empty(self):
        self.assertEqual(self.ing.extract_address(None), "")

    def test_empty_dict_returns_empty(self):
        self.assertEqual(self.ing.extract_address({}), "")

    def test_dict_missing_address_key_returns_empty(self):
        self.assertEqual(self.ing.extract_address({"chain": "base"}), "")

    def test_real_fixture_from_address(self):
        addr = self.ing.extract_address(TRANSFER_LIFI_BSC["fromAddress"])
        self.assertEqual(addr, "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE")

    def test_real_fixture_to_address(self):
        addr = self.ing.extract_address(TRANSFER_NO_ENTITY["toAddress"])
        self.assertEqual(addr, "0xA4a4f610e89488EB4ECc6c63069f241a54485269")


# ---------------------------------------------------------------------------
# identify_protocol_from_arkham_entity
# ---------------------------------------------------------------------------

class TestIdentifyProtocolFromArkhamEntity(unittest.TestCase):
    def setUp(self):
        self.ing, _ = _make_ingestor()

    def test_lifi_entity_not_matched(self):
        """LiFi is not in the entity mapping — should return None."""
        result = self.ing.identify_protocol_from_arkham_entity(
            TRANSFER_LIFI_BSC["fromAddress"]
        )
        self.assertIsNone(result)

    def test_1inch_entity(self):
        addr_obj = {
            "address": "0x111",
            "arkhamEntity": {"id": "1inch-network", "name": "1inch"},
        }
        self.assertEqual(
            self.ing.identify_protocol_from_arkham_entity(addr_obj), "1inch"
        )

    def test_paraswap_entity(self):
        addr_obj = {
            "address": "0x222",
            "arkhamEntity": {"id": "paraswap", "name": "ParaSwap"},
        }
        self.assertEqual(
            self.ing.identify_protocol_from_arkham_entity(addr_obj), "paraswap"
        )

    def test_cowswap_entity(self):
        addr_obj = {
            "address": "0x333",
            "arkhamEntity": {"id": "cow-protocol", "name": "CowSwap"},
        }
        self.assertEqual(
            self.ing.identify_protocol_from_arkham_entity(addr_obj), "cowswap"
        )

    def test_matcha_0x_entity(self):
        addr_obj = {
            "address": "0x444",
            "arkhamEntity": {"id": "matcha-xyz", "name": "0x Exchange"},
        }
        self.assertEqual(
            self.ing.identify_protocol_from_arkham_entity(addr_obj), "matcha"
        )

    def test_no_entity_returns_none(self):
        result = self.ing.identify_protocol_from_arkham_entity(
            TRANSFER_NO_ENTITY["fromAddress"]
        )
        self.assertIsNone(result)

    def test_none_input_returns_none(self):
        self.assertIsNone(self.ing.identify_protocol_from_arkham_entity(None))

    def test_string_input_returns_none(self):
        self.assertIsNone(
            self.ing.identify_protocol_from_arkham_entity("0xABC")
        )

    def test_empty_entity_returns_none(self):
        addr_obj = {"address": "0x555", "arkhamEntity": None}
        self.assertIsNone(
            self.ing.identify_protocol_from_arkham_entity(addr_obj)
        )

    def test_entity_without_id_or_name(self):
        addr_obj = {"address": "0x666", "arkhamEntity": {"type": "dex"}}
        self.assertIsNone(
            self.ing.identify_protocol_from_arkham_entity(addr_obj)
        )


# ---------------------------------------------------------------------------
# insert_transfer — verifies SQL params and branching logic
# ---------------------------------------------------------------------------

class TestInsertTransfer(unittest.TestCase):
    def setUp(self):
        self.ing, self.mock_conn = _make_ingestor()
        self.mock_cursor = self.mock_conn.cursor.return_value

    def _get_insert_params(self):
        """Extract the parameter tuple from the last cursor.execute call."""
        calls = self.mock_cursor.execute.call_args_list
        # The INSERT call is the most recent one
        for c in reversed(calls):
            sql = c[0][0]
            if "INSERT INTO dex_aggregator_revenue" in sql:
                return c[0][1]
        return None

    def test_basic_transfer_inserts_correctly(self):
        self.ing.insert_transfer(_clone(TRANSFER_LIFI_BSC))
        params = self._get_insert_params()
        self.assertIsNotNone(params)

        tx_hash, chain, protocol, timestamp = params[0], params[1], params[2], params[3]
        self.assertEqual(
            tx_hash,
            "0xce1e15a364fc7af65b4fd1f979909513dbe07db55b6402e7ebcc300ca7473830",
        )
        self.assertEqual(chain, "BSC")
        self.assertIsInstance(timestamp, datetime)

    def test_lifi_entity_falls_through_to_protocol_identifier(self):
        """LiFi isn't in the entity map, so protocol_identifier is called."""
        self.ing.protocol_identifier.identify_protocol.return_value = "lifi"
        self.ing.insert_transfer(_clone(TRANSFER_LIFI_BSC))
        self.ing.protocol_identifier.identify_protocol.assert_called_once()
        params = self._get_insert_params()
        self.assertEqual(params[2], "lifi")  # protocol

    def test_1inch_entity_skips_protocol_identifier(self):
        """When arkhamEntity matches, protocol_identifier is NOT called."""
        transfer = _clone(TRANSFER_LIFI_BSC)
        transfer["fromAddress"]["arkhamEntity"]["id"] = "1inch-router"
        transfer["fromAddress"]["arkhamEntity"]["name"] = "1inch"
        self.ing.insert_transfer(transfer)
        self.ing.protocol_identifier.identify_protocol.assert_not_called()
        params = self._get_insert_params()
        self.assertEqual(params[2], "1inch")

    def test_historical_usd_as_fee(self):
        self.ing.insert_transfer(_clone(TRANSFER_LIFI_BSC))
        params = self._get_insert_params()
        actual_fee_usd = params[4]
        self.assertAlmostEqual(actual_fee_usd, 0.00887637275)

    def test_zero_historical_usd(self):
        self.ing.insert_transfer(_clone(TRANSFER_NO_ENTITY))
        params = self._get_insert_params()
        self.assertEqual(params[4], 0.0)  # actual_fee_usd

    def test_native_token_handling(self):
        """When tokenAddress is null, token_in should be set to NATIVE/ETH."""
        self.ing.insert_transfer(_clone(TRANSFER_NATIVE_ETH))
        params = self._get_insert_params()
        token_in_symbol = params[9]   # token_in_symbol
        token_in_address = params[10]  # token_in_address
        self.assertEqual(token_in_address, "NATIVE")
        self.assertEqual(token_in_symbol, "ETH")

    def test_erc20_token_does_not_set_native(self):
        """When tokenAddress is set, token_in_address should NOT be NATIVE."""
        self.ing.insert_transfer(_clone(TRANSFER_LIFI_BSC))
        params = self._get_insert_params()
        token_in_address = params[10]
        self.assertIsNone(token_in_address)  # not NATIVE

    def test_timestamp_parsed_from_iso(self):
        self.ing.insert_transfer(_clone(TRANSFER_LIFI_BSC))
        params = self._get_insert_params()
        ts = params[3]
        self.assertIsInstance(ts, datetime)
        self.assertEqual(ts.year, 2026)
        self.assertEqual(ts.month, 4)
        self.assertEqual(ts.day, 12)

    def test_missing_timestamp_uses_now(self):
        transfer = _clone(TRANSFER_NO_ENTITY)
        transfer["blockTimestamp"] = None
        before = datetime.now(timezone.utc)
        self.ing.insert_transfer(transfer)
        after = datetime.now(timezone.utc)
        params = self._get_insert_params()
        ts = params[3]
        # The fallback must be timezone-aware UTC, not naive local time
        self.assertEqual(ts.utcoffset(), timedelta(0))
        self.assertGreaterEqual(ts, before)
        self.assertLessEqual(ts, after)

    def test_missing_tx_hash_skips_insert(self):
        transfer = _clone(TRANSFER_NO_ENTITY)
        del transfer["transactionHash"]
        self.ing.insert_transfer(transfer)
        # No INSERT should have been executed
        for c in self.mock_cursor.execute.call_args_list:
            self.assertNotIn("INSERT", c[0][0])

    def test_block_number_preserved(self):
        self.ing.insert_transfer(_clone(TRANSFER_NATIVE_ETH))
        params = self._get_insert_params()
        block_number = params[15]
        self.assertEqual(block_number, 44594246)

    def test_from_and_to_addresses_extracted(self):
        self.ing.insert_transfer(_clone(TRANSFER_NATIVE_ETH))
        params = self._get_insert_params()
        from_addr = params[16]
        to_addr = params[17]
        self.assertEqual(from_addr, "0xC18D9E84b8687A2645447A61e52c455Dac1675e1")
        self.assertEqual(to_addr, "0xA4a4f610e89488EB4ECc6c63069f241a54485269")

    def test_fee_data_source_is_arkham(self):
        self.ing.insert_transfer(_clone(TRANSFER_LIFI_BSC))
        params = self._get_insert_params()
        fee_data_source = params[18]
        self.assertEqual(fee_data_source, "arkham")

    def test_fee_token_fields(self):
        self.ing.insert_transfer(_clone(TRANSFER_LIFI_BSC))
        params = self._get_insert_params()
        fee_token_symbol = params[5]
        fee_token_address = params[6]
        fee_amount_raw = params[7]
        self.assertEqual(fee_token_symbol, "ETH")
        self.assertEqual(fee_token_address, "0x2170Ed0880ac9A755fd29B2688956BD959F933F8")
        self.assertEqual(fee_amount_raw, "4.025e-06")

    def test_amount_in_calculated_when_unit_value_and_usd_present(self):
        self.ing.insert_transfer(_clone(TRANSFER_LIFI_BSC))
        params = self._get_insert_params()
        amount_in = params[13]
        self.assertAlmostEqual(amount_in, 0.000004025)

    def test_amount_in_none_when_zero_usd(self):
        """If historicalUSD is 0, amount_in stays None."""
        self.ing.insert_transfer(_clone(TRANSFER_NO_ENTITY))
        params = self._get_insert_params()
        amount_in = params[13]
        self.assertIsNone(amount_in)

    def test_exception_triggers_rollback(self):
        self.mock_cursor.execute.side_effect = Exception("db error")
        self.ing.insert_transfer(_clone(TRANSFER_LIFI_BSC))
        self.mock_conn.rollback.assert_called_once()


# ---------------------------------------------------------------------------
# fetch_all_transfers
# ---------------------------------------------------------------------------

class TestFetchAllTransfers(unittest.TestCase):
    def setUp(self):
        self.ing, self.mock_conn = _make_ingestor()
        self.mock_cursor = self.mock_conn.cursor.return_value

    @patch("ingestors.arkham_ingestor.requests.get")
    def test_single_page_returns_transfers(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "transfers": [_clone(TRANSFER_LIFI_BSC), _clone(TRANSFER_NATIVE_ETH)]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = self.ing.fetch_all_transfers("1inch", "0xA4a4f610e89488EB4ECc6c63069f241a54485269")
        self.assertEqual(len(result), 2)
        self.assertEqual(
            result[0]["transactionHash"],
            "0xce1e15a364fc7af65b4fd1f979909513dbe07db55b6402e7ebcc300ca7473830",
        )

    @patch("ingestors.arkham_ingestor.requests.get")
    def test_empty_response_returns_empty(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"transfers": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = self.ing.fetch_all_transfers("1inch", "0xA4a4f610e89488EB4ECc6c63069f241a54485269")
        self.assertEqual(result, [])

    @patch("ingestors.arkham_ingestor.requests.get")
    def test_api_key_sent_in_headers(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"transfers": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        self.ing.fetch_all_transfers("1inch", "0xA4a4f610e89488EB4ECc6c63069f241a54485269")
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs["headers"]["API-Key"], "test-key-12345")

    @patch("ingestors.arkham_ingestor.requests.get")
    def test_pagination_fetches_multiple_pages(self, mock_get):
        # Page 1: 1000 transfers (triggers next page fetch)
        page1 = [_clone(TRANSFER_LIFI_BSC) for _ in range(1000)]
        # Page 2: fewer than 1000 (signals end)
        page2 = [_clone(TRANSFER_NATIVE_ETH)]

        mock_resp1 = MagicMock()
        mock_resp1.json.return_value = {"transfers": page1}
        mock_resp1.raise_for_status = MagicMock()

        mock_resp2 = MagicMock()
        mock_resp2.json.return_value = {"transfers": page2}
        mock_resp2.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_resp1, mock_resp2]

        result = self.ing.fetch_all_transfers("1inch", "0xA4a4f610e89488EB4ECc6c63069f241a54485269")
        self.assertEqual(len(result), 1001)
        self.assertEqual(mock_get.call_count, 2)

    @patch("ingestors.arkham_ingestor.requests.get")
    def test_request_exception_returns_partial(self, mock_get):
        import requests as req

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"transfers": [_clone(TRANSFER_NO_ENTITY)]}
        mock_resp.raise_for_status = MagicMock()

        mock_get.side_effect = [
            mock_resp,
            req.exceptions.RequestException("timeout"),
        ]
        # First page has 1000 items to trigger second fetch
        mock_resp.json.return_value = {
            "transfers": [_clone(TRANSFER_NO_ENTITY)] * 1000
        }

        result = self.ing.fetch_all_transfers("1inch", "0xA4a4f610e89488EB4ECc6c63069f241a54485269")
        # Should have page 1 results, page 2 failed
        self.assertEqual(len(result), 1000)

    @patch("ingestors.arkham_ingestor.requests.get")
    def test_stops_when_reaching_existing_timestamps(self, mock_get):
        """When latest_timestamp is set, stops at transfers already in DB."""
        from datetime import timezone

        self.mock_cursor.fetchone.return_value = (
            datetime(2026, 4, 12, 20, 0, 0, tzinfo=timezone.utc),
        )

        # Transfer with blockTimestamp before latest_timestamp
        old_transfer = _clone(TRANSFER_LIFI_BSC)
        old_transfer["blockTimestamp"] = "2026-04-12T19:47:47Z"

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"transfers": [old_transfer]}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = self.ing.fetch_all_transfers("1inch", "0xA4a4f610e89488EB4ECc6c63069f241a54485269")
        # The old transfer should trigger early return
        self.assertEqual(len(result), 0)


# ---------------------------------------------------------------------------
# ingest — end-to-end orchestration
# ---------------------------------------------------------------------------

class TestIngest(unittest.TestCase):
    def setUp(self):
        self.ing, self.mock_conn = _make_ingestor()
        # Scope orchestration tests to a single integrator so commit/rollback
        # counts are deterministic. Multi-source orchestration runs against
        # the real DEFAULT_INTEGRATORS list in production.
        self.ing.integrators = [
            ("1inch", "0xA4a4f610e89488EB4ECc6c63069f241a54485269"),
        ]

    def test_ingest_commits_and_closes(self):
        with patch.object(
            self.ing, "fetch_all_transfers",
            return_value=[_clone(TRANSFER_LIFI_BSC)],
        ):
            self.ing.ingest()
        self.mock_conn.commit.assert_called()
        self.mock_conn.close.assert_called_once()

    def test_ingest_with_no_transfers_still_closes(self):
        with patch.object(self.ing, "fetch_all_transfers", return_value=[]):
            self.ing.ingest()
        self.mock_conn.close.assert_called_once()

    def test_ingest_with_error_rolls_back_and_closes(self):
        with patch.object(
            self.ing, "fetch_all_transfers", side_effect=Exception("boom")
        ):
            self.ing.ingest()
        self.mock_conn.rollback.assert_called_once()
        self.mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# _get_connection
# ---------------------------------------------------------------------------

class TestGetConnection(unittest.TestCase):
    def setUp(self):
        self.ing, self.mock_conn = _make_ingestor()

    def test_reuses_existing_live_connection(self):
        conn = self.ing._get_connection()
        self.assertIs(conn, self.mock_conn)

    @patch("ingestors.arkham_ingestor.psycopg2.connect")
    def test_reconnects_on_closed_connection(self, mock_connect):
        self.mock_conn.closed = True
        new_conn = MagicMock()
        mock_connect.return_value = new_conn

        conn = self.ing._get_connection()
        self.assertIs(conn, new_conn)
        mock_connect.assert_called_once()

    @patch("ingestors.arkham_ingestor.psycopg2.connect")
    def test_reconnects_on_operational_error(self, mock_connect):
        import psycopg2

        self.mock_conn.cursor.return_value.execute.side_effect = (
            psycopg2.OperationalError("gone")
        )
        new_conn = MagicMock()
        mock_connect.return_value = new_conn

        conn = self.ing._get_connection()
        self.assertIs(conn, new_conn)


if __name__ == "__main__":
    unittest.main()
