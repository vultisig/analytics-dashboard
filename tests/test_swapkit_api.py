"""API wiring for the single SwapKit dashboard bucket.

Flask is not installed in unit tests — parse api_server.py like
tests/test_sql_injection.py.
"""
import ast
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

API_SERVER_PATH = Path(__file__).resolve().parent.parent / "vultisig-analytics" / "api_server.py"
_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

if "psycopg2" not in sys.modules:
    _mock_pg = MagicMock()
    _mock_pg.OperationalError = type("OperationalError", (Exception,), {})
    _mock_pg.InterfaceError = type("InterfaceError", (Exception,), {})
    sys.modules["psycopg2"] = _mock_pg

from config import config  # noqa: E402


def _handler_source(name: str) -> str:
    source = API_SERVER_PATH.read_text()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(source, node)
    raise AssertionError(f"handler {name} not found")


class TestSwapKitApiWiring(unittest.TestCase):
    def test_known_and_dex_lists_include_swapkit(self):
        source = API_SERVER_PATH.read_text()
        self.assertIn("DEX_REVENUE_PROVIDERS", source)
        self.assertIn("SWAPKIT_PROTOCOL", source)
        self.assertIn("KNOWN_PROVIDERS", source)
        self.assertIn("swapkit", config.DEX_REVENUE_PROVIDERS)
        self.assertIn("swapkit", config.SWAPKIT_PROTOCOL)

    def test_provider_aggregates_merge_same_name_rows(self):
        volume = _handler_source("get_swap_volume")
        count = _handler_source("get_swap_count")
        revenue = _handler_source("get_revenue")
        self.assertIn("merge_rows_by_key", volume)
        self.assertIn("merge_rows_by_key", count)
        self.assertIn("merge_rows_by_key", revenue)

    def test_merge_rows_by_key_sums_split_swapkit(self):
        source = API_SERVER_PATH.read_text()
        tree = ast.parse(source)
        wanted = {"safe_float", "safe_int", "merge_rows_by_key"}
        chunks = [
            ast.get_source_segment(source, node)
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name in wanted
        ]
        namespace = {}
        exec("\n\n".join(chunks), namespace)
        merge_rows_by_key = namespace["merge_rows_by_key"]

        rows = [
            {"source": "swapkit", "total_volume": 100.0, "swap_count": 2},
            {"source": "thorchain", "total_volume": 50.0, "swap_count": 1},
            {"source": "swapkit", "total_volume": 25.5, "swap_count": 3},
        ]
        merged = merge_rows_by_key(
            rows, "source", float_fields=("total_volume",), int_fields=("swap_count",)
        )
        by_source = {row["source"]: row for row in merged}
        self.assertEqual(len(merged), 2)
        self.assertEqual(by_source["swapkit"]["total_volume"], 125.5)
        self.assertEqual(by_source["swapkit"]["swap_count"], 5)
        self.assertEqual(by_source["thorchain"]["total_volume"], 50.0)

    def test_payout_rows_excluded_from_volume(self):
        source = API_SERVER_PATH.read_text()
        self.assertIn("DEX_VOLUME_PAYOUT_EXCLUDE", source)
        self.assertIn("AND NOT (protocol = %s AND LOWER(from_address) IN %s)", source)
        volume = _handler_source("get_swap_volume_by_provider")
        self.assertIn("UNION ALL", volume)
        self.assertIn("LOWER(from_address) IN %s", volume)

    def test_no_second_fee_receiver_and_no_v7(self):
        receiver_names = [name for name, _addr in config.ARKHAM_FEE_RECEIVERS]
        self.assertNotIn("swapkit", receiver_names)
        self.assertNotIn("v7", config.VULTISIG_AFFILIATES)
        self.assertEqual(config.VULTISIG_AFFILIATES, ["va", "vi", "v0"])
