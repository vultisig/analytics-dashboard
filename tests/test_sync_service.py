"""Regression tests for rolling Midgard reconciliation."""

import importlib.util
import os
import sys
from types import ModuleType
import unittest
from unittest.mock import Mock, patch

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
_MAIN = os.path.join(_VA, "main.py")


def _module(name, **attributes):
    module = ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


_stubs = {
    "schedule": _module("schedule"),
    "database": _module("database", __path__=[]),
    "database.connection": _module("database.connection", db_manager=Mock()),
    "config": _module("config", config=Mock()),
    "ingestors": _module("ingestors", __path__=[]),
    "ingestors.thorchain": _module(
        "ingestors.thorchain", THORChainIngestor=Mock
    ),
    "ingestors.mayachain": _module(
        "ingestors.mayachain", MayaChainIngestor=Mock
    ),
    "ingestors.lifi": _module("ingestors.lifi", LiFiIngestor=Mock),
    "ingestors.etherscan_ingestor": _module(
        "ingestors.etherscan_ingestor", EtherscanIngestor=Mock
    ),
    "ingestors.router_source_classifier": _module(
        "ingestors.router_source_classifier",
        reclassify_all=Mock(),
        sync_attributed_gap_rows=Mock(),
    ),
    "ingestors.vult_holders": _module(
        "ingestors.vult_holders", VultHoldersIngestor=Mock
    ),
    "enrichers": _module("enrichers", __path__=[]),
    "enrichers.enrich_arkham_volumes": _module(
        "enrichers.enrich_arkham_volumes", VolumeEnricher=Mock
    ),
}

with patch.dict(sys.modules, _stubs):
    spec = importlib.util.spec_from_file_location("sync_main_under_test", _MAIN)
    sync_main = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sync_main)


def _action(tx_hash, timestamp="2026-08-01T00:00:00Z"):
    return {
        "in": [{"txID": tx_hash}],
        "tx_hash": tx_hash,
        "timestamp": timestamp,
    }


class FakeMidgardIngestor:
    def __init__(self):
        self.pages = {
            None: {
                "actions": [_action("known"), _action("new")],
                "meta": {"nextPageToken": "page-2"},
            },
            "page-2": {
                "actions": [_action("late")],
                "meta": {},
            },
        }
        self.fetch_tokens = []
        self.parsed_hashes = []

    def fetch_data(self, next_page_token=None):
        self.fetch_tokens.append(next_page_token)
        return self.pages[next_page_token]

    def parse_swap(self, action):
        action_inputs = action.get("in") or []
        if not action_inputs or not isinstance(action_inputs[0], dict):
            return None

        tx_hash = action_inputs[0].get("txID")
        if not tx_hash:
            return None

        self.parsed_hashes.append(tx_hash)
        return {
            "tx_hash": tx_hash,
            "timestamp": action["timestamp"],
        }


class FakeDatabase:
    def __init__(self):
        self.inserted_batches = []
        self.status_updates = []

    def get_sync_status(self, source_name):
        return {"source": source_name, "error_count": 0}

    def execute_query(self, query, params=None, fetch=False):
        if "tx_hash = ANY" in query:
            requested_hashes = set(params[1])
            return [{"tx_hash": "known"}] if "known" in requested_hashes else []
        if "refresh_materialized_views" in query:
            return []
        raise AssertionError(f"Unexpected query: {query}")

    def insert_swaps(self, records):
        self.inserted_batches.append(records)
        return len(records)

    def update_sync_status(self, source_name, **kwargs):
        self.status_updates.append((source_name, kwargs))


class SyncServiceReconciliationTests(unittest.TestCase):
    def run_sync(self, ingestor):
        database = FakeDatabase()
        service = sync_main.SyncService.__new__(sync_main.SyncService)
        service.ingestors = {"thorchain": ingestor}

        with patch.object(sync_main, "db_manager", database), patch.object(
            sync_main.time, "sleep"
        ):
            service.sync_source("thorchain")

        return database

    def test_midgard_rescan_skips_known_rows_and_reaches_late_action(self):
        ingestor = FakeMidgardIngestor()
        database = self.run_sync(ingestor)

        self.assertEqual(ingestor.fetch_tokens, [None, "page-2"])
        self.assertEqual(ingestor.parsed_hashes, ["new", "late"])
        self.assertEqual(
            [[row["tx_hash"] for row in batch] for batch in database.inserted_batches],
            [["new"], ["late"]],
        )

    def test_malformed_action_does_not_block_later_valid_action(self):
        ingestor = FakeMidgardIngestor()
        ingestor.pages = {
            None: {
                "actions": [{"in": [None]}, _action("late")],
                "meta": {},
            }
        }

        database = self.run_sync(ingestor)

        self.assertEqual(ingestor.parsed_hashes, ["late"])
        self.assertEqual(
            [[row["tx_hash"] for row in batch] for batch in database.inserted_batches],
            [["late"]],
        )


if __name__ == "__main__":
    unittest.main()
