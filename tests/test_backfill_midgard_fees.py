# tests/test_backfill_midgard_fees.py
"""Unit tests for jobs/backfill_midgard_fees.py (paging, cutoff, writes)."""
import importlib.util
import os
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

if "psycopg2" not in sys.modules:
    _mock_pg = MagicMock()
    _mock_pg.OperationalError = type("OperationalError", (Exception,), {})
    _mock_pg.InterfaceError = type("InterfaceError", (Exception,), {})
    sys.modules["psycopg2"] = _mock_pg

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

_spec = importlib.util.spec_from_file_location(
    "backfill_midgard_fees", os.path.join(_VA, "jobs", "backfill_midgard_fees.py")
)
bmf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bmf)


def _ts(day):
    return datetime(2026, 5, day, tzinfo=timezone.utc)


class _FakeIngestor:
    """Serves pages newest-first, like Midgard, and parses a stub action."""

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def fetch_data(self, next_page_token=''):
        self.calls.append(next_page_token)
        index = int(next_page_token or 0)
        actions = self.pages[index]
        meta = {'nextPageToken': str(index + 1)} if index + 1 < len(self.pages) else {}
        return {'actions': actions, 'meta': meta}

    def parse_swap(self, action):
        if action.get('skip'):
            return None
        return {'timestamp': action['ts'], 'affiliate_fee_usd': action['fee']}


class TestBackfill(unittest.TestCase):
    def test_pages_until_cutoff_and_sums_by_month(self):
        ingestor = _FakeIngestor([
            [{'ts': _ts(20), 'fee': 1.5}, {'skip': True}],
            [{'ts': _ts(10), 'fee': 2.0}, {'ts': _ts(1), 'fee': 99.0}],
            [{'ts': _ts(1), 'fee': 99.0}],  # never fetched: cutoff reached on page 2
        ])
        result = bmf.backfill('thorchain', _ts(5), dry_run=True, ingestor=ingestor)
        self.assertEqual(result['pages'], 2)
        self.assertEqual(result['fetched'], 4)
        self.assertEqual(result['written'], 0)
        self.assertEqual(result['fee_by_month'], {'2026-05': 3.5})
        self.assertEqual(ingestor.calls, ['', '1'])

    def test_stops_when_no_next_page_token(self):
        ingestor = _FakeIngestor([[{'ts': _ts(20), 'fee': 1.0}]])
        result = bmf.backfill('mayachain', _ts(1), dry_run=True, ingestor=ingestor)
        self.assertEqual(result['pages'], 1)
        self.assertEqual(ingestor.calls, [''])

    def test_writes_only_rows_inside_window(self):
        ingestor = _FakeIngestor([[{'ts': _ts(20), 'fee': 1.0}, {'ts': _ts(1), 'fee': 9.0}]])
        db = MagicMock()
        cursor = db.get_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        result = bmf.backfill('thorchain', _ts(5), dry_run=False, ingestor=ingestor, db=db)
        self.assertEqual(result['written'], 1)
        query, records = cursor.executemany.call_args[0]
        self.assertIn('ON CONFLICT (timestamp, tx_hash, source) DO UPDATE', query)
        self.assertEqual(len(records), 1)
        self.assertIsNone(records[0]['tool'])

    def test_upsert_refreshes_parser_owned_columns(self):
        for column in ('affiliate_fee_usd', 'in_amount_usd', 'volume_tier', 'swap_slip',
                       'metadata_complete', 'swap_status', 'is_streaming_swap'):
            self.assertIn(f'{column} = EXCLUDED.{column}', bmf.UPSERT_QUERY)
        self.assertNotIn('tool = EXCLUDED.tool', bmf.UPSERT_QUERY)


if __name__ == '__main__':
    unittest.main()
