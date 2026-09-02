"""Cursor persistence and steady-state rescan for volume-only feeds."""
import json
import os
import sys
import unittest
from datetime import datetime, timezone

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from ingestors.paged_volume_sync import (  # noqa: E402
    DUPLICATE_PAGE_STOP_LIMIT,
    MAX_PAGES_PER_SYNC,
    VolumeSyncState,
    ingest_feeds,
)


def _row(key):
    return {"tx_hash": key, "timestamp": datetime(2026, 8, 1, tzinfo=timezone.utc)}


class RecordingFeed:
    """Newest-first pages keyed by cursor; `known` rows insert nothing."""

    def __init__(self, pages, known=()):
        self.pages = pages
        self.known = set(known)
        self.cursors_seen = []
        self.inserted = []

    def fetch(self, cursor):
        self.cursors_seen.append(cursor)
        return self.pages[cursor]

    def insert(self, rows):
        fresh = [r["tx_hash"] for r in rows if r["tx_hash"] not in self.known]
        self.inserted.extend(fresh)
        self.known.update(fresh)
        return len(fresh)


class TestBackfill(unittest.TestCase):
    def test_resumes_from_persisted_cursor_and_marks_done(self):
        feed = RecordingFeed({None: ([_row("a")], "p2"), "p2": ([_row("b")], "p3"), "p3": ([_row("c")], None)})
        token = VolumeSyncState(cursors={"cf": "p2"}).to_token()

        result = ingest_feeds("chainflip", [("cf", feed.fetch)], token, feed.insert)

        self.assertEqual(feed.cursors_seen, ["p2", "p3"])
        self.assertEqual(feed.inserted, ["b", "c"])
        self.assertEqual(json.loads(result["next_state"]), {"cursors": {}, "done": {"cf": True}})
        self.assertIsNone(result["error"])

    def test_page_budget_persists_cursor_for_next_run(self):
        pages = {None: ([_row("r0")], "c1")}
        for i in range(1, MAX_PAGES_PER_SYNC + 5):
            pages[f"c{i}"] = ([_row(f"r{i}")], f"c{i + 1}")
        feed = RecordingFeed(pages)

        result = ingest_feeds("chainflip", [("cf", feed.fetch)], None, feed.insert)

        self.assertEqual(result["pages"], MAX_PAGES_PER_SYNC)
        state = json.loads(result["next_state"])
        self.assertEqual(state["cursors"], {"cf": f"c{MAX_PAGES_PER_SYNC}"})
        self.assertEqual(state["done"], {})


class TestState(unittest.TestCase):
    def test_corrupt_token_restarts_backfill_instead_of_wedging(self):
        state = VolumeSyncState.from_token("{not json")
        self.assertEqual(state, VolumeSyncState())

    def test_token_round_trip(self):
        state = VolumeSyncState(cursors={"Web": "c3"}, done={"iOS": True})
        self.assertEqual(VolumeSyncState.from_token(state.to_token()), state)


class TestSteadyState(unittest.TestCase):
    def test_walks_head_and_stops_after_quiet_pages(self):
        pages = {None: ([_row("new")], "c1")}
        for i in range(1, 10):
            pages[f"c{i}"] = ([_row(f"old{i}")], f"c{i + 1}")
        feed = RecordingFeed(pages, known=[f"old{i}" for i in range(1, 10)])
        token = VolumeSyncState(done={"cf": True}).to_token()

        result = ingest_feeds("chainflip", [("cf", feed.fetch)], token, feed.insert)

        self.assertEqual(feed.inserted, ["new"])
        self.assertEqual(result["pages"], 1 + DUPLICATE_PAGE_STOP_LIMIT)
        self.assertEqual(json.loads(result["next_state"])["done"], {"cf": True})

    def test_new_rows_below_known_page_reset_the_quiet_counter(self):
        pages = {
            None: ([_row("k1")], "c1"),
            "c1": ([_row("k2")], "c2"),
            "c2": ([_row("late")], "c3"),
            "c3": ([_row("k3")], None),
        }
        feed = RecordingFeed(pages, known=["k1", "k2", "k3"])
        token = VolumeSyncState(done={"cf": True}).to_token()

        ingest_feeds("chainflip", [("cf", feed.fetch)], token, feed.insert)

        self.assertEqual(feed.inserted, ["late"])
        self.assertEqual(feed.cursors_seen, [None, "c1", "c2", "c3"])


class TestMultipleFeeds(unittest.TestCase):
    def test_one_failing_feed_keeps_the_others_progress(self):
        ok = RecordingFeed({None: ([_row("x")], None)})

        def broken(_cursor):
            raise RuntimeError("account not found")

        result = ingest_feeds("chainflip", [("iOS", broken), ("Web", ok.fetch)], None, ok.insert)

        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["error"], "iOS: account not found")
        self.assertEqual(json.loads(result["next_state"])["done"], {"Web": True})

    def test_latest_ts_is_newest_row_seen(self):
        older = dict(_row("o"), timestamp=datetime(2026, 7, 1, tzinfo=timezone.utc))
        newer = dict(_row("n"), timestamp=datetime(2026, 8, 9, tzinfo=timezone.utc))
        feed = RecordingFeed({None: ([older, newer], None)})

        result = ingest_feeds("near-intents", [("explorer", feed.fetch)], None, feed.insert)

        self.assertEqual(result["latest_ts"], newer["timestamp"])


if __name__ == "__main__":
    unittest.main()
