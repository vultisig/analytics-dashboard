"""Cursor-persisting page walker for volume-only feeds (Chainflip, Near-Intents).

Feeds page newest-first. Until a feed is exhausted once, the walk resumes
from the persisted cursor (backfill). After that it walks from the head and
stops once DUPLICATE_PAGE_STOP_LIMIT pages in a row insert nothing.
State round-trips through `sync_status.next_page_token` as JSON.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

MAX_PAGES_PER_SYNC = 10
# Rescan depth = limit × page size. Enough for per-account feeds that see a
# handful of swaps a week; raise it before a feed outgrows that.
DUPLICATE_PAGE_STOP_LIMIT = 3

Rows = List[Dict[str, Any]]
Page = Tuple[Rows, Optional[str]]
FetchPage = Callable[[Optional[str]], Page]
Insert = Callable[[Rows], int]


@dataclass
class VolumeSyncState:
    cursors: Dict[str, str] = field(default_factory=dict)
    done: Dict[str, bool] = field(default_factory=dict)

    @classmethod
    def from_token(cls, token: Optional[str]) -> "VolumeSyncState":
        if not token:
            return cls()
        try:
            data = json.loads(token)
        except json.JSONDecodeError:
            # A corrupt token would otherwise wedge the feed on every run; restart the backfill.
            logger.warning("Discarding unreadable volume-sync state: %r", token)
            return cls()
        return cls(cursors=dict(data.get("cursors") or {}), done=dict(data.get("done") or {}))

    def to_token(self) -> str:
        return json.dumps({"cursors": self.cursors, "done": self.done}, sort_keys=True)

    def is_backfilled(self, key: str) -> bool:
        return self.done.get(key, False)


@dataclass
class FeedProgress:
    inserted: int = 0
    latest_ts: Optional[datetime] = None
    pages: int = 0

    def absorb(self, rows: Rows, inserted: int) -> None:
        self.pages += 1
        self.inserted += inserted
        for row in rows:
            ts = row.get("timestamp")
            if ts and (self.latest_ts is None or ts > self.latest_ts):
                self.latest_ts = ts


def walk_backfill(key: str, fetch_page: FetchPage, state: VolumeSyncState, insert: Insert) -> FeedProgress:
    progress = FeedProgress()
    cursor = state.cursors.get(key)
    for _ in range(MAX_PAGES_PER_SYNC):
        rows, next_cursor = fetch_page(cursor)
        progress.absorb(rows, insert(rows) if rows else 0)
        if next_cursor is None:
            state.done[key] = True
            state.cursors.pop(key, None)
            return progress
        cursor = next_cursor
    state.cursors[key] = cursor
    return progress


def walk_head(fetch_page: FetchPage, insert: Insert) -> FeedProgress:
    progress = FeedProgress()
    cursor = None
    quiet_pages = 0
    for _ in range(MAX_PAGES_PER_SYNC):
        rows, next_cursor = fetch_page(cursor)
        inserted = insert(rows) if rows else 0
        progress.absorb(rows, inserted)
        quiet_pages = 0 if inserted else quiet_pages + 1
        if next_cursor is None or quiet_pages >= DUPLICATE_PAGE_STOP_LIMIT:
            return progress
        cursor = next_cursor
    return progress


def ingest_feeds(
    source: str,
    feeds: Sequence[Tuple[str, FetchPage]],
    state_token: Optional[str],
    insert: Insert,
) -> Dict[str, Any]:
    """Walk every feed, persisting progress even when one of them fails."""
    state = VolumeSyncState.from_token(state_token)
    total = FeedProgress()
    errors: List[str] = []
    for key, fetch_page in feeds:
        try:
            if state.is_backfilled(key):
                progress = walk_head(fetch_page, insert)
            else:
                progress = walk_backfill(key, fetch_page, state, insert)
        except Exception as exc:  # one feed's outage must not hide the others' progress
            errors.append(f"{key}: {exc}")
            continue
        total.inserted += progress.inserted
        total.pages += progress.pages
        if progress.latest_ts and (total.latest_ts is None or progress.latest_ts > total.latest_ts):
            total.latest_ts = progress.latest_ts
    return {
        "source": source,
        "inserted": total.inserted,
        "pages": total.pages,
        "latest_ts": total.latest_ts,
        "error": "; ".join(errors) or None,
        "next_state": state.to_token(),
    }
