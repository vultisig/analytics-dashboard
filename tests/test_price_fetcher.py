# tests/test_price_fetcher.py
"""Unit tests for PriceFetcher's synchronous wrapper."""
import os
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from unittest.mock import MagicMock, patch

# Another test module may have mocked psycopg2 already (without the extras
# submodule), so register both unconditionally-but-idempotently.
sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())
sys.modules.setdefault("aiohttp", MagicMock())

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from utils.price_fetcher import PriceFetcher  # noqa: E402


class TestGetHistoricalPriceSync(unittest.TestCase):
    def _fetcher(self, cached=None, fetched=None):
        pf = PriceFetcher('postgresql://unused')
        pf._check_cache = MagicMock(return_value=cached)
        pf._save_to_cache = MagicMock()

        async def fake_fetch(token_id, price_date):
            return fetched

        pf._fetch_from_coingecko = fake_fetch
        return pf

    def test_cache_hit_skips_fetch(self):
        pf = self._fetcher(cached=42.0)
        self.assertEqual(pf.get_historical_price('vultisig', date(2026, 6, 11)), 42.0)

    def test_cache_miss_fetches_and_caches(self):
        pf = self._fetcher(cached=None, fetched=1.23)
        self.assertEqual(pf.get_historical_price('vultisig', date(2026, 6, 11)), 1.23)
        pf._save_to_cache.assert_called_once()

    def test_works_from_worker_thread(self):
        """The sync service calls this from ThreadPoolExecutor workers, where
        asyncio.get_event_loop() raises — the wrapper must not depend on a
        pre-existing event loop."""
        pf = self._fetcher(cached=None, fetched=1.23)
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(pf.get_historical_price, 'vultisig', date(2026, 6, 11)).result(timeout=10)
        self.assertEqual(result, 1.23)


if __name__ == '__main__':
    unittest.main()
