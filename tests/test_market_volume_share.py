"""Tests for Vultisig volume as a share of total crypto market volume."""

from datetime import date, datetime, timezone
import importlib.util
import os
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch


_ANALYTICS_DIR = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
_API_SERVER_PATH = os.path.join(_ANALYTICS_DIR, "api_server.py")


def _module(name, **attributes):
    module = ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


_db_stub = Mock()
_stubs = {
    "database": _module("database", __path__=[]),
    "database.connection": _module("database.connection", db_manager=_db_stub),
    "config": _module(
        "config",
        config=SimpleNamespace(ARKHAM_PROVIDERS=("1inch", "kyberswap")),
    ),
}

with patch.dict(sys.modules, _stubs):
    spec = importlib.util.spec_from_file_location(
        "market_volume_api_under_test", _API_SERVER_PATH
    )
    market_api = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(market_api)


def _utc(date_value):
    return datetime.strptime(date_value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _ms(date_value):
    return int(_utc(date_value).timestamp() * 1000)


# CoinGecko stamps a day's volume at the next UTC midnight; the newest point
# is a 0 placeholder until published. These points are Nov 14 and Nov 15.
COINGECKO_VOLUME = [
    [_ms("2023-11-15"), 1_000_000],
    [_ms("2023-11-16"), 2_000_000],
    [_ms("2023-11-17"), 0],
]


class FakeCoinGeckoResponse:
    def __init__(self, volume=COINGECKO_VOLUME):
        self.volume = volume

    def raise_for_status(self):
        return None

    def json(self):
        return {"market_cap_chart": {"market_cap": [], "volume": self.volume}}


def _execute_query(query, params=None, fetch=False):
    if "FROM swaps" in query:
        return [
            {"time_period": _utc("2023-11-14"), "source": "thorchain", "volume": 700},
            {"time_period": _utc("2023-11-14"), "source": "lifi", "volume": 200},
            {"time_period": _utc("2023-11-15"), "source": "thorchain", "volume": 1_500},
            {"time_period": _utc("2023-11-16"), "source": "thorchain", "volume": 9_999},
        ]
    if "FROM dex_aggregator_revenue" in query:
        return [
            {"time_period": _utc("2023-11-14"), "source": "kyberswap", "volume": 100},
            {"time_period": _utc("2023-11-15"), "source": "kyberswap", "volume": None},
        ]
    raise AssertionError(f"Unexpected query: {query}")


class MarketVolumeShareApiTests(unittest.TestCase):
    def setUp(self):
        market_api._global_market_cache.update(
            series=None, expires_at=0.0, is_stale=False,
        )
        self.database = Mock()
        self.database.execute_query.side_effect = _execute_query
        self.client = market_api.app.test_client()
        today = patch.object(market_api, "_utc_today", return_value=date(2023, 11, 17))
        today.start()
        self.addCleanup(today.stop)

    def _get(self, url, market_get=None):
        with patch.object(market_api, "db_manager", self.database), patch.object(
            market_api.requests,
            "get",
            side_effect=market_get or (lambda *_args, **_kwargs: FakeCoinGeckoResponse()),
        ) as get_market:
            response = self.client.get(url)
        return response, get_market

    def test_daily_share_divides_all_vultisig_volume_by_total_market(self):
        response, get_market = self._get(
            "/api/market-volume-share?r=custom&g=d&sd=2023-11-14&ed=2023-11-16"
        )

        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertEqual(
            result["series"],
            [
                {
                    "date": "2023-11-14",
                    "vultisigVolumeUsd": 1_000,
                    "marketVolumeUsd": 1_000_000,
                    "sharePercent": 0.1,
                },
                {
                    "date": "2023-11-15",
                    "vultisigVolumeUsd": 1_500,
                    "marketVolumeUsd": 2_000_000,
                    "sharePercent": 0.075,
                },
            ],
        )
        self.assertEqual(result["asOfDate"], "2023-11-15")
        self.assertEqual(result["source"], "CoinGecko")
        self.assertFalse(result["isStale"])
        self.assertIn("max-age=60", response.headers["Cache-Control"])
        self.assertIn("coingeicko", get_market.call_args.args[0])
        self.assertEqual(get_market.call_args.kwargs["params"]["days"], "max")

    def test_numerator_is_the_swap_volume_tab_series(self):
        with patch.object(
            market_api, "_query_volume_over_time", wraps=market_api._query_volume_over_time
        ) as volume_over_time:
            self._get("/api/market-volume-share?r=custom&g=d&sd=2023-11-14&ed=2023-11-16")

        volume_over_time.assert_called_once()
        self.assertEqual(volume_over_time.call_args.args[0], "day")

    def test_weekly_share_sums_before_dividing(self):
        response, _ = self._get(
            "/api/market-volume-share?r=custom&g=w&sd=2023-11-14&ed=2023-11-16"
        )

        result = response.get_json()
        self.assertEqual(result["effectiveGranularity"], "week")
        self.assertEqual(
            result["series"],
            [{
                "date": "2023-11-12",
                "vultisigVolumeUsd": 2_500,
                "marketVolumeUsd": 3_000_000,
                "sharePercent": 2_500 / 3_000_000 * 100,
            }],
        )

    def test_all_history_starts_at_first_vultisig_day(self):
        volume = [[_ms("2023-11-14"), 5_000_000], *COINGECKO_VOLUME]
        response, _ = self._get(
            "/api/market-volume-share?r=all&g=d",
            market_get=lambda *_args, **_kwargs: FakeCoinGeckoResponse(volume),
        )

        dates = [point["date"] for point in response.get_json()["series"]]
        self.assertEqual(dates, ["2023-11-14", "2023-11-15"])

    def test_custom_range_after_last_published_day_is_empty(self):
        response, _ = self._get(
            "/api/market-volume-share?r=custom&g=d&sd=2023-11-16&ed=2023-11-16"
        )

        result = response.get_json()
        self.assertEqual(result["series"], [])
        self.assertIsNone(result["asOfDate"])

    def test_drops_incomplete_current_utc_day(self):
        with patch.object(market_api, "_utc_today", return_value=date(2023, 11, 15)):
            response, _ = self._get("/api/market-volume-share?r=1d&g=d")

        result = response.get_json()
        self.assertEqual(result["asOfDate"], "2023-11-14")
        self.assertEqual([point["date"] for point in result["series"]], ["2023-11-14"])

    def test_fixed_ranges_end_on_last_published_market_day(self):
        response, _ = self._get("/api/market-volume-share?r=1d&g=d")

        result = response.get_json()
        self.assertEqual(result["asOfDate"], "2023-11-15")
        self.assertEqual([point["date"] for point in result["series"]], ["2023-11-15"])

    def test_skips_malformed_market_points(self):
        volume = [[_ms("2023-11-14"), None], *COINGECKO_VOLUME]
        response, _ = self._get(
            "/api/market-volume-share?r=custom&g=d&sd=2023-11-13&ed=2023-11-16",
            market_get=lambda *_args, **_kwargs: FakeCoinGeckoResponse(volume),
        )

        self.assertEqual(response.status_code, 200)
        dates = [point["date"] for point in response.get_json()["series"]]
        self.assertEqual(dates, ["2023-11-14", "2023-11-15"])

    def test_serves_last_series_as_stale_during_upstream_outage(self):
        initial, _ = self._get(
            "/api/market-volume-share?r=custom&g=d&sd=2023-11-14&ed=2023-11-16"
        )
        market_api._global_market_cache["expires_at"] = 0.0

        def outage(*_args, **_kwargs):
            raise market_api.requests.RequestException("proxy unavailable")

        stale, _ = self._get(
            "/api/market-volume-share?r=custom&g=d&sd=2023-11-14&ed=2023-11-16",
            market_get=outage,
        )

        self.assertEqual(stale.status_code, 200)
        self.assertTrue(stale.get_json()["isStale"])
        self.assertEqual(stale.get_json()["series"], initial.get_json()["series"])

    def test_returns_503_without_any_market_series(self):
        def outage(*_args, **_kwargs):
            raise market_api.requests.RequestException("proxy unavailable")

        response, _ = self._get("/api/market-volume-share?r=30d&g=d", market_get=outage)

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.get_json()["error"],
            "Total market volume is temporarily unavailable",
        )

        retry, get_market = self._get("/api/market-volume-share?r=30d&g=d", market_get=outage)
        self.assertEqual(retry.status_code, 503)
        self.assertEqual(get_market.call_count, 0)

    def test_market_series_is_cached_between_requests(self):
        _, first = self._get("/api/market-volume-share?r=30d&g=d")
        _, second = self._get("/api/market-volume-share?r=30d&g=d")

        self.assertEqual(first.call_count, 1)
        self.assertEqual(second.call_count, 0)


if __name__ == "__main__":
    unittest.main()
