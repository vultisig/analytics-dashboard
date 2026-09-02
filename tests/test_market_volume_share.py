"""Tests for historical Vultisig/comparable-market volume share."""

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
        config=SimpleNamespace(
            ARKHAM_PROVIDERS=("1inch", "kyberswap"),
            DEX_REVENUE_PROVIDERS=("1inch", "kyberswap", "swapkit"),
            SWAPKIT_PROTOCOL="swapkit",
            SWAPKIT_PAYOUT_SENDERS=frozenset(),
            SWAPKIT_FEE_SENDERS=frozenset(),
        ),
    ),
}

with patch.dict(sys.modules, _stubs):
    spec = importlib.util.spec_from_file_location(
        "market_volume_api_under_test", _API_SERVER_PATH
    )
    market_api = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(market_api)


def _timestamp(date_value):
    return int(
        datetime.strptime(date_value, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )


class FakeDefiLlamaResponse:
    SERIES = {
        "thorchain-dex": ("THORChain DEX", [1_000, 2_000]),
        "li.fi-dex-aggregator": ("LI.FI DEX Aggregator", [500, 1_000]),
    }

    def __init__(self, url):
        self.url = url

    def raise_for_status(self):
        return None

    def json(self):
        slug = next(slug for slug in self.SERIES if f"/{slug}?" in self.url)
        name, volumes = self.SERIES[slug]
        return {
            "name": name,
            "totalDataChart": [
                [_timestamp("2023-11-14"), volumes[0]],
                [_timestamp("2023-11-15"), volumes[1]],
            ],
        }


class FakeMidgardResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {
            "intervals": [
                {
                    "startTime": str(_timestamp("2023-11-14")),
                    "totalVolumeUSD": "150000",
                },
                {
                    "startTime": str(_timestamp("2023-11-15")),
                    "totalVolumeUSD": "300000",
                },
            ],
        }


class MarketVolumeShareApiTests(unittest.TestCase):
    def setUp(self):
        market_api._global_market_cache["data"] = None
        market_api._global_market_cache["expires_at"] = 0
        market_api._global_market_cache["filling"] = False
        self.database = Mock()
        self.database.execute_query.side_effect = self._execute_query
        self.client = market_api.app.test_client()

    @staticmethod
    def _execute_query(query, params=None, fetch=False):
        if "FROM swaps" in query:
            return [
                {"date": "2023-11-14", "provider": "thorchain", "volume": 100},
                {"date": "2023-11-15", "provider": "thorchain", "volume": 200},
                {"date": "2023-11-14", "provider": "lifi", "volume": 50},
                {"date": "2023-11-15", "provider": "lifi", "volume": 100},
                {"date": "2023-11-14", "provider": "mayachain", "volume": 75},
                {"date": "2023-11-15", "provider": "mayachain", "volume": 150},
            ]
        raise AssertionError(f"Unexpected query: {query}")

    @staticmethod
    def _get_market(url, **_kwargs):
        if "midgard.mayachain.info" in url:
            return FakeMidgardResponse()
        return FakeDefiLlamaResponse(url)

    @staticmethod
    def _point(result, provider, date_value):
        return next(
            point
            for point in result["series"]
            if point["provider"] == provider and point["date"] == date_value
        )

    def test_endpoint_calculates_daily_like_for_like_provider_shares(self):
        with patch.object(market_api, "db_manager", self.database), patch.object(
            market_api.requests, "get", side_effect=self._get_market
        ) as get_market:
            response = self.client.get(
                "/api/market-volume-share?r=custom&g=d&sd=2023-11-14&ed=2023-11-15"
            )

        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertEqual(len(result["benchmarks"]), 4)
        self.assertEqual(len(result["series"]), 8)
        self.assertEqual(result["benchmarks"][0]["provider"], "all")
        self.assertEqual(result["effectiveGranularity"], "day")
        self.assertAlmostEqual(
            self._point(result, "thorchain", "2023-11-14")["sharePercent"], 10
        )
        self.assertAlmostEqual(
            self._point(result, "lifi", "2023-11-15")["sharePercent"], 10
        )
        self.assertAlmostEqual(
            self._point(result, "mayachain", "2023-11-14")["sharePercent"], 5
        )
        all_routes = self._point(result, "all", "2023-11-14")
        self.assertEqual(all_routes["vultisigVolumeUsd"], 225)
        self.assertEqual(all_routes["marketVolumeUsd"], 3_000)
        self.assertAlmostEqual(all_routes["sharePercent"], 7.5)
        self.assertFalse(result["isStale"])
        self.assertIn("MayaChain", " ".join(result["notes"]))
        self.assertIn("overlap", " ".join(result["notes"]))
        self.assertIn("max-age=60", response.headers["Cache-Control"])
        self.assertEqual(get_market.call_count, 3)

        swaps_query = self.database.execute_query.call_args_list[0].args[0]
        self.assertIn("bridge_metadata,from_chain_id", swaps_query)
        self.assertIn("bridge_metadata,to_chain_id", swaps_query)
        self.assertEqual(self.database.execute_query.call_count, 1)

    def test_endpoint_sums_numerator_and_denominator_before_weekly_share(self):
        with patch.object(market_api, "db_manager", self.database), patch.object(
            market_api.requests, "get", side_effect=self._get_market
        ):
            response = self.client.get(
                "/api/market-volume-share?r=custom&g=w&sd=2023-11-14&ed=2023-11-15"
            )

        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        thorchain = self._point(result, "thorchain", "2023-11-12")
        self.assertEqual(thorchain["vultisigVolumeUsd"], 300)
        self.assertEqual(thorchain["marketVolumeUsd"], 3_000)
        self.assertAlmostEqual(thorchain["sharePercent"], 10)
        all_routes = self._point(result, "all", "2023-11-12")
        self.assertEqual(all_routes["vultisigVolumeUsd"], 675)
        self.assertEqual(all_routes["marketVolumeUsd"], 9_000)
        self.assertAlmostEqual(all_routes["sharePercent"], 7.5)
        self.assertEqual(result["effectiveGranularity"], "week")

    def test_all_routes_only_uses_dates_shared_by_every_benchmark(self):
        def get_market(url, **_kwargs):
            if "midgard.mayachain.info" not in url:
                return FakeDefiLlamaResponse(url)
            response = Mock()
            response.json.return_value = {
                "intervals": [{
                    "startTime": str(_timestamp("2023-11-14")),
                    "totalVolumeUSD": "150000",
                }],
            }
            return response

        with patch.object(market_api, "db_manager", self.database), patch.object(
            market_api.requests, "get", side_effect=get_market
        ):
            response = self.client.get(
                "/api/market-volume-share?r=custom&g=d&sd=2023-11-14&ed=2023-11-15"
            )

        self.assertEqual(response.status_code, 200)
        all_dates = [
            point["date"]
            for point in response.get_json()["series"]
            if point["provider"] == "all"
        ]
        self.assertEqual(all_dates, ["2023-11-14"])

    def test_all_time_series_starts_at_each_providers_first_vultisig_date(self):
        def execute_query(query, params=None, fetch=False):
            rows = self._execute_query(query, params, fetch)
            if "FROM swaps" in query:
                return [
                    row for row in rows
                    if row["provider"] != "lifi" or row["date"] == "2023-11-15"
                ]
            return rows

        self.database.execute_query.side_effect = execute_query
        with patch.object(market_api, "db_manager", self.database), patch.object(
            market_api.requests, "get", side_effect=self._get_market
        ):
            response = self.client.get("/api/market-volume-share?r=all&g=d")

        self.assertEqual(response.status_code, 200)
        lifi_dates = [
            point["date"]
            for point in response.get_json()["series"]
            if point["provider"] == "lifi"
        ]
        self.assertEqual(lifi_dates, ["2023-11-15"])

    def test_endpoint_uses_last_good_series_during_upstream_outage(self):
        with patch.object(market_api, "db_manager", self.database), patch.object(
            market_api.requests, "get", side_effect=self._get_market
        ):
            initial_response = self.client.get(
                "/api/market-volume-share?r=custom&g=d&sd=2023-11-14&ed=2023-11-15"
            )

        market_api._global_market_cache["expires_at"] = 0
        with patch.object(market_api, "db_manager", self.database), patch.object(
            market_api.requests,
            "get",
            side_effect=market_api.requests.RequestException("upstream unavailable"),
        ):
            stale_response = self.client.get(
                "/api/market-volume-share?r=custom&g=d&sd=2023-11-14&ed=2023-11-15"
            )

        self.assertEqual(initial_response.status_code, 200)
        self.assertEqual(stale_response.status_code, 200)
        self.assertTrue(stale_response.get_json()["isStale"])
        self.assertEqual(len(stale_response.get_json()["series"]), 8)

    def test_endpoint_returns_503_when_no_benchmark_is_available(self):
        with patch.object(market_api, "db_manager", self.database), patch.object(
            market_api.requests,
            "get",
            side_effect=market_api.requests.RequestException("upstream unavailable"),
        ):
            response = self.client.get("/api/market-volume-share?r=30d&g=d")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.get_json()["error"],
            "Comparable market volume is temporarily unavailable",
        )

    def test_mayachain_total_volume_usd_is_cents(self):
        self.assertEqual(
            market_api._mayachain_interval_volume_usd({"totalVolumeUSD": "200000000"}),
            2_000_000.0,
        )
        self.assertEqual(
            market_api._mayachain_interval_volume_usd({"totalVolumeUSD": "150000"}),
            1_500.0,
        )

    def test_drops_incomplete_current_utc_day(self):
        with patch.object(market_api, "_utc_today", return_value=date(2023, 11, 15)), patch.object(
            market_api, "db_manager", self.database
        ), patch.object(market_api.requests, "get", side_effect=self._get_market):
            response = self.client.get(
                "/api/market-volume-share?r=custom&g=d&sd=2023-11-14&ed=2023-11-15"
            )

        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertEqual(result["asOfDate"], "2023-11-14")
        dates = {point["date"] for point in result["series"]}
        self.assertEqual(dates, {"2023-11-14"})

    def test_one_day_range_uses_last_complete_utc_day(self):
        with patch.object(market_api, "_utc_today", return_value=date(2023, 11, 15)), patch.object(
            market_api, "db_manager", self.database
        ), patch.object(market_api.requests, "get", side_effect=self._get_market):
            response = self.client.get("/api/market-volume-share?r=1d&g=d")

        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertEqual(result["asOfDate"], "2023-11-14")
        dates = {point["date"] for point in result["series"]}
        self.assertEqual(dates, {"2023-11-14"})
        swaps_call = self.database.execute_query.call_args_list[0]
        swaps_query = swaps_call.args[0]
        swaps_params = swaps_call.args[1]
        self.assertIn("date_only >= %s::date", swaps_query)
        self.assertIn("date_only <= %s::date", swaps_query)
        self.assertEqual(swaps_params[1:], ('2023-11-14', '2023-11-14'))
        self.assertNotIn("INTERVAL '24 hours'", swaps_query)

    def test_all_routes_omitted_when_a_benchmark_is_missing(self):
        def get_market(url, **_kwargs):
            if "midgard.mayachain.info" in url:
                raise market_api.requests.RequestException("midgard timeout")
            return FakeDefiLlamaResponse(url)

        with patch.object(market_api, "db_manager", self.database), patch.object(
            market_api.requests, "get", side_effect=get_market
        ):
            response = self.client.get(
                "/api/market-volume-share?r=custom&g=d&sd=2023-11-14&ed=2023-11-15"
            )

        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertFalse(result["isStale"])
        providers = {benchmark["provider"] for benchmark in result["benchmarks"]}
        self.assertEqual(providers, {"thorchain", "lifi"})
        series_providers = {point["provider"] for point in result["series"]}
        self.assertEqual(series_providers, {"thorchain", "lifi"})

    def test_snapshot_fetch_runs_without_cache_lock(self):
        held = []
        real_fetch = market_api._fetch_market_benchmark

        def fetch_and_check(provider, benchmark):
            held.append(market_api._global_market_cache_lock.locked())
            return real_fetch(provider, benchmark)

        with patch.object(
            market_api, "_fetch_market_benchmark", side_effect=fetch_and_check
        ), patch.object(market_api.requests, "get", side_effect=self._get_market):
            snapshot = market_api.get_global_market_snapshot()

        self.assertIn("thorchain", snapshot["providers"])
        self.assertTrue(held)
        self.assertFalse(any(held))
        self.assertFalse(market_api._global_market_cache["filling"])


if __name__ == "__main__":
    unittest.main()
