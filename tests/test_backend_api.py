#!/usr/bin/env python3
"""
Backend API Test Suite for Vultisig Analytics Dashboard

This script tests all backend API endpoints to verify they are:
1. Accessible and responding
2. Returning valid JSON responses
3. Returning expected data structure

Usage:
    python test_backend_api.py [--base-url http://localhost:8080]
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from typing import Any

try:
    import requests
except ImportError:
    requests = None

import unittest

if requests is None:
    # Skip this module when discovered by unittest (it's an integration test
    # that requires the `requests` library and a running server).
    raise unittest.SkipTest("requests library not installed")


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


DATETIME_STRING = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')
UTC_OFFSET = re.compile(r'(?:[+-]\d{2}:\d{2}|Z)$')


def find_offsetless_datetimes(node, path="$"):
    """Walk a JSON payload and return paths of datetime strings without a UTC offset.

    Guards the API's single-serializer contract (iso_utc): every datetime the API
    emits must carry an explicit offset, or JS Date() parses it as local time.
    """
    bad = []
    if isinstance(node, dict):
        for key, value in node.items():
            bad += find_offsetless_datetimes(value, f"{path}.{key}")
    elif isinstance(node, list):
        for i, value in enumerate(node[:50]):
            bad += find_offsetless_datetimes(value, f"{path}[{i}]")
    elif isinstance(node, str) and DATETIME_STRING.match(node) and not UTC_OFFSET.search(node):
        bad.append(f"{path} = {node}")
    return bad


def print_pass(message: str):
    print(f"  {Colors.GREEN}[PASS]{Colors.RESET} {message}")


def print_fail(message: str):
    print(f"  {Colors.RED}[FAIL]{Colors.RESET} {message}")


def print_warn(message: str):
    print(f"  {Colors.YELLOW}[WARN]{Colors.RESET} {message}")


def print_info(message: str):
    print(f"  {Colors.BLUE}[INFO]{Colors.RESET} {message}")


def print_header(title: str):
    print(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{title}{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*60}{Colors.RESET}")


class APITester:
    """Test harness for backend API endpoints"""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.passed = 0
        self.failed = 0
        self.warnings = 0

    def test_endpoint(
        self,
        name: str,
        endpoint: str,
        expected_keys: list[str] | None = None,
        expected_type: type | None = None,
        params: dict | None = None
    ) -> bool:
        """Test a single API endpoint"""
        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.get(url, params=params, timeout=30)

            # Check status code
            if response.status_code != 200:
                print_fail(f"{name}: HTTP {response.status_code}")
                self.failed += 1
                return False

            # Parse JSON
            try:
                data = response.json()
            except json.JSONDecodeError:
                print_fail(f"{name}: Invalid JSON response")
                self.failed += 1
                return False

            # Check expected type
            if expected_type is not None:
                if not isinstance(data, expected_type):
                    print_fail(f"{name}: Expected {expected_type.__name__}, got {type(data).__name__}")
                    self.failed += 1
                    return False

            # Check expected keys for dict responses
            if expected_keys and isinstance(data, dict):
                missing_keys = [k for k in expected_keys if k not in data]
                if missing_keys:
                    print_warn(f"{name}: Missing keys: {missing_keys}")
                    self.warnings += 1

            # Enforce the single-serializer contract: no offset-less datetimes
            offsetless = find_offsetless_datetimes(data)
            if offsetless:
                print_fail(f"{name}: offset-less datetime strings: {offsetless[:3]}")
                self.failed += 1
                return False

            print_pass(f"{name}")
            self.passed += 1
            return True

        except requests.exceptions.ConnectionError:
            print_fail(f"{name}: Connection refused (is server running?)")
            self.failed += 1
            return False
        except requests.exceptions.Timeout:
            print_fail(f"{name}: Request timed out")
            self.failed += 1
            return False
        except Exception as e:
            print_fail(f"{name}: {str(e)}")
            self.failed += 1
            return False

    def run_all_tests(self):
        """Run all API endpoint tests"""

        # Test Health Check
        print_header("Health Check Endpoints")
        self.test_endpoint(
            "Health Check",
            "/api/health",
            expected_keys=["status"],
            expected_type=dict
        )

        # Test Legacy Endpoints
        print_header("Legacy Dashboard Endpoints")
        self.test_endpoint(
            "Summary Stats",
            "/api/summary",
            expected_keys=["totalSwaps", "totalFees", "totalVolume"],
            expected_type=dict
        )
        self.test_endpoint(
            "Overview Chart",
            "/api/overview-chart",
            expected_keys=["stats"],
            expected_type=dict
        )
        self.test_endpoint(
            "Timeseries",
            "/api/timeseries",
            expected_keys=["dates", "fees", "volume"],
            expected_type=dict
        )
        self.test_endpoint(
            "Stacked Timeseries",
            "/api/timeseries/stacked",
            expected_keys=["data", "providers"],
            expected_type=dict
        )
        self.test_endpoint(
            "Recent Activity",
            "/api/activity",
            expected_type=list
        )
        self.test_endpoint(
            "Database Stats",
            "/api/stats",
            expected_type=dict
        )
        self.test_endpoint(
            "Stats by Provider",
            "/api/stats/provider",
            expected_type=list
        )
        self.test_endpoint(
            "Stats by Platform",
            "/api/stats/platform",
            expected_type=list
        )
        self.test_endpoint(
            "Top Paths",
            "/api/top-paths",
            expected_type=list
        )

        # Test Revenue Endpoints
        print_header("Revenue API Endpoints")
        self.test_endpoint(
            "Revenue (All Time)",
            "/api/revenue",
            expected_keys=["totalRevenue", "revenueOverTime", "revenueByProvider"],
            expected_type=dict,
            params={"r": "all", "g": "d"}
        )
        self.test_endpoint(
            "Revenue (30 Days)",
            "/api/revenue",
            expected_keys=["totalRevenue"],
            expected_type=dict,
            params={"r": "30d", "g": "d"}
        )
        self.test_endpoint(
            "Revenue by Provider (THORChain)",
            "/api/revenue/provider/thorchain",
            expected_keys=["provider", "totalRevenue", "platformBreakdown"],
            expected_type=dict,
            params={"r": "all", "g": "d"}
        )
        self.test_endpoint(
            "Revenue by Provider (LiFi)",
            "/api/revenue/provider/lifi",
            expected_keys=["provider", "totalRevenue"],
            expected_type=dict,
            params={"r": "all", "g": "d"}
        )
        self.test_endpoint(
            "Revenue by Provider (1inch)",
            "/api/revenue/provider/1inch",
            expected_keys=["provider"],
            expected_type=dict,
            params={"r": "all", "g": "d"}
        )

        # Test Swap Volume Endpoints
        print_header("Swap Volume API Endpoints")
        self.test_endpoint(
            "Swap Volume (All Time)",
            "/api/swap-volume",
            expected_keys=["globalStats", "volumeOverTime", "volumeByProvider"],
            expected_type=dict,
            params={"r": "all", "g": "d"}
        )
        self.test_endpoint(
            "Swap Volume (7 Days)",
            "/api/swap-volume",
            expected_keys=["globalStats"],
            expected_type=dict,
            params={"r": "7d", "g": "d"}
        )
        self.test_endpoint(
            "Vultisig Share of Routed Markets",
            "/api/market-volume-share",
            expected_keys=[
                "series",
                "benchmarks",
                "effectiveGranularity",
            ],
            expected_type=dict,
            params={"r": "30d", "g": "d"},
        )
        self.test_endpoint(
            "Swap Volume by Provider (THORChain)",
            "/api/swap-volume/provider/thorchain",
            expected_keys=["provider", "totalVolume", "platformBreakdown"],
            expected_type=dict,
            params={"r": "all", "g": "d"}
        )

        # Test Swap Count Endpoints
        print_header("Swap Count API Endpoints")
        self.test_endpoint(
            "Swap Count (All Time)",
            "/api/swap-count",
            expected_keys=["totalCount", "countOverTime", "countByProvider"],
            expected_type=dict,
            params={"r": "all", "g": "d"}
        )
        self.test_endpoint(
            "Swap Count by Provider (THORChain)",
            "/api/swap-count/provider/thorchain",
            expected_keys=["provider", "totalCount", "platformBreakdown"],
            expected_type=dict,
            params={"r": "all", "g": "d"}
        )

        # Test Users Endpoints
        print_header("Users API Endpoints")
        self.test_endpoint(
            "Users (All Time)",
            "/api/users",
            expected_keys=["globalStats", "usersOverTime", "usersByProvider"],
            expected_type=dict,
            params={"r": "all", "g": "d"}
        )
        self.test_endpoint(
            "Users by Provider (THORChain)",
            "/api/users/provider/thorchain",
            expected_keys=["provider", "totalUsers", "platformBreakdown"],
            expected_type=dict,
            params={"r": "all", "g": "d"}
        )

        # Test Fee Tiers Endpoint
        print_header("Fee Tiers API Endpoints")
        self.test_endpoint(
            "Fee Tiers (All Time)",
            "/api/users/fee-tiers",
            expected_keys=["tierDistribution", "totalUsers", "totalVolume"],
            expected_type=dict,
            params={"r": "all", "g": "d"}
        )
        self.test_endpoint(
            "Fee Tiers (7 Days)",
            "/api/users/fee-tiers",
            expected_keys=["tierDistribution", "totalUsers", "totalVolume"],
            expected_type=dict,
            params={"r": "7d", "g": "d"}
        )
        self.test_endpoint(
            "Fee Tiers (30 Days)",
            "/api/users/fee-tiers",
            expected_keys=["tierDistribution", "totalUsers", "totalVolume"],
            expected_type=dict,
            params={"r": "30d", "g": "d"}
        )

        # Test Holders Endpoints
        print_header("Holders API Endpoints")
        self.test_endpoint(
            "Holders Overview",
            "/api/holders",
            expected_keys=["tiers", "totalHolders", "tieredHolders"],
            expected_type=dict
        )
        self.test_endpoint(
            "Holder Lookup (Invalid Address)",
            "/api/holders/lookup",
            expected_type=dict,
            params={"address": "0x0000000000000000000000000000000000000000"}
        )

        # Test Referrals Endpoint
        print_header("Referrals API Endpoint")
        self.test_endpoint(
            "Referrals (All Time)",
            "/api/referrals",
            expected_keys=["totalFeesSaved", "totalReferrerRevenue", "leaderboardByRevenue"],
            expected_type=dict,
            params={"r": "all", "g": "d"}
        )

        # Test System Status Endpoint
        print_header("System Status API Endpoint")
        self.test_endpoint(
            "System Status",
            "/api/system-status",
            expected_type=list
        )

        # Print Summary
        print_header("Test Summary")
        total = self.passed + self.failed
        print(f"  Total tests: {total}")
        print_pass(f"Passed: {self.passed}")
        if self.failed > 0:
            print_fail(f"Failed: {self.failed}")
        if self.warnings > 0:
            print_warn(f"Warnings: {self.warnings}")

        return self.failed == 0


def main():
    parser = argparse.ArgumentParser(description="Test Vultisig Analytics Backend API")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8080",
        help="Base URL of the backend API (default: http://localhost:8080)"
    )
    args = parser.parse_args()

    print(f"\nVultisig Analytics - Backend API Test Suite")
    print(f"Testing against: {args.base_url}")
    print(f"Time: {datetime.now().isoformat()}")

    tester = APITester(args.base_url)
    success = tester.run_all_tests()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
