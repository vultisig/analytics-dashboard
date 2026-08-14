#!/usr/bin/env python3
"""
Unit tests for SQL injection prevention in build_date_filter and for
public API hardening (no operational disclosure in system-status).

Tests the date validation regex and address validation independently
of the Flask app, so no backend dependencies are required. The Flask
app itself is not importable here (no flask installed), so handler
checks parse the api_server.py source instead.

Run with: python3 -m unittest tests.test_sql_injection -v
"""

import ast
import re
import time
import unittest
from collections import defaultdict
from pathlib import Path

API_SERVER_PATH = Path(__file__).resolve().parent.parent / 'vultisig-analytics' / 'api_server.py'


def find_function_node(name):
    """Return the ast.FunctionDef node of a top-level function in api_server.py."""
    source = API_SERVER_PATH.read_text()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node, source
    raise AssertionError(f'handler {name!r} not found in {API_SERVER_PATH}')


def read_handler_source(name):
    """Return the source segment of a top-level function in api_server.py."""
    node, source = find_function_node(name)
    return ast.get_source_segment(source, node)


def load_function(name, namespace):
    """Exec a top-level api_server.py function into namespace and return it."""
    exec(read_handler_source(name), namespace)
    return namespace[name]


# Replicate the validation logic from api_server.py build_date_filter
_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_ETH_ADDR_RE = re.compile(r'^0x[a-fA-F0-9]{40}$')


def validate_date_param(value):
    """Validate a date parameter matches YYYY-MM-DD format exactly."""
    if value and not _DATE_RE.match(value):
        raise ValueError(f"Invalid date format, expected YYYY-MM-DD")
    return True


def is_valid_ethereum_address(address):
    """Validate Ethereum address format."""
    return bool(_ETH_ADDR_RE.match(address))


class TestDateValidation(unittest.TestCase):
    """Tests that date validation rejects malicious inputs."""

    def test_valid_dates(self):
        """Valid YYYY-MM-DD dates should pass."""
        valid_dates = ['2024-01-01', '2024-12-31', '2000-06-15', '9999-12-31']
        for date in valid_dates:
            self.assertTrue(validate_date_param(date))

    def test_none_and_empty_pass(self):
        """None and empty string are allowed (treated as no filter)."""
        self.assertTrue(validate_date_param(None))
        self.assertTrue(validate_date_param(''))

    def test_sql_injection_sleep(self):
        """pg_sleep injection should be rejected."""
        with self.assertRaises(ValueError):
            validate_date_param("2024-01-01' AND (SELECT pg_sleep(5)) IS NOT NULL --")

    def test_sql_injection_drop_table(self):
        """DROP TABLE injection should be rejected."""
        with self.assertRaises(ValueError):
            validate_date_param("2024-12-31'; DROP TABLE swaps; --")

    def test_sql_injection_union_select(self):
        """UNION SELECT injection should be rejected."""
        with self.assertRaises(ValueError):
            validate_date_param("' UNION SELECT version(),NULL --")

    def test_sql_injection_or_tautology(self):
        """OR tautology injection should be rejected."""
        with self.assertRaises(ValueError):
            validate_date_param("' OR '1'='1")

    def test_sql_injection_comment(self):
        """SQL comment injection should be rejected."""
        with self.assertRaises(ValueError):
            validate_date_param("2024-01-01'--")

    def test_sql_injection_semicolon(self):
        """Semicolon-based injection should be rejected."""
        with self.assertRaises(ValueError):
            validate_date_param("2024-01-01;")

    def test_partial_date_rejected(self):
        """Partial dates like '2024-01' should be rejected."""
        with self.assertRaises(ValueError):
            validate_date_param('2024-01')

    def test_date_with_time_rejected(self):
        """Dates with time component should be rejected."""
        with self.assertRaises(ValueError):
            validate_date_param('2024-01-01T00:00:00')

    def test_date_with_spaces_rejected(self):
        """Dates with spaces should be rejected."""
        with self.assertRaises(ValueError):
            validate_date_param(' 2024-01-01')
        with self.assertRaises(ValueError):
            validate_date_param('2024-01-01 ')

    def test_date_with_extra_characters_rejected(self):
        """Dates with trailing characters should be rejected."""
        with self.assertRaises(ValueError):
            validate_date_param('2024-01-01abc')

    def test_alphabetic_date_rejected(self):
        """Non-numeric date components should be rejected."""
        with self.assertRaises(ValueError):
            validate_date_param('abcd-ef-gh')


class TestEthereumAddressValidation(unittest.TestCase):
    """Tests for Ethereum address validation used in holder lookup."""

    def test_valid_lowercase(self):
        self.assertTrue(is_valid_ethereum_address('0x' + 'a' * 40))

    def test_valid_uppercase(self):
        self.assertTrue(is_valid_ethereum_address('0x' + 'A' * 40))

    def test_valid_mixed_case(self):
        self.assertTrue(is_valid_ethereum_address('0xAbCdEf0123456789AbCdEf0123456789AbCdEf01'))

    def test_valid_all_digits(self):
        self.assertTrue(is_valid_ethereum_address('0x' + '0' * 40))

    def test_too_short(self):
        self.assertFalse(is_valid_ethereum_address('0x' + 'a' * 39))

    def test_too_long(self):
        self.assertFalse(is_valid_ethereum_address('0x' + 'a' * 41))

    def test_no_prefix(self):
        self.assertFalse(is_valid_ethereum_address('a' * 40))

    def test_injection_attempt(self):
        self.assertFalse(is_valid_ethereum_address("0x' OR '1'='1"))

    def test_empty_string(self):
        self.assertFalse(is_valid_ethereum_address(''))

    def test_invalid_characters(self):
        self.assertFalse(is_valid_ethereum_address('0x' + 'g' * 40))


class TestSystemStatusDisclosure(unittest.TestCase):
    """The public system-status response must never expose raw error text."""

    def test_last_error_absent(self):
        """Neither the query nor the response of get_system_status touches last_error."""
        self.assertNotIn('last_error', read_handler_source('get_system_status'))

    def test_public_fields_present(self):
        """The non-sensitive public contract fields remain in the response."""
        src = read_handler_source('get_system_status')
        for field in ('source', 'last_synced_timestamp', 'latest_data_timestamp', 'is_active'):
            self.assertIn(f"'{field}'", src)


class TestPublicLimitBounds(unittest.TestCase):
    """Externally supplied row-count limits must be clamped to explicit caps."""

    def setUp(self):
        self.parse_limit = load_function('parse_limit', {})

    def test_absent_limit_uses_default(self):
        self.assertEqual(self.parse_limit({}, 50, 200), 50)

    def test_oversized_limit_clamped_to_cap(self):
        self.assertEqual(self.parse_limit({'limit': '999999'}, 50, 200), 200)

    def test_zero_and_negative_clamped_to_one(self):
        self.assertEqual(self.parse_limit({'limit': '0'}, 50, 200), 1)
        self.assertEqual(self.parse_limit({'limit': '-5'}, 50, 200), 1)

    def test_in_range_limit_passes_through(self):
        self.assertEqual(self.parse_limit({'limit': '25'}, 50, 200), 25)

    def test_non_integer_limit_rejected(self):
        with self.assertRaises(ValueError):
            self.parse_limit({'limit': '10; DROP TABLE swaps'}, 50, 200)
        with self.assertRaises(ValueError):
            self.parse_limit({'limit': 'abc'}, 50, 200)

    def test_activity_handler_uses_bounded_limit(self):
        src = read_handler_source('get_recent_activity')
        self.assertIn('parse_limit', src)
        self.assertIn('MAX_ACTIVITY_LIMIT', src)
        self.assertNotIn("int(request.args.get('limit'", src)

    def test_top_paths_handler_uses_bounded_limit(self):
        src = read_handler_source('get_top_paths')
        self.assertIn('parse_limit', src)
        self.assertIn('MAX_TOP_PATHS_LIMIT', src)
        self.assertNotIn("int(request.args.get('limit'", src)


class TestPublicRateLimit(unittest.TestCase):
    """All public API routes get a per-IP limit; holder lookup keeps its stricter one."""

    def _check_rate_limit(self):
        namespace = {'time': time, 'RATE_LIMIT_WINDOW_MS': 60 * 1000, 'MAX_TRACKED_IPS': 10000}
        return load_function('check_rate_limit', namespace)

    def test_blocks_after_max_requests(self):
        check = self._check_rate_limit()
        store = defaultdict(lambda: {'count': 0, 'reset_time': 0})
        for _ in range(10):
            self.assertTrue(check('1.2.3.4', store, 10)['allowed'])
        blocked = check('1.2.3.4', store, 10)
        self.assertFalse(blocked['allowed'])
        self.assertEqual(blocked['remaining'], 0)

    def test_limits_are_per_ip(self):
        check = self._check_rate_limit()
        store = defaultdict(lambda: {'count': 0, 'reset_time': 0})
        for _ in range(10):
            check('1.2.3.4', store, 10)
        self.assertFalse(check('1.2.3.4', store, 10)['allowed'])
        self.assertTrue(check('5.6.7.8', store, 10)['allowed'])

    def test_general_limiter_registered_before_request(self):
        node, _ = find_function_node('limit_public_api')
        decorators = {
            d.attr for d in node.decorator_list if isinstance(d, ast.Attribute)
        }
        self.assertIn('before_request', decorators)

    def test_general_limiter_scopes_to_api_and_skips_preflight(self):
        src = read_handler_source('limit_public_api')
        self.assertIn("'/api/'", src)
        self.assertIn('OPTIONS', src)
        self.assertIn('PUBLIC_RATE_LIMIT_MAX_REQUESTS', src)
        self.assertIn('429', src)

    def test_holder_lookup_keeps_strict_limit(self):
        src = read_handler_source('lookup_holder')
        self.assertIn('check_rate_limit', src)
        self.assertIn('RATE_LIMIT_MAX_REQUESTS', src)
        module = ast.parse(API_SERVER_PATH.read_text())
        strict = {
            t.id: n.value.value
            for n in ast.walk(module) if isinstance(n, ast.Assign)
            for t in n.targets if isinstance(t, ast.Name)
            if isinstance(n.value, ast.Constant)
        }
        self.assertEqual(strict.get('RATE_LIMIT_MAX_REQUESTS'), 10)


if __name__ == '__main__':
    unittest.main(verbosity=2)
