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
import unittest
from pathlib import Path

API_SERVER_PATH = Path(__file__).resolve().parent.parent / 'vultisig-analytics' / 'api_server.py'


def read_handler_source(name):
    """Return the source segment of a top-level function in api_server.py."""
    source = API_SERVER_PATH.read_text()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(source, node)
    raise AssertionError(f'handler {name!r} not found in {API_SERVER_PATH}')


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


if __name__ == '__main__':
    unittest.main(verbosity=2)
