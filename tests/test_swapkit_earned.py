#!/usr/bin/env python3
"""
Pure unit tests for the /api/swapkit/earned helpers: parameter parsing, date
filter, rounding. The helpers are extracted from api_server.py by AST so the
test needs no Flask and no database.

Run with: python3 -m unittest tests.test_swapkit_earned -v
"""

import ast
import re
import unittest
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

API_SERVER_PATH = Path(__file__).resolve().parent.parent / 'vultisig-analytics' / 'api_server.py'


def load_names(function_names, constant_names):
    """Exec the named top-level functions and constants of api_server.py."""
    source = API_SERVER_PATH.read_text()
    tree = ast.parse(source)
    namespace = {
        'datetime': datetime, 'timedelta': timedelta,
        'Decimal': Decimal, 'ROUND_HALF_UP': ROUND_HALF_UP, 're': re,
    }
    wanted = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in function_names:
            wanted.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id in constant_names for t in node.targets
        ):
            wanted.append(node)
    found = {n.name for n in wanted if isinstance(n, ast.FunctionDef)}
    assert found == set(function_names), f'missing functions: {set(function_names) - found}'
    exec(compile(ast.Module(body=wanted, type_ignores=[]), str(API_SERVER_PATH), 'exec'), namespace)
    return namespace


NS = load_names(
    ['round_money', 'parse_swapkit_granularity', 'parse_iso_date', 'build_swapkit_date_filter'],
    ['ISO_DATE_RE', 'GRAN_TO_SQL', 'RANGE_TO_SQL', 'SWAPKIT_RANGES', 'SWAPKIT_GRANULARITIES', 'MONEY_QUANTUM'],
)
round_money = NS['round_money']
parse_granularity = NS['parse_swapkit_granularity']
build_filter = NS['build_swapkit_date_filter']

TODAY = date(2026, 3, 15)


class RoundMoneyTest(unittest.TestCase):
    def test_rounds_half_up_to_two_places(self):
        self.assertEqual(round_money(Decimal('1.005')), 1.01)
        self.assertEqual(round_money(Decimal('1.004999')), 1.0)
        self.assertEqual(round_money(Decimal('2.345000')), 2.35)

    def test_returns_float(self):
        self.assertIsInstance(round_money(Decimal('3')), float)
        self.assertEqual(round_money(Decimal(0)), 0.0)


class StrictInputTest(unittest.TestCase):
    def test_non_finite_money_is_refused(self):
        for bad in ('NaN', 'Infinity', '-Infinity', Decimal('NaN')):
            with self.assertRaises(ArithmeticError):
                round_money(bad)

    def test_dates_accept_ascii_digits_only(self):
        parse = NS['parse_iso_date']
        self.assertEqual(parse('2026-03-15', 'x'), date(2026, 3, 15))
        for bad in ('\u0662\u0660\u0662\u0666-03-15', '2026-03-15\n', ' 2026-03-15'):
            with self.assertRaises(ValueError):
                parse(bad, 'x')


class GranularityTest(unittest.TestCase):
    def test_default_is_day(self):
        self.assertEqual(parse_granularity(None), 'day')
        self.assertEqual(parse_granularity(''), 'day')

    def test_short_and_long_names(self):
        for raw, expected in (('d', 'day'), ('w', 'week'), ('m', 'month'),
                              ('day', 'day'), ('week', 'week'), ('month', 'month')):
            self.assertEqual(parse_granularity(raw), expected)

    def test_hourly_becomes_daily(self):
        self.assertEqual(parse_granularity('h'), 'day')
        self.assertEqual(parse_granularity('hour'), 'day')

    def test_unknown_value_raises(self):
        with self.assertRaises(ValueError):
            parse_granularity('year')


class DateFilterTest(unittest.TestCase):
    def test_all_stops_before_today(self):
        self.assertEqual(build_filter(None, None, None, TODAY), ('date < %s', [TODAY]))
        self.assertEqual(build_filter('all', None, None, TODAY), ('date < %s', [TODAY]))

    def test_one_day_is_last_closed_day(self):
        self.assertEqual(build_filter('1d', None, None, TODAY), ('date = %s', [date(2026, 3, 14)]))
        self.assertEqual(build_filter('24h', None, None, TODAY), ('date = %s', [date(2026, 3, 14)]))

    def test_rolling_ranges(self):
        for raw, days in (('7d', 7), ('30d', 30), ('90d', 90), ('1y', 365), ('365d', 365)):
            sql, params = build_filter(raw, None, None, TODAY)
            self.assertEqual(sql, 'date >= %s AND date < %s')
            self.assertEqual(params, [TODAY - timedelta(days=days), TODAY])

    def test_ytd_starts_on_january_first(self):
        self.assertEqual(build_filter('ytd', None, None, TODAY), ('date >= %s AND date < %s', [date(2026, 1, 1), TODAY]))

    def test_custom_uses_bound_dates(self):
        sql, params = build_filter('custom', '2026-01-02', '2026-02-03', TODAY)
        self.assertEqual(sql, 'date >= %s AND date <= %s')
        self.assertEqual(params, [date(2026, 1, 2), date(2026, 2, 3)])

    def test_user_input_never_enters_sql(self):
        sql, _ = build_filter('custom', '2026-01-02', '2026-02-03', TODAY)
        self.assertNotIn('2026', sql)

    def test_invalid_inputs_raise(self):
        bad_cases = [
            ('bogus', None, None),
            ('custom', None, None),
            ('custom', '2026-01-02', None),
            ('custom', None, '2026-01-02'),
            ('custom', '2026-1-2', '2026-02-03'),
            ('custom', "2026-01-02'; DROP TABLE x;--", '2026-02-03'),
            ('custom', '2026-02-31', '2026-03-01'),
            ('custom', '2026-03-02', '2026-03-01'),
        ]
        for range_param, start, end in bad_cases:
            with self.subTest(range=range_param, start=start, end=end):
                with self.assertRaises(ValueError):
                    build_filter(range_param, start, end, TODAY)


if __name__ == '__main__':
    unittest.main()
