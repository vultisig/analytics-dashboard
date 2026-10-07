#!/usr/bin/env python3
"""
Database-backed tests for swapkit_daily and GET /api/swapkit/earned.

Needs a Postgres database. Set TEST_DATABASE_URL, for example:
    docker run --rm -d -e POSTGRES_PASSWORD=test -p 55433:5432 postgres:16
    TEST_DATABASE_URL=postgresql://postgres:test@localhost:55433/postgres \
        python3 -m unittest tests.test_swapkit_daily_db -v
Without it, every test here is skipped. All figures are fake.

Run this file in its own process: other test modules replace psycopg2 with a mock.
"""

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parent.parent
ANALYTICS_DIR = ROOT / 'vultisig-analytics'
MIGRATION = ANALYTICS_DIR / 'migrations' / 'create_swapkit_daily.sql'
DATABASE_URL = os.environ.get('TEST_DATABASE_URL')

# Minimal copy of the sync_status table from ingestors/database_schema.sql.
SYNC_STATUS_DDL = """
CREATE TABLE IF NOT EXISTS sync_status (
    id SERIAL PRIMARY KEY,
    source VARCHAR(20) NOT NULL UNIQUE,
    last_synced_timestamp TIMESTAMPTZ,
    latest_data_timestamp TIMESTAMPTZ,
    last_synced_block BIGINT,
    next_page_token TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""


def today_utc():
    return datetime.now(timezone.utc).date()


@unittest.skipUnless(DATABASE_URL, 'TEST_DATABASE_URL is not set; skipping database tests')
class SwapkitDailyDbTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg2
        if isinstance(psycopg2, Mock) or not hasattr(psycopg2, '__version__'):
            raise unittest.SkipTest('psycopg2 is mocked in this process; run this file alone')
        cls.psycopg2 = psycopg2
        os.environ['DATABASE_URL'] = DATABASE_URL
        sys.path.insert(0, str(ANALYTICS_DIR))
        import api_server
        cls.api = api_server
        cls.migration_sql = MIGRATION.read_text()

    def setUp(self):
        self.conn = self.psycopg2.connect(DATABASE_URL, options='-c timezone=UTC')
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        self.cur.execute('DROP TABLE IF EXISTS swapkit_daily')
        self.cur.execute('DROP FUNCTION IF EXISTS record_swapkit_sync(DATE, TEXT)')
        self.cur.execute('DROP TABLE IF EXISTS sync_status')
        self.cur.execute(SYNC_STATUS_DDL)
        self.cur.execute(self.migration_sql)
        self.api.public_rate_limit_store.clear()
        self.client = self.api.app.test_client()

    def tearDown(self):
        self.cur.close()
        self.conn.close()

    # -- helpers ------------------------------------------------------------

    def insert(self, day, provider, revenue, volume):
        self.cur.execute(
            'INSERT INTO swapkit_daily (date, provider, revenue_usd, volume_usd) VALUES (%s, %s, %s, %s)',
            (day, provider, revenue, volume),
        )

    def get(self, query=''):
        return self.client.get('/api/swapkit/earned' + query)

    def dates(self, response):
        return sorted({row['date'] for row in response.get_json()['series']})

    def sync(self, latest, error=None):
        self.cur.execute('SELECT record_swapkit_sync(%s, %s)', (latest, error))

    def status_row(self):
        self.cur.execute(
            "SELECT last_synced_timestamp, latest_data_timestamp, error_count, last_error, is_active "
            "FROM sync_status WHERE source = 'swapkit-earned'"
        )
        return self.cur.fetchone()

    # -- migration ----------------------------------------------------------

    def test_migration_runs_twice(self):
        self.insert('2026-01-01', 'alpha', 1, 2)
        self.cur.execute(self.migration_sql)
        self.cur.execute(self.migration_sql)
        self.cur.execute('SELECT count(*) FROM swapkit_daily')
        self.assertEqual(self.cur.fetchone()[0], 1)
        self.cur.execute("SELECT to_regproc('record_swapkit_sync') IS NOT NULL")
        self.assertTrue(self.cur.fetchone()[0])

    def test_table_rejects_negative_values_and_duplicates(self):
        with self.assertRaises(self.psycopg2.errors.CheckViolation):
            self.insert('2026-01-01', 'alpha', -1, 0)
        self.insert('2026-01-01', 'alpha', 1, 1)
        with self.assertRaises(self.psycopg2.errors.UniqueViolation):
            self.insert('2026-01-01', 'alpha', 1, 1)

    def test_table_rejects_nan(self):
        for col in ('revenue_usd', 'volume_usd'):
            with self.assertRaises(self.psycopg2.errors.CheckViolation):
                self.cur.execute(
                    "INSERT INTO swapkit_daily (date, provider, revenue_usd, volume_usd) "
                    "VALUES ('2026-01-01', 'nan', %s, %s)",
                    ('NaN', 1) if col == 'revenue_usd' else (1, 'NaN'))

    def test_migration_upgrades_a_table_with_old_checks(self):
        self.cur.execute('DROP TABLE swapkit_daily')
        self.cur.execute(
            'CREATE TABLE swapkit_daily (date DATE NOT NULL, provider VARCHAR(32) NOT NULL, '
            'revenue_usd NUMERIC(20,6) NOT NULL CHECK (revenue_usd >= 0), '
            'volume_usd NUMERIC(24,6) NOT NULL CHECK (volume_usd >= 0), '
            'first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(), '
            'updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (date, provider))')
        self.cur.execute(self.migration_sql)
        self.cur.execute(self.migration_sql)
        with self.assertRaises(self.psycopg2.errors.CheckViolation):
            self.insert('2026-01-01', 'alpha', 'NaN', 1)

    # -- record_swapkit_sync ------------------------------------------------

    def test_sync_success_writes_status(self):
        self.sync('2026-01-05')
        synced, latest, errors, last_error, active = self.status_row()
        self.assertIsNotNone(synced)
        self.assertEqual(latest, datetime(2026, 1, 5, tzinfo=timezone.utc))
        self.assertEqual((errors, last_error, active), (0, None, True))

    def test_sync_failure_keeps_previous_values(self):
        self.sync('2026-01-05')
        synced_before, latest_before, *_ = self.status_row()
        self.sync(None, 'UPSTREAM_ERROR')
        synced, latest, errors, last_error, _ = self.status_row()
        self.assertEqual((synced, latest), (synced_before, latest_before))
        self.assertEqual((errors, last_error), (1, 'UPSTREAM_ERROR'))
        self.sync(None, 'UPSTREAM_ERROR')
        self.assertEqual(self.status_row()[2], 2)

    def test_sync_failure_with_a_date_keeps_old_latest(self):
        self.sync('2026-01-05')
        _, latest_before, *_ = self.status_row()
        self.sync('2026-02-01', 'UPSTREAM_ERROR')
        self.assertEqual(self.status_row()[1], latest_before)
        self.sync('2026-02-01', None)
        self.assertEqual(self.status_row()[1], datetime(2026, 2, 1, tzinfo=timezone.utc))

    def test_sync_first_failure_with_a_date_stores_no_latest(self):
        self.sync('2026-02-01', 'UPSTREAM_ERROR')
        self.assertIsNone(self.status_row()[1])

    def test_sync_success_after_failure_clears_error(self):
        self.sync(None, 'UPSTREAM_ERROR')
        self.sync('2026-01-06')
        _, latest, errors, last_error, _ = self.status_row()
        self.assertEqual(latest, datetime(2026, 1, 6, tzinfo=timezone.utc))
        self.assertEqual((errors, last_error), (0, None))

    def test_sync_first_run_failure_with_null_latest(self):
        self.sync(None, 'AUTH_FAILED')
        synced, latest, errors, last_error, _ = self.status_row()
        self.assertEqual((synced, latest, errors, last_error), (None, None, 1, 'AUTH_FAILED'))

    def test_sync_free_text_error_becomes_invalid_code(self):
        for text in ('x' * 200, 'lower_case', 'Has Space', 'A', 'A' * 33, ''):
            self.sync(None, text)
            self.assertEqual(self.status_row()[3], 'INVALID_CODE', text)
        self.sync(None, 'A' * 32)
        self.assertEqual(self.status_row()[3], 'A' * 32)

    def test_sync_success_needs_a_closed_day(self):
        today = datetime.now(timezone.utc).date()
        for latest in (None, today, today + timedelta(days=1)):
            with self.assertRaises(self.psycopg2.errors.InvalidParameterValue):
                self.sync(latest)
        self.sync(today - timedelta(days=1))
        self.assertEqual(self.status_row()[2], 0)

    def test_sync_error_counter_saturates(self):
        self.sync(None, 'UPSTREAM_ERROR')
        self.cur.execute("UPDATE sync_status SET error_count = 2147483647 WHERE source = 'swapkit-earned'")
        self.sync(None, 'UPSTREAM_ERROR')
        self.assertEqual(self.status_row()[2], 2147483647)

    # -- endpoint: empty and filled ------------------------------------------

    def test_empty_table(self):
        response = self.get()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {
            'series': [],
            'totals': {'revenue_usd': 0.0, 'volume_usd': 0.0},
            'granularity': 'day',
            'last_updated': None,
            'data_through': None,
        })

    def test_filled_table_shape_rounding_and_freshness(self):
        self.insert('2026-03-01', 'alpha', Decimal('1.004000'), Decimal('10.005000'))
        self.insert('2026-03-01', 'beta', Decimal('2.006000'), Decimal('20.000000'))
        self.sync('2026-03-01')
        body = self.get('?r=all').get_json()
        self.assertEqual(body['series'], [
            {'date': '2026-03-01', 'provider': 'alpha', 'revenue_usd': 1.0, 'volume_usd': 10.01},
            {'date': '2026-03-01', 'provider': 'beta', 'revenue_usd': 2.01, 'volume_usd': 20.0},
        ])
        self.assertEqual(body['totals'], {'revenue_usd': 3.01, 'volume_usd': 30.01})
        self.assertEqual(body['data_through'], '2026-03-01T00:00:00+00:00')
        self.assertTrue(body['last_updated'].endswith('+00:00'))

    def test_stale_failure_row_returns_no_freshness_from_nothing(self):
        self.sync(None, 'AUTH_FAILED')
        body = self.get().get_json()
        self.assertIsNone(body['last_updated'])
        self.assertIsNone(body['data_through'])

    # -- endpoint: ranges ------------------------------------------------------

    def test_each_range(self):
        today = today_utc()
        offsets = [1, 3, 10, 40, 100, 400]
        for offset in offsets:
            self.insert(today - timedelta(days=offset), 'alpha', 1, 1)
        ytd_start = today.replace(month=1, day=1)
        expected = {
            '1d': [1],
            '7d': [1, 3],
            '30d': [1, 3, 10],
            '90d': [1, 3, 10, 40],
            '1y': [1, 3, 10, 40, 100],
            'all': offsets,
            'ytd': [o for o in offsets if today - timedelta(days=o) >= ytd_start],
        }
        for range_param, wanted in expected.items():
            with self.subTest(range=range_param):
                response = self.get(f'?r={range_param}')
                self.assertEqual(response.status_code, 200)
                want_dates = sorted((today - timedelta(days=o)).isoformat() for o in wanted)
                self.assertEqual(self.dates(response), want_dates)

    def test_long_parameter_names_and_custom_range(self):
        for day in ('2026-02-01', '2026-02-10', '2026-02-20'):
            self.insert(day, 'alpha', 1, 1)
        response = self.get('?range=custom&startDate=2026-02-05&endDate=2026-02-15')
        self.assertEqual(self.dates(response), ['2026-02-10'])
        response = self.get('?r=custom&sd=2026-02-01&ed=2026-02-10')
        self.assertEqual(self.dates(response), ['2026-02-01', '2026-02-10'])

    # -- endpoint: granularity ---------------------------------------------------

    def test_granularity_buckets_are_utc_week_and_month(self):
        # 2026-09-29 is a Tuesday and 2026-10-01 a Thursday of the same ISO week.
        self.insert('2026-09-29', 'alpha', Decimal('1.004'), Decimal('1'))
        self.insert('2026-10-01', 'alpha', Decimal('2.006'), Decimal('2'))
        self.insert('2026-10-01', 'beta', Decimal('5'), Decimal('5'))
        base = '?r=custom&sd=2026-09-01&ed=2026-10-31'

        day = self.get(base + '&g=d').get_json()
        self.assertEqual(day['granularity'], 'day')
        self.assertEqual(len(day['series']), 3)

        hour = self.get(base + '&g=h').get_json()
        self.assertEqual(hour['granularity'], 'day')
        self.assertEqual(hour['series'], day['series'])

        week = self.get(base + '&g=w').get_json()
        self.assertEqual(week['granularity'], 'week')
        self.assertEqual(
            [(r['date'], r['provider'], r['revenue_usd']) for r in week['series']],
            [('2026-09-28', 'alpha', 3.01), ('2026-09-28', 'beta', 5.0)],
        )

        month = self.get(base + '&granularity=month').get_json()
        self.assertEqual(month['granularity'], 'month')
        self.assertEqual(
            [(r['date'], r['provider'], r['revenue_usd']) for r in month['series']],
            [('2026-09-01', 'alpha', 1.0), ('2026-10-01', 'alpha', 2.01), ('2026-10-01', 'beta', 5.0)],
        )
        for body in (day, week, month):
            self.assertEqual(body['totals']['revenue_usd'], 8.01)

    # -- endpoint: errors ----------------------------------------------------------

    def test_bad_requests_return_400(self):
        bad_queries = [
            '?r=bogus',
            '?r=custom',
            '?r=custom&sd=2026-01-01',
            '?r=custom&ed=2026-01-01',
            '?r=custom&sd=2026-1-1&ed=2026-01-02',
            "?r=custom&sd=2026-01-01'--&ed=2026-01-02",
            '?r=custom&sd=2026-02-31&ed=2026-03-01',
            '?r=custom&sd=2026-03-02&ed=2026-03-01',
            '?g=year',
        ]
        for query in bad_queries:
            with self.subTest(query=query):
                response = self.get(query)
                self.assertEqual(response.status_code, 400)
                self.assertIn('error', response.get_json())

    def test_sql_injection_attempt_leaves_table_intact(self):
        self.insert('2026-01-01', 'alpha', 1, 1)
        self.get("?r=custom&sd=2026-01-01';DROP TABLE swapkit_daily;--&ed=2026-01-02")
        self.cur.execute('SELECT count(*) FROM swapkit_daily')
        self.assertEqual(self.cur.fetchone()[0], 1)

    def test_rate_limit_and_cors_match_neighbours(self):
        response = self.client.get('/api/swapkit/earned', headers={'Origin': 'https://example.com'})
        self.assertIn(response.headers.get('Access-Control-Allow-Origin'), ('*', 'https://example.com'))
        self.api.public_rate_limit_store.clear()
        for _ in range(self.api.PUBLIC_RATE_LIMIT_MAX_REQUESTS):
            self.get()
        self.assertEqual(self.get().status_code, 429)

    # -- regression: existing endpoints --------------------------------------------

    def test_system_status_lists_the_new_source(self):
        self.sync('2026-01-05')
        rows = self.client.get('/api/system-status').get_json()
        row = next(r for r in rows if r['source'] == 'swapkit-earned')
        self.assertEqual(row['latest_data_timestamp'], '2026-01-05T00:00:00+00:00')
        self.assertFalse(row['has_error'])

    def test_existing_revenue_route_does_not_touch_new_table(self):
        import inspect
        self.assertNotIn('swapkit', inspect.getsource(self.api.get_revenue).lower())
        self.assertNotIn('swapkit', inspect.getsource(self.api.build_date_filter).lower())
        rules = {rule.rule for rule in self.api.app.url_map.iter_rules()}
        self.assertIn('/api/revenue', rules)
        self.assertEqual(sum(1 for r in rules if 'swapkit' in r), 1)

    # -- endpoint: edge cases ---------------------------------------------------

    def test_provider_with_zero_revenue_and_positive_volume_is_kept(self):
        self.insert('2026-01-05', 'alpha', 0, Decimal('12.5'))
        body = self.get('?r=all').get_json()
        self.assertEqual(
            body['series'],
            [{'date': '2026-01-05', 'provider': 'alpha', 'revenue_usd': 0.0, 'volume_usd': 12.5}],
        )
        self.assertEqual(body['totals'], {'revenue_usd': 0.0, 'volume_usd': 12.5})

    def test_half_cent_rounds_up_and_below_half_cent_rounds_down(self):
        self.insert('2026-01-05', 'alpha', Decimal('0.005'), Decimal('0.004999'))
        row = self.get('?r=all').get_json()['series'][0]
        self.assertEqual((row['revenue_usd'], row['volume_usd']), (0.01, 0.0))

    def test_huge_numeric_stays_finite_and_close(self):
        self.insert('2026-01-05', 'alpha', Decimal('99999999999999.985'), Decimal('999999999999999999.99'))
        body = self.get('?r=all').get_json()
        self.assertEqual(body['series'][0]['revenue_usd'], float(Decimal('99999999999999.99')))
        self.assertEqual(body['series'][0]['volume_usd'], float(Decimal('999999999999999999.99')))

    def test_future_rows_never_appear_in_rolling_ranges_or_all(self):
        today = today_utc()
        self.insert(today - timedelta(days=1), 'alpha', 1, 1)
        self.insert(today, 'alpha', 2, 2)
        self.insert(today + timedelta(days=3), 'alpha', 4, 4)
        for query in ('?r=all', '?r=7d', '?r=1y', '?r=ytd', '?r=1d'):
            with self.subTest(query=query):
                body = self.get(query).get_json()
                self.assertTrue(all(r['date'] < today.isoformat() for r in body['series']), body['series'])
        explicit = self.get('?r=custom&sd=%s&ed=%s' % (today, today + timedelta(days=3))).get_json()
        self.assertEqual(len(explicit['series']), 2)

    def test_leap_day_is_a_row_and_buckets_into_its_week_and_month(self):
        self.insert('2024-02-29', 'alpha', 1, 1)
        base = '?r=custom&sd=2024-02-01&ed=2024-03-31'
        self.assertEqual(self.dates(self.get(base)), ['2024-02-29'])
        self.assertEqual(self.dates(self.get(base + '&g=w')), ['2024-02-26'])
        self.assertEqual(self.dates(self.get(base + '&g=m')), ['2024-02-01'])

    def test_week_starts_on_monday_and_boundaries_cross_months_and_years(self):
        # Sunday 2023-12-31 belongs to the week of Monday 2023-12-25; Monday 2024-01-01 starts a new week.
        for day in ('2023-12-31', '2024-01-01'):
            self.insert(day, 'alpha', 1, 1)
        base = '?r=custom&sd=2023-12-01&ed=2024-01-31'
        weeks = self.dates(self.get(base + '&g=w'))
        self.assertEqual(weeks, ['2023-12-25', '2024-01-01'])
        self.assertTrue(all(datetime.strptime(d, '%Y-%m-%d').weekday() == 0 for d in weeks))
        self.assertEqual(self.dates(self.get(base + '&g=m')), ['2023-12-01', '2024-01-01'])

    def test_empty_table_with_a_status_row_and_missing_status_row(self):
        self.sync(today_utc() - timedelta(days=1))
        body = self.get('?r=all').get_json()
        self.assertEqual(body['series'], [])
        self.assertEqual(body['totals'], {'revenue_usd': 0.0, 'volume_usd': 0.0})
        self.assertIsNotNone(body['last_updated'])
        self.assertIsNotNone(body['data_through'])
        self.cur.execute('DELETE FROM sync_status')
        body = self.get('?r=all').get_json()
        self.assertEqual((body['series'], body['last_updated'], body['data_through']), ([], None, None))


if __name__ == '__main__':
    unittest.main()
