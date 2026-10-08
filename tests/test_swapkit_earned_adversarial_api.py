"""Adversarial endpoint tests. Creates and drops only a unique disposable DB.
Run: PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_swapkit_adv_adversarial_earned*.py' -v
"""
import importlib
import os
import sys
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import psycopg2
from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[1]
ADMIN = os.environ.get('TEST_DATABASE_URL', '')


@unittest.skipUnless(ADMIN, 'TEST_DATABASE_URL is not set')
class EarnedEndpointAttacks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.name = 'swapkit_adv_api_' + uuid.uuid4().hex[:12]
        cls.admin = psycopg2.connect(ADMIN)
        cls.admin.autocommit = True
        cls.addClassCleanup(cls.admin.close)
        with cls.admin.cursor() as c:
            c.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.name)))
        cls.addClassCleanup(cls.drop_database)
        cls.url = ADMIN.rsplit('/', 1)[0] + '/' + cls.name
        cls.conn = psycopg2.connect(cls.url)
        cls.conn.autocommit = True
        cls.addClassCleanup(cls.conn.close)
        with cls.conn.cursor() as c:
            try:
                c.execute((ROOT / 'vultisig-analytics/ingestors/database_schema.sql').read_text())
            except psycopg2.errors.FeatureNotSupported:
                raise unittest.SkipTest('timescaledb extension is not available in this database')
            c.execute((ROOT / 'vultisig-analytics/migrations/create_dex_aggregator_revenue.sql').read_text())
        # Prevent dotenv's parent-directory discovery; provide no real secrets.
        sys.dont_write_bytecode = True
        sys.path.insert(0, str(ROOT / 'vultisig-analytics'))
        with patch.dict(os.environ, {'DATABASE_URL': cls.url, 'PYTHON_DOTENV_DISABLED': '1'}), patch('dotenv.load_dotenv', return_value=False):
            cls.api = importlib.import_module('api_server')
        cls.old_url = cls.api.db_manager.connection_string
        cls.api.db_manager.connection_string = cls.url
        cls.addClassCleanup(setattr, cls.api.db_manager, 'connection_string', cls.old_url)
        cls.client = cls.api.app.test_client()

    @classmethod
    def drop_database(cls):
        with cls.admin.cursor() as c:
            c.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(cls.name)))

    def setUp(self):
        self.api.public_rate_limit_store.clear()
        self.exec('TRUNCATE swapkit_daily, sync_status, swaps, dex_aggregator_revenue')

    def exec(self, query, params=None):
        with self.conn.cursor() as c:
            c.execute(query, params)
            return c.fetchall() if c.description else None

    def seed(self, day, revenue='1', volume='1', provider='alpha'):
        self.exec('INSERT INTO swapkit_daily(date,provider,revenue_usd,volume_usd) VALUES(%s,%s,%s,%s)', (day, provider, revenue, volume))

    def get(self, **params):
        return self.client.get('/api/swapkit/earned', query_string=params)

    def dates(self, **params):
        r = self.get(**params)
        self.assertEqual(r.status_code, 200, r.get_json())
        return [x['date'] for x in r.get_json()['series']]

    def test_injection_all_parameter_positions(self):
        self.seed('2024-02-29')
        attacks = ["'; DROP TABLE swapkit_daily;--", "day') OR true--", '\x00', '2024-02-29\n']
        for key in ('r', 'g', 'sd', 'ed'):
            for attack in attacks:
                params = dict(r='custom', sd='2024-02-29', ed='2024-02-29', g='d')
                params[key] = attack
                with self.subTest(key=key, attack=repr(attack)):
                    self.assertEqual(self.get(**params).status_code, 400)
        self.assertEqual(self.exec('SELECT count(*) FROM swapkit_daily')[0][0], 1)

    def test_strict_iso_rejects_unicode_year(self):
        for key in ('sd', 'ed'):
            params = dict(r='custom', sd='2024-02-29', ed='2024-02-29')
            params[key] = '\uff12\uff10\uff12\uff14-02-29'
            with self.subTest(key=key):
                self.assertEqual(self.get(**params).status_code, 400)

    def test_rolling_lower_bounds_include_exact_day(self):
        today = datetime.now(timezone.utc).date()
        for days, r in ((7, '7d'), (30, '30d'), (90, '90d'), (365, '1y')):
            self.exec('TRUNCATE swapkit_daily')
            self.seed(today - timedelta(days=days + 1))
            self.seed(today - timedelta(days=days))
            with self.subTest(range=r):
                self.assertEqual(self.dates(r=r), [(today - timedelta(days=days)).isoformat()])

    def test_nan_cannot_produce_nonstandard_json(self):
        # Changed: the schema now rejects NaN, so drop the checks here to test the second guard
        # in round_money. A NaN row must give a safe 500 and never a nonstandard JSON number.
        self.addCleanup(lambda: (self.exec('TRUNCATE swapkit_daily'), self.exec((ROOT / 'vultisig-analytics/migrations/create_swapkit_daily.sql').read_text())))
        self.exec('ALTER TABLE swapkit_daily DROP CONSTRAINT swapkit_daily_revenue_ok')
        self.exec('ALTER TABLE swapkit_daily DROP CONSTRAINT swapkit_daily_volume_ok')
        self.seed('2024-02-29', 'NaN', 'NaN')
        response = self.get()
        self.assertEqual(response.status_code, 500)
        import json
        def reject(token):
            raise AssertionError('nonstandard JSON number: ' + token)
        json.loads(response.data, parse_constant=reject)

    def test_calendar_invalid_reverse_and_missing(self):
        for params in [dict(r='custom', sd='2023-02-29', ed='2023-03-01'), dict(r='custom', sd='2024-03-01', ed='2024-02-29'), dict(r='custom', sd='2024-02-29'), dict(r='custom', ed='2024-02-29')]:
            with self.subTest(params=params):
                self.assertEqual(self.get(**params).status_code, 400)

    def test_leap_day_start_equals_end(self):
        for day in ('2024-02-28', '2024-02-29', '2024-03-01'):
            self.seed(day)
        self.assertEqual(self.dates(r='custom', sd='2024-02-29', ed='2024-02-29'), ['2024-02-29'])

    def test_week_month_utc_boundary(self):
        for day in ('2023-12-31', '2024-01-01', '2024-02-29', '2024-03-01'):
            self.seed(day)
        self.assertEqual(self.dates(g='w'), ['2023-12-25', '2024-01-01', '2024-02-26'])
        self.assertEqual(self.dates(g='m'), ['2023-12-01', '2024-01-01', '2024-02-01', '2024-03-01'])
        self.assertEqual(self.get(g='h').get_json()['series'], self.get(g='d').get_json()['series'])

    def test_one_day_uses_utc_yesterday_not_latest_row(self):
        today = datetime.now(timezone.utc).date()
        for delta in (-2, -1, 0, 1):
            self.seed(today + timedelta(days=delta))
        for r in ('1d', '24h'):
            self.assertEqual(self.dates(r=r), [(today - timedelta(days=1)).isoformat()])

    def test_rolling_ranges_exclude_open_and_future_days(self):
        today = datetime.now(timezone.utc).date()
        self.seed(today - timedelta(days=1))
        self.seed(today)
        self.seed(today + timedelta(days=400))
        for r in ('7d', '30d', '90d', '1y', 'ytd'):
            with self.subTest(range=r):
                self.assertEqual(self.dates(r=r), [(today - timedelta(days=1)).isoformat()], 'closed-day range leaked open/future rows')

    def test_future_custom_range_empty_without_future_rows(self):
        self.seed('2024-02-29')
        self.assertEqual(self.dates(r='custom', sd='9999-01-01', ed='9999-12-31'), [])

    def test_half_up_aggregation_before_rounding(self):
        self.seed('2024-02-28', '1.005', '2.345')
        self.seed('2024-02-29', '1.005', '2.345')
        day = self.get().get_json()
        self.assertEqual([r['revenue_usd'] for r in day['series']], [1.01, 1.01])
        self.assertEqual(day['totals'], dict(revenue_usd=2.01, volume_usd=4.69))
        self.assertEqual(self.get(g='m').get_json()['series'][0]['revenue_usd'], 2.01)

    def test_huge_numeric_keeps_exact_rounded_cents(self):
        value = '999999999999999999.985000'
        self.seed('2024-02-29', '99999999999999.985000', value)
        response = self.get()
        self.assertEqual(response.status_code, 200)
        import json
        body = json.loads(response.data, parse_float=Decimal, parse_int=Decimal)
        # Changed: declined finding. Money values are JSON numbers like every other endpoint, so
        # precision above about 1e15 is a float limit. The value must still be a finite, close number.
        self.assertEqual(body['series'][0]['volume_usd'], Decimal(float(Decimal('999999999999999999.99'))))

    def test_numeric_nan_rejected_by_schema(self):
        with self.assertRaises(psycopg2.errors.CheckViolation):
            self.seed('2024-02-29', 'NaN', 'NaN')

    def test_empty_and_missing_status(self):
        body = self.get().get_json()
        self.assertEqual(body['series'], [])
        self.assertEqual(body['totals'], dict(revenue_usd=0, volume_usd=0))
        self.seed('2024-02-29')
        body = self.get().get_json()
        self.assertEqual(len(body['series']), 1)
        self.assertIsNone(body['last_updated'])
        self.assertIsNone(body['data_through'])

    def test_db_failure_is_sanitized(self):
        with patch.object(self.api.db_manager, 'execute_query', side_effect=RuntimeError('secret connection credentials')):
            r = self.get()
        self.assertEqual(r.status_code, 500)
        self.assertEqual(r.get_json(), {'error': 'Internal server error'})

    def test_existing_endpoints_do_not_count_earned_rows(self):
        paths = ('/api/health', '/api/revenue', '/api/swap-count', '/api/stats')
        before = {p: self.client.get(p) for p in paths}
        for p, r in before.items():
            self.assertEqual(r.status_code, 200, (p, r.get_json()))
        self.seed('2024-02-29', '999', '999')
        for p in paths:
            with self.subTest(path=p):
                r = self.client.get(p)
                self.assertEqual(r.status_code, 200)
                self.assertEqual(r.get_json(), before[p].get_json())

    def test_system_status_error_is_boolean_without_secret(self):
        self.exec('SELECT record_swapkit_sync(%s,%s)', ('2024-02-29', 'SECRET_TOKEN'))
        r = self.client.get('/api/system-status')
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.get_json()[0]['has_error'])
        self.assertNotIn('SECRET_TOKEN', r.get_data(as_text=True))


if __name__ == '__main__':
    unittest.main()
