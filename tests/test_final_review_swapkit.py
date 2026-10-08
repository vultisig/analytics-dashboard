"""Additional boundary attacks; all writes use a disposable database."""
import os
import unittest
import uuid
from pathlib import Path

URL = os.environ.get('TEST_DATABASE_URL')
ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(URL, 'TEST_DATABASE_URL is not set')
class FinalReviewSwapkit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg2
        from psycopg2 import sql
        cls.pg = psycopg2
        cls.name = 'swapkit_final_' + uuid.uuid4().hex[:12]
        cls.admin = psycopg2.connect(URL)
        cls.admin.autocommit = True
        cls.addClassCleanup(cls.admin.close)
        with cls.admin.cursor() as cur:
            cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.name)))
        def drop():
            with cls.admin.cursor() as cur:
                cur.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(cls.name)))
        cls.addClassCleanup(drop)
        cls.conn = psycopg2.connect(URL.rsplit('/', 1)[0] + '/' + cls.name)
        cls.conn.autocommit = True
        cls.addClassCleanup(cls.conn.close)
        with cls.conn.cursor() as cur:
            cur.execute((ROOT / 'vultisig-analytics/ingestors/database_schema.sql').read_text())

    def query(self, query, params=None):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else None

    def setUp(self):
        self.query('TRUNCATE public.swapkit_daily, public.sync_status')
        self.query("SET TIME ZONE 'UTC'")

    def test_error_code_boundaries_and_control_characters(self):
        for code, expected in [('A', 'INVALID_CODE'), ('AB', 'AB'), ('A' * 32, 'A' * 32),
                               ('A' * 33, 'INVALID_CODE'), ('AB\n', 'INVALID_CODE'),
                               ('AB\r', 'INVALID_CODE'), ('AB\t', 'INVALID_CODE'),
                               ('ＡＢ', 'INVALID_CODE')]:
            with self.subTest(code=repr(code)):
                self.query('SELECT record_swapkit_sync(NULL, %s)', (code,))
                self.assertEqual(self.query('SELECT last_error FROM sync_status')[0][0], expected)

    def test_non_finite_numeric_in_both_columns(self):
        for column in ('revenue_usd', 'volume_usd'):
            for value in ('Infinity', '-Infinity', 'NaN'):
                with self.subTest(column=column, value=value):
                    with self.assertRaises(self.pg.Error):
                        self.query('INSERT INTO swapkit_daily(date,provider,revenue_usd,volume_usd) '
                                   'VALUES (%s,%s,%s,%s)',
                                   ('2024-01-01', 'alpha', value if column == 'revenue_usd' else '1',
                                    value if column == 'volume_usd' else '1'))
        self.assertEqual(self.query('SELECT count(*) FROM swapkit_daily')[0][0], 0)

    def test_success_uses_utc_under_extreme_session_timezones(self):
        for zone in ('Pacific/Kiritimati', 'Etc/GMT+12'):
            with self.subTest(zone=zone):
                self.query('SET TIME ZONE %s', (zone,))
                day = self.query("SELECT (now() AT TIME ZONE 'UTC')::date - 1")[0][0]
                self.query('SELECT record_swapkit_sync(%s)', (day,))
                self.assertEqual(self.query("SELECT latest_data_timestamp AT TIME ZONE 'UTC' FROM sync_status")[0][0].date(), day)
                with self.assertRaises(self.pg.errors.InvalidParameterValue):
                    self.query("SELECT record_swapkit_sync((now() AT TIME ZONE 'UTC')::date)")

    def test_invalid_success_preserves_entire_previous_status(self):
        self.query("SELECT record_swapkit_sync('2024-01-01')")
        self.query("SELECT record_swapkit_sync(NULL, 'FAILED')")
        before = self.query('SELECT * FROM sync_status')
        with self.assertRaises(self.pg.errors.InvalidParameterValue):
            self.query("SELECT record_swapkit_sync('9999-12-31')")
        self.assertEqual(self.query('SELECT * FROM sync_status'), before)

    def test_data_and_success_can_be_rolled_back_together(self):
        self.query('BEGIN')
        try:
            self.query("INSERT INTO swapkit_daily(date,provider,revenue_usd,volume_usd) VALUES ('2024-01-01','alpha',1,2)")
            self.query("SELECT record_swapkit_sync('2024-01-01')")
        finally:
            self.query('ROLLBACK')
        self.assertEqual(self.query('SELECT count(*) FROM swapkit_daily')[0][0], 0)
        self.assertEqual(self.query('SELECT count(*) FROM sync_status')[0][0], 0)

    def test_caller_schema_cannot_redirect_status_write(self):
        self.query('CREATE SCHEMA shadow')
        try:
            self.query('CREATE TABLE shadow.sync_status (LIKE public.sync_status INCLUDING ALL)')
            self.query('SET search_path = shadow, public')
            self.query("SELECT public.record_swapkit_sync(NULL, 'FAILED')")
            self.assertEqual(self.query('SELECT count(*) FROM shadow.sync_status')[0][0], 0)
            self.assertEqual(self.query('SELECT error_count FROM public.sync_status')[0][0], 1)
        finally:
            self.query('SET search_path = public')
            self.query('DROP SCHEMA shadow CASCADE')
