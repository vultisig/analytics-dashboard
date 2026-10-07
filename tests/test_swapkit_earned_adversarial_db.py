"""Adversarial database tests for the SwapKit earned migration.

These tests create and destroy a uniquely named database on the local test
PostgreSQL cluster.  They intentionally do not import the Flask application.
"""

import os
import secrets
import unittest
from decimal import Decimal
from pathlib import Path

import psycopg2
from psycopg2 import sql


ADMIN_URL = os.environ.get('TEST_DATABASE_URL', '')
ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "vultisig-analytics" / "migrations" / "create_swapkit_daily.sql"

SYNC_STATUS_DDL = """
CREATE TABLE public.sync_status (
    id SERIAL PRIMARY KEY,
    source VARCHAR(20) NOT NULL UNIQUE,
    last_synced_timestamp TIMESTAMPTZ,
    latest_data_timestamp TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT TRUE,
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
)
"""


@unittest.skipUnless(ADMIN_URL, 'TEST_DATABASE_URL is not set')
class AdversarialEarnedDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.suffix = secrets.token_hex(6)
        cls.db_name = "swapkit_adv_" + cls.suffix
        cls.role_name = "swapkit_adv_earned_" + cls.suffix
        cls.admin = psycopg2.connect(ADMIN_URL)
        cls.admin.autocommit = True
        with cls.admin.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.db_name)))
        cls.url = ADMIN_URL.rsplit("/", 1)[0] + "/" + cls.db_name
        cls.conn = psycopg2.connect(cls.url, options="-c timezone=UTC")
        cls.conn.autocommit = True
        cls.migration = MIGRATION.read_text(encoding="utf-8")
        with cls.conn.cursor() as cur:
            cur.execute(SYNC_STATUS_DDL)
            cur.execute(cls.migration)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.conn.close()
            with cls.admin.cursor() as cur:
                cur.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (cls.db_name,),
                )
                cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(cls.db_name)))
                cur.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(cls.role_name)))
        finally:
            cls.admin.close()

    def setUp(self):
        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS pg_temp.sync_status")
            cur.execute("TRUNCATE public.swapkit_daily, public.sync_status RESTART IDENTITY")

    def test_numeric_half_cent_rounding_and_declared_precision(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO swapkit_daily(date, provider, revenue_usd, volume_usd) "
                "VALUES ('2024-02-29', 'edge', %s, %s) RETURNING revenue_usd, volume_usd",
                (Decimal("1.005"), Decimal("999999999999999999.995")),
            )
            revenue, volume = cur.fetchone()
            self.assertEqual(revenue, Decimal("1.005000"))
            self.assertEqual(volume, Decimal("999999999999999999.995000"))
            cur.execute("SELECT round(revenue_usd, 2), round(volume_usd, 2) FROM swapkit_daily")
            self.assertEqual(cur.fetchone(), (Decimal("1.01"), Decimal("1000000000000000000.00")))

    def test_numeric_overflow_is_rejected_without_partial_insert(self):
        with self.assertRaises(psycopg2.errors.NumericValueOutOfRange):
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO swapkit_daily(date, provider, revenue_usd, volume_usd) "
                    "VALUES ('2026-01-01', 'too-big', %s, 1)",
                    (Decimal("100000000000000.000000"),),
                )
        with self.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM swapkit_daily")
            self.assertEqual(cur.fetchone()[0], 0)

    def test_non_finite_numeric_is_rejected(self):
        with self.assertRaises(psycopg2.errors.CheckViolation):
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.swapkit_daily"
                    "(date, provider, revenue_usd, volume_usd) VALUES ('2026-01-01', 'nan', 'NaN', 1)"
                )

    def test_migration_is_idempotent_and_preserves_rows(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO swapkit_daily(date, provider, revenue_usd, volume_usd) "
                "VALUES ('2026-01-01', 'kept', 1, 2)"
            )
            cur.execute(self.migration)
            cur.execute(self.migration)
            cur.execute("SELECT provider, revenue_usd FROM swapkit_daily")
            self.assertEqual(cur.fetchall(), [("kept", Decimal("1.000000"))])

    def test_first_failure_creates_status_without_success_timestamps(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT record_swapkit_sync(%s, %s)", (None, "AUTH_FAILED"))
            cur.execute(
                "SELECT last_synced_timestamp, latest_data_timestamp, error_count, last_error "
                "FROM public.sync_status WHERE source='swapkit-earned'"
            )
            self.assertEqual(cur.fetchone(), (None, None, 1, "AUTH_FAILED"))

    def test_failure_handles_legacy_null_error_count(self):
        """A nullable schema column must not silently defeat failure counting."""
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.sync_status(source, error_count, last_error) "
                "VALUES ('swapkit-earned', NULL, NULL)"
            )
            cur.execute("SELECT record_swapkit_sync(NULL, 'NETWORK_ERROR')")
            cur.execute(
                "SELECT error_count, last_error FROM public.sync_status WHERE source='swapkit-earned'"
            )
            self.assertEqual(cur.fetchone(), (1, "NETWORK_ERROR"))

    def test_failure_does_not_advance_prior_success_and_truncates_error(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT record_swapkit_sync('2024-02-29', NULL)")
            cur.execute(
                "SELECT last_synced_timestamp, latest_data_timestamp FROM public.sync_status "
                "WHERE source='swapkit-earned'"
            )
            before = cur.fetchone()
            cur.execute("SELECT record_swapkit_sync('2099-12-31', %s)", ("x" * 1000,))
            cur.execute(
                "SELECT last_synced_timestamp, latest_data_timestamp, error_count, last_error "
                "FROM public.sync_status WHERE source='swapkit-earned'"
            )
            synced, latest, count, error = cur.fetchone()
            self.assertEqual((synced, latest), before)
            # Updated for the stricter record_swapkit_sync: codes must match ^[A-Z_]{2,32}$ (else INVALID_CODE); success needs a closed day.
            self.assertEqual((count, error), (1, "INVALID_CODE"))

    def test_failure_at_integer_counter_limit_still_records_error(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.sync_status(source, error_count) "
                "VALUES ('swapkit-earned', 2147483647)"
            )
            cur.execute("SELECT record_swapkit_sync(NULL, 'STILL_FAILING')")
            cur.execute(
                "SELECT error_count, last_error FROM public.sync_status "
                "WHERE source='swapkit-earned'"
            )
            self.assertEqual(cur.fetchone(), (2147483647, "STILL_FAILING"))

    def test_public_cannot_execute_definer_function(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT has_function_privilege('public', "
                "'public.record_swapkit_sync(date,text)', 'EXECUTE')"
            )
            self.assertFalse(cur.fetchone()[0])

    def test_execute_only_role_cannot_hijack_with_temp_table_or_crafted_args(self):
        with self.admin.cursor() as cur:
            cur.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(self.role_name)))
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(self.role_name)))
            cur.execute(
                sql.SQL("GRANT EXECUTE ON FUNCTION record_swapkit_sync(date,text) TO {}").format(
                    sql.Identifier(self.role_name)
                )
            )
            cur.execute("INSERT INTO sync_status(source, error_count) VALUES ('thorchain', 7)")
            cur.execute("SET search_path = pg_temp, public")
            cur.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(self.role_name)))
            try:
                cur.execute(
                    "CREATE TEMP TABLE sync_status "
                    "(source text UNIQUE, last_synced_timestamp timestamptz, "
                    "latest_data_timestamp timestamptz, error_count int, last_error text, "
                    "is_active bool, updated_at timestamptz)"
                )
                cur.execute("SELECT public.record_swapkit_sync(NULL, %s)", ("x'); DELETE FROM sync_status; --",))
                cur.execute("SELECT count(*) FROM pg_temp.sync_status")
                self.assertEqual(cur.fetchone()[0], 0)
                with self.assertRaises(psycopg2.errors.InsufficientPrivilege):
                    cur.execute("SELECT * FROM public.sync_status")
            finally:
                cur.execute("RESET ROLE")
                cur.execute("RESET search_path")
                cur.execute("DROP TABLE IF EXISTS pg_temp.sync_status")
            cur.execute("SELECT error_count FROM public.sync_status WHERE source='thorchain'")
            self.assertEqual(cur.fetchone()[0], 7)
            cur.execute(
                "SELECT error_count, last_error FROM public.sync_status "
                "WHERE source='swapkit-earned'"
            )
            count, error = cur.fetchone()
            self.assertEqual(count, 1)
            # Updated for the stricter record_swapkit_sync: codes must match ^[A-Z_]{2,32}$ (else INVALID_CODE); success needs a closed day.
            self.assertEqual(error, "INVALID_CODE")

    def test_execute_only_role_cannot_report_future_success(self):
        self._ensure_execute_role()
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(self.role_name)))
            try:
                with self.assertRaises(psycopg2.errors.InvalidParameterValue):  # changed from CheckViolation: explicit closed-day check
                    cur.execute("SELECT public.record_swapkit_sync('9999-12-31', NULL)")
            finally:
                cur.execute("RESET ROLE")

    def test_execute_only_role_cannot_report_null_date_as_success(self):
        self._ensure_execute_role()
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(self.role_name)))
            try:
                with self.assertRaises(psycopg2.errors.InvalidParameterValue):  # changed from NotNullViolation: explicit closed-day check
                    cur.execute("SELECT public.record_swapkit_sync(NULL, NULL)")
            finally:
                cur.execute("RESET ROLE")

    def _ensure_execute_role(self):
        with self.admin.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_roles WHERE rolname=%s", (self.role_name,)
            )
            if cur.fetchone() is None:
                cur.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(self.role_name)))
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(self.role_name)))
            cur.execute(
                sql.SQL("GRANT EXECUTE ON FUNCTION record_swapkit_sync(date,text) TO {}").format(
                    sql.Identifier(self.role_name)
                )
            )

    def test_function_has_pinned_search_path_and_expected_owner(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT p.prosecdef, p.proconfig, r.rolname "
                "FROM pg_proc p JOIN pg_roles r ON r.oid=p.proowner "
                "WHERE p.oid='public.record_swapkit_sync(date,text)'::regprocedure"
            )
            security_definer, config, owner = cur.fetchone()
            self.assertTrue(security_definer)
            self.assertIn("search_path=public, pg_temp", config)
            self.assertEqual(owner, "postgres")


if __name__ == "__main__":
    unittest.main()
