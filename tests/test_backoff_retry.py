# tests/test_backoff_retry.py
"""
Unit tests for ingestors/base.py BackoffRetry.

Run from the repo root with:
    python3 -m unittest discover -s tests -p 'test_*.py' -v
"""
import os
import sys
import unittest
from unittest.mock import patch

_VA = os.path.join(os.path.dirname(__file__), "..", "vultisig-analytics")
if os.path.isdir(_VA) and _VA not in sys.path:
    sys.path.insert(0, _VA)

from ingestors.base import BackoffRetry  # noqa: E402


class BackoffRetryTests(unittest.TestCase):
    def test_returns_immediately_when_fn_succeeds(self):
        retry = BackoffRetry(max_retries=3, initial_backoff=0.1)
        calls = []

        def fn():
            calls.append(1)
            return "ok"

        with patch("ingestors.base.time.sleep") as mock_sleep:
            result = retry.retry(fn)

        self.assertEqual(result, "ok")
        self.assertEqual(len(calls), 1)
        mock_sleep.assert_not_called()

    def test_retries_until_success(self):
        retry = BackoffRetry(max_retries=5, initial_backoff=1.0)
        attempts = {"n": 0}

        def fn():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise ValueError("transient")
            return "done"

        with patch("ingestors.base.time.sleep") as mock_sleep:
            result = retry.retry(fn)

        self.assertEqual(result, "done")
        self.assertEqual(attempts["n"], 3)
        # Two sleeps: after attempt 1 (1.0 * 1 = 1.0) and attempt 2 (1.0 * 2 = 2.0)
        self.assertEqual(mock_sleep.call_count, 2)
        self.assertEqual(
            [c.args[0] for c in mock_sleep.call_args_list],
            [1.0, 2.0],
        )

    def test_raises_after_max_retries(self):
        retry = BackoffRetry(max_retries=3, initial_backoff=1.0)
        calls = []

        def fn():
            calls.append(1)
            raise RuntimeError("boom")

        with patch("ingestors.base.time.sleep") as mock_sleep:
            with self.assertRaises(Exception) as ctx:
                retry.retry(fn)

        self.assertEqual(len(calls), 3)
        self.assertIn("Max retries reached", str(ctx.exception))
        self.assertIn("boom", str(ctx.exception))
        # Sleeps only happen between attempts, not after the last one.
        self.assertEqual(mock_sleep.call_count, 2)

    def test_additive_backoff_progression(self):
        # backoff_duration starts at initial_backoff and is multiplied by
        # the attempt number on each failure:
        #   after attempt 1: 2.0 * 1 = 2.0
        #   after attempt 2: 2.0 * 2 = 4.0
        #   after attempt 3: 4.0 * 3 = 12.0
        retry = BackoffRetry(max_retries=4, initial_backoff=2.0)

        def fn():
            raise ValueError("nope")

        with patch("ingestors.base.time.sleep") as mock_sleep:
            with self.assertRaises(Exception):
                retry.retry(fn)

        self.assertEqual(
            [c.args[0] for c in mock_sleep.call_args_list],
            [2.0, 4.0, 12.0],
        )

    def test_max_retries_of_one_does_not_sleep(self):
        retry = BackoffRetry(max_retries=1, initial_backoff=1.0)

        def fn():
            raise ValueError("first and last")

        with patch("ingestors.base.time.sleep") as mock_sleep:
            with self.assertRaises(Exception) as ctx:
                retry.retry(fn)

        mock_sleep.assert_not_called()
        self.assertIn("first and last", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
