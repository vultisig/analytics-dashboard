#!/usr/bin/env python3
"""Unit tests for iso_utc — the API's single timestamp serializer.

Runs offline (no server, no DB): python tests/test_iso_utc.py
"""
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'vultisig-analytics'))

# api_server imports flask at module level; import just the helper's source instead
# of the app so the test needs no dependencies.
_src = (Path(__file__).resolve().parent.parent / 'vultisig-analytics' / 'api_server.py').read_text()
_match = re.search(r'def iso_utc\(value\):\n(?:    .*\n|\n)*?    return value\.isoformat\(\) if value is not None else None\n', _src)
assert _match, 'iso_utc not found in api_server.py'
_ns = {'datetime': datetime, 'timezone': timezone}
exec(_match.group(0), _ns)
iso_utc = _ns['iso_utc']

OFFSET_SUFFIX = re.compile(r'[+-]\d{2}:\d{2}$')


def test_aware_serializes_with_offset():
    assert iso_utc(datetime(2026, 8, 10, tzinfo=timezone.utc)) == '2026-08-10T00:00:00+00:00'


def test_none_passes_through():
    assert iso_utc(None) is None


def test_date_passes_through():
    assert iso_utc(date(2026, 8, 10)) == '2026-08-10'


def test_aware_output_carries_offset():
    # Naive datetimes are a schema violation since the TIMESTAMPTZ migration;
    # the integration suite's response walker is the guard that catches them.
    value = iso_utc(datetime(2026, 8, 10, 14, 30, tzinfo=timezone.utc))
    assert OFFSET_SUFFIX.search(value), f'no offset on {value}'


if __name__ == '__main__':
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            try:
                fn()
                print(f'PASS {name}')
            except AssertionError as e:
                failures += 1
                print(f'FAIL {name}: {e}')
    sys.exit(1 if failures else 0)
