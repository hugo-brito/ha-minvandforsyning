"""Contract tests for the parser using a fully synthetic fixture.

The fixture is generated from hand-authored parameters (see the fixture
generator and docs/testing-fixtures.md); no value is derived from a real
household. It is two short contiguous UTC hourly windows, one bracketing the
autumn fall-back (2025-10-26) and one the spring-forward (2026-03-29), so both
DST directions are exercised.

These tests enforce the documented contract; they do not prove the supplier's
real behaviour (that was established by live API probing for issue #9):
1. Consumption(T) == (Reading(T) - Reading(T-1)) * 1000 for all rows -> end-of-interval.
2. Spring-forward 2026-03-29 has 24 UTC rows including 02:00, nonexistent in
   Europe/Copenhagen local time -> the timestamps are UTC, not local.
3. UTC->local gives a 23-hour local day in spring and a 25-hour local day in autumn.
4. Winter/summer bucket math (inline) shows why the old local-time reading was correct
   in winter but an hour early in summer.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

BASE = Path(__file__).resolve().parents[1] / "custom_components" / "minvandforsyning"
FIXTURE_BIN = Path(__file__).resolve().parent / "fixtures" / "meter_data.bin"
DK = ZoneInfo("Europe/Copenhagen")


def _load(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, BASE / path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


if "custom_components" not in sys.modules:
    pkg = types.ModuleType("custom_components")
    pkg.__path__ = [str(BASE.parent)]
    sys.modules["custom_components"] = pkg

if "custom_components.minvandforsyning" not in sys.modules:
    pkg = types.ModuleType("custom_components.minvandforsyning")
    pkg.__path__ = [str(BASE)]
    sys.modules["custom_components.minvandforsyning"] = pkg

_load("custom_components.minvandforsyning.const", "const.py")
protobuf_parser = _load("custom_components.minvandforsyning.protobuf_parser", "protobuf_parser.py")
parse_dataset = protobuf_parser.parse_dataset


@pytest.fixture(scope="module")
def readings_table():
    """Parse the binary fixture and return table index 6."""
    if not FIXTURE_BIN.exists():
        pytest.skip(f"Missing test fixture: {FIXTURE_BIN}")
    with open(FIXTURE_BIN, "rb") as f:
        tables = parse_dataset(f.read())
    return tables[6]


class TestReadingDateIsUtc:
    def test_consumption_equals_reading_diff(self, readings_table):
        """Consumption(T) == (Reading(T) - Reading(T-1)) * 1000 for every row."""
        rows = readings_table.rows
        mismatches = []
        for i in range(1, len(rows)):
            prev = Decimal(str(rows[i - 1]["Reading"]))
            curr = Decimal(str(rows[i]["Reading"]))
            cons = Decimal(str(rows[i]["Consumption"]))
            diff = (curr - prev) * 1000
            if abs(diff - cons) > Decimal("0.5"):
                mismatches.append((i, rows[i]["ReadingDate"], diff, cons))
        assert mismatches == [], f"Reading diff != Consumption: {mismatches[:5]}"

    def test_spring_forward_day_has_24_hourly_rows(self, readings_table):
        """2026-03-29 has 24 rows including T=02:00 — valid UTC, nonexistent in Copenhagen local time."""
        rows = [r for r in readings_table.rows if r["ReadingDate"].date() == date(2026, 3, 29)]
        assert len(rows) == 24
        hours = {r["ReadingDate"].hour for r in rows}
        assert 2 in hours, "02:00 UTC must exist on spring-forward day"

    def test_spring_forward_local_day_has_23_hours(self, readings_table):
        """UTC->local on the spring-forward day yields a 23-hour local day."""
        n = sum(
            1 for r in readings_table.rows
            if r["ReadingDate"].replace(tzinfo=timezone.utc).astimezone(DK).date() == date(2026, 3, 29)
        )
        assert n == 23

    def test_autumn_fallback_local_day_has_25_hours(self, readings_table):
        """UTC->local on the autumn fall-back day yields a 25-hour local day, the
        mirror of spring, confirming UTC interpretation in both directions."""
        n = sum(
            1 for r in readings_table.rows
            if r["ReadingDate"].replace(tzinfo=timezone.utc).astimezone(DK).date() == date(2025, 10, 26)
        )
        assert n == 25

    def test_within_window_readings_are_one_hour_apart(self, readings_table):
        """The fixture is two contiguous hourly windows (one per DST transition).
        Every step is exactly 3600 s except the single intentional gap between
        the two windows."""
        rows = readings_table.rows
        gaps = [
            (rows[i]["ReadingDate"] - rows[i - 1]["ReadingDate"]).total_seconds()
            for i in range(1, len(rows))
        ]
        off_grid = [g for g in gaps if g != 3600]
        assert len(off_grid) == 1, f"expected one window boundary, got {off_grid}"

    def test_winter_utc_bucket_matches_local_conversion(self):
        """In winter (CET=UTC+1), UTC-1h gives same result as local-to-UTC conversion."""
        naive_reading = datetime(2026, 1, 15, 9, 0, 0)
        utc_bucket = naive_reading.replace(tzinfo=timezone.utc) - timedelta(hours=1)
        local_bucket = naive_reading.replace(tzinfo=DK).astimezone(timezone.utc)
        assert utc_bucket == local_bucket == datetime(2026, 1, 15, 8, 0, 0, tzinfo=timezone.utc)

    def test_summer_utc_bucket_differs_from_local_conversion(self):
        """In summer (CEST=UTC+2), UTC-1h and local-to-UTC give DIFFERENT buckets."""
        naive_reading = datetime(2026, 6, 5, 9, 0, 0)
        utc_bucket = naive_reading.replace(tzinfo=timezone.utc) - timedelta(hours=1)
        local_bucket = naive_reading.replace(tzinfo=DK).astimezone(timezone.utc)
        assert utc_bucket == datetime(2026, 6, 5, 8, 0, 0, tzinfo=timezone.utc)
        assert local_bucket == datetime(2026, 6, 5, 7, 0, 0, tzinfo=timezone.utc)
        assert utc_bucket != local_bucket
