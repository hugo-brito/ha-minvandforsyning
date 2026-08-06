"""Tests for the MinvandforsyningData coordinator data class."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from custom_components.minvandforsyning.const import READING_DATE_TZ
from custom_components.minvandforsyning.coordinator import MeterReading, MinvandforsyningCoordinator, MinvandforsyningData


def _reading(year: int, month: int, day: int, hour: int, total: str, consumption: str) -> MeterReading:
    return MeterReading(
        date=datetime(year, month, day, hour),
        reading=Decimal(total),
        consumption=Decimal(consumption),
    )


@pytest.fixture
def sample_data() -> MinvandforsyningData:
    return MinvandforsyningData([
        _reading(2026, 4, 11, 0, "300.000", "10"),
        _reading(2026, 4, 11, 1, "300.010", "10"),
        _reading(2026, 4, 11, 2, "300.030", "20"),
        _reading(2026, 4, 12, 0, "300.050", "20"),
        _reading(2026, 4, 12, 1, "300.080", "30"),
    ])


class TestMinvandforsyningData:
    def test_latest_reading(self, sample_data: MinvandforsyningData):
        assert sample_data.latest_reading is not None
        assert sample_data.latest_reading.reading == Decimal("300.080")

    def test_latest_reading_empty(self):
        data = MinvandforsyningData([])
        assert data.latest_reading is None

    def test_total_m3(self, sample_data: MinvandforsyningData):
        assert sample_data.total_m3 == Decimal("300.080")

    def test_total_m3_empty(self):
        assert MinvandforsyningData([]).total_m3 is None

    def test_last_hour_liters(self, sample_data: MinvandforsyningData):
        assert sample_data.last_hour_liters == Decimal("30")

    def test_last_hour_liters_empty(self):
        assert MinvandforsyningData([]).last_hour_liters is None

    def test_daily_liters_specific_date(self, sample_data: MinvandforsyningData):
        apr11 = datetime(2026, 4, 11)
        assert sample_data.daily_liters(apr11) == Decimal("40")  # 10 + 10 + 20

    def test_daily_liters_another_date(self, sample_data: MinvandforsyningData):
        apr12 = datetime(2026, 4, 12)
        assert sample_data.daily_liters(apr12) == Decimal("50")  # 20 + 30

    def test_daily_liters_no_data_for_date(self, sample_data: MinvandforsyningData):
        jan1 = datetime(2026, 1, 1)
        assert sample_data.daily_liters(jan1) == Decimal("0")

    def test_daily_liters_empty_readings(self):
        data = MinvandforsyningData([])
        assert data.daily_liters(datetime(2026, 4, 12)) == Decimal("0")

    def test_daily_liters_summer_hour_ending_at_local_midnight_counts_previous_day(self):
        """Summer (CEST=UTC+2): the reading ending 22:00Z covers [21:00Z, 22:00Z)
        = 23:00-00:00 CEST on Jun 5, so it belongs to Jun 5, not Jun 6. This
        matches its statistics bucket (start 21:00Z, shown at 23:00 local)."""
        data = MinvandforsyningData([
            _reading(2026, 6, 5, 21, "300.000", "10"),  # [20:00Z, 21:00Z) = 22:00-23:00 CEST Jun 5
            _reading(2026, 6, 5, 22, "300.010", "20"),  # [21:00Z, 22:00Z) = 23:00-00:00 CEST Jun 5
        ])
        jun5 = datetime(2026, 6, 5, tzinfo=ZoneInfo(READING_DATE_TZ))
        jun6 = datetime(2026, 6, 6, tzinfo=ZoneInfo(READING_DATE_TZ))
        assert data.daily_liters(jun5) == Decimal("30")
        assert data.daily_liters(jun6) == Decimal("0")

    def test_daily_liters_winter_hour_ending_at_local_midnight_counts_previous_day(self):
        """Winter (CET=UTC+1): the reading ending 23:00Z covers [22:00Z, 23:00Z)
        = 23:00-00:00 CET on Jan 15, so it belongs to Jan 15, not Jan 16."""
        data = MinvandforsyningData([
            _reading(2026, 1, 15, 22, "300.000", "15"),  # [21:00Z, 22:00Z) = 22:00-23:00 CET Jan 15
            _reading(2026, 1, 15, 23, "300.015", "25"),  # [22:00Z, 23:00Z) = 23:00-00:00 CET Jan 15
        ])
        local_jan15 = datetime(2026, 1, 15, tzinfo=ZoneInfo(READING_DATE_TZ))
        local_jan16 = datetime(2026, 1, 16, tzinfo=ZoneInfo(READING_DATE_TZ))
        assert data.daily_liters(local_jan15) == Decimal("40")
        assert data.daily_liters(local_jan16) == Decimal("0")

    def test_daily_liters_returns_zero_for_missing_local_day(self):
        data = MinvandforsyningData([
            _reading(2026, 6, 5, 22, "300.000", "20"),  # interval start 21:00Z = 23:00 CEST Jun 5
        ])
        assert data.daily_liters(datetime(2026, 6, 5, tzinfo=ZoneInfo(READING_DATE_TZ))) == Decimal("20")
        assert data.daily_liters(datetime(2026, 6, 7, tzinfo=ZoneInfo(READING_DATE_TZ))) == Decimal("0")

    def test_daily_grouping_agrees_with_statistics_bucket(self):
        """The daily-sensor local day equals the local day of the statistics
        bucket start for the same reading, in both winter and summer, so the
        daily sensor and the Energy dashboard never disagree on ownership."""
        for month in (1, 6):  # winter (CET) and summer (CEST)
            reading = _reading(2026, month, 15, 22, "300.000", "10")
            bucket_local_day = (
                MinvandforsyningCoordinator.reading_start_utc(reading.date)
                .astimezone(ZoneInfo(READING_DATE_TZ))
                .date()
            )
            data = MinvandforsyningData([reading])
            local_midnight = datetime(
                bucket_local_day.year, bucket_local_day.month, bucket_local_day.day,
                tzinfo=ZoneInfo(READING_DATE_TZ),
            )
            assert data.daily_liters(local_midnight) == Decimal("10")


class TestCoordinatorDateRange:
    """Regression: DateTo must be tomorrow to include today's intraday data.

    The API treats DateTo as exclusive — passing today's date excludes all of
    today's readings.  This was the root cause of sensors showing midnight-stale
    data while the website had up-to-the-hour values.
    """

    @pytest.mark.asyncio
    async def test_date_to_is_tomorrow(self):
        """Coordinator must pass DateTo = now + 1 day, not DateTo = now."""
        hass = MagicMock()
        client = MagicMock()
        # Make async_get_meter_data capture the args it's called with
        captured_args = {}

        async def capture_meter_data(meter, supplier, date_from, date_to):
            captured_args["date_from"] = date_from
            captured_args["date_to"] = date_to
            # Return the test fixture binary
            with open("tests/fixtures/meter_data.bin", "rb") as f:
                return f.read()

        client.async_get_meter_data = AsyncMock(side_effect=capture_meter_data)

        coordinator = MinvandforsyningCoordinator(
            hass, client, "99999999", 1,
            import_statistics=False,
        )

        frozen_now = datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)
        with patch(
            "custom_components.minvandforsyning.coordinator.datetime",
        ) as mock_dt:
            mock_dt.now.return_value = frozen_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            await coordinator._async_update_data()

        assert "date_to" in captured_args
        date_to = captured_args["date_to"]
        # DateTo must be at least tomorrow relative to "now"
        assert date_to.date() > frozen_now.date(), (
            f"DateTo ({date_to.date()}) must be after today ({frozen_now.date()}) "
            "because the API treats DateTo as exclusive"
        )
