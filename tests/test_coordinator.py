"""Tests for the MinvandforsyningData coordinator data class."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
            hass, client, "23148103", 15,
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
