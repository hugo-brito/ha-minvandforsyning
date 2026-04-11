"""Tests for the MinvandforsyningData coordinator data class."""
from datetime import datetime
from decimal import Decimal

import pytest

from custom_components.minvandforsyning.coordinator import MeterReading, MinvandforsyningData


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
