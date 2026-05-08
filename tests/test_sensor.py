"""Tests for MinVandforsyning sensor definitions."""
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

from homeassistant.components.sensor import SensorDeviceClass, SensorStateClass
from homeassistant.const import UnitOfVolume

from custom_components.minvandforsyning.coordinator import MeterReading, MinvandforsyningData
from custom_components.minvandforsyning.sensor import (
    MinvandforsyningSensor,
    SENSOR_DESCRIPTIONS,
)


def _desc(key: str):
    """Return the sensor description matching *key*."""
    return next(d for d in SENSOR_DESCRIPTIONS if d.key == key)


class TestSensorDescriptions:
    def test_has_total_sensor(self):
        keys = [d.key for d in SENSOR_DESCRIPTIONS]
        assert "total" in keys

    def test_has_hourly_sensor(self):
        keys = [d.key for d in SENSOR_DESCRIPTIONS]
        assert "hourly" in keys

    def test_has_daily_sensor(self):
        keys = [d.key for d in SENSOR_DESCRIPTIONS]
        assert "daily" in keys

    def test_total_is_total_increasing(self):
        total = _desc("total")
        assert total.state_class == SensorStateClass.TOTAL_INCREASING

    def test_total_has_water_device_class(self):
        total = _desc("total")
        assert total.device_class == SensorDeviceClass.WATER

    def test_hourly_has_no_device_class(self):
        """Hourly consumption is instantaneous; device_class=WATER requires
        total_increasing or total state_class, so it must be None."""
        hourly = _desc("hourly")
        assert hourly.device_class is None
        assert hourly.state_class == SensorStateClass.MEASUREMENT

    def test_daily_has_no_device_class(self):
        """Daily consumption is instantaneous; same constraint as hourly."""
        daily = _desc("daily")
        assert daily.device_class is None
        assert daily.state_class == SensorStateClass.MEASUREMENT


class TestUnitContract:
    """Pin the (device_class, state_class, unit) triplet per sensor key.

    This triplet is a backward-compatibility contract with HA's long-term
    statistics recorder.  Changing any of these for an existing sensor key
    corrupts every existing install's LTS and produces the ``units_changed``
    errors that block the Energy dashboard.

    If this test needs to change, bump the sensor's ``key`` to force HA to
    treat it as a new entity.
    """

    EXPECTED = {
        "total": (
            SensorDeviceClass.WATER,
            SensorStateClass.TOTAL_INCREASING,
            UnitOfVolume.CUBIC_METERS,
        ),
        "hourly": (
            None,
            SensorStateClass.MEASUREMENT,
            UnitOfVolume.LITERS,
        ),
        "daily": (
            None,
            SensorStateClass.MEASUREMENT,
            UnitOfVolume.LITERS,
        ),
    }

    def test_all_triplets_frozen(self):
        actual = {
            d.key: (d.device_class, d.state_class, d.native_unit_of_measurement)
            for d in SENSOR_DESCRIPTIONS
        }
        assert actual == self.EXPECTED


class TestSensorAvailability:
    """Entity must report unavailable rather than None to keep LTS clean."""

    def _make_sensor(self, key: str, data: MinvandforsyningData | None):
        coordinator = MagicMock()
        coordinator.data = data
        coordinator.last_update_success = True
        # CoordinatorEntity.available checks last_update_success
        entry = MagicMock()
        entry.data = {"meter_number": "12345"}
        return MinvandforsyningSensor(coordinator, _desc(key), entry)

    def _reading(self, total: str, consumption: str) -> MeterReading:
        return MeterReading(
            date=datetime.now(),
            reading=Decimal(total),
            consumption=Decimal(consumption),
        )

    def test_unavailable_when_coordinator_data_is_none(self):
        sensor = self._make_sensor("total", None)
        assert sensor.available is False

    def test_unavailable_when_readings_empty(self):
        """Empty readings list = total_m3 returns None = entity unavailable."""
        sensor = self._make_sensor("total", MinvandforsyningData([]))
        assert sensor.available is False

    def test_unavailable_when_last_update_failed(self):
        sensor = self._make_sensor(
            "total",
            MinvandforsyningData([self._reading("300.000", "10")]),
        )
        sensor.coordinator.last_update_success = False
        assert sensor.available is False

    def test_available_when_data_present(self):
        sensor = self._make_sensor(
            "total",
            MinvandforsyningData([self._reading("300.000", "10")]),
        )
        assert sensor.available is True

    def test_hourly_unavailable_when_no_readings(self):
        sensor = self._make_sensor("hourly", MinvandforsyningData([]))
        assert sensor.available is False

    def test_hourly_available_with_readings(self):
        sensor = self._make_sensor(
            "hourly",
            MinvandforsyningData([self._reading("300.000", "10")]),
        )
        assert sensor.available is True

    def test_daily_available_returns_zero_is_still_available(self):
        """daily_liters() sums to Decimal(0) for no-match dates - that is a
        legitimate value (0 L consumed), not "no data". The entity must be
        available so HA can record a genuine 0 into statistics."""
        # Reading from a different day - today's sum will be Decimal(0)
        sensor = self._make_sensor(
            "daily",
            MinvandforsyningData([
                MeterReading(
                    date=datetime(2020, 1, 1),
                    reading=Decimal("300.000"),
                    consumption=Decimal("10"),
                )
            ]),
        )
        assert sensor.available is True


class TestExtraStateAttributes:
    """Every sensor must expose ``last_reading_date`` so users can see how
    fresh the supplier's published data is without enabling debug logs.

    Some suppliers publish meter data only once per 24h (see issue #2);
    surfacing the timestamp lets users self-diagnose "stale data" reports.
    """

    def _make_sensor(self, key: str, data: MinvandforsyningData | None):
        coordinator = MagicMock()
        coordinator.data = data
        coordinator.last_update_success = True
        entry = MagicMock()
        entry.data = {"meter_number": "12345"}
        return MinvandforsyningSensor(coordinator, _desc(key), entry)

    def _reading(self, total: str, consumption: str, date: datetime | None = None) -> MeterReading:
        return MeterReading(
            date=date or datetime(2026, 5, 8, 12, 0, 0),
            reading=Decimal(total),
            consumption=Decimal(consumption),
        )

    def test_attrs_are_none_when_no_coordinator_data(self):
        for key in ("total", "hourly", "daily"):
            sensor = self._make_sensor(key, None)
            assert sensor.extra_state_attributes is None, key

    def test_total_exposes_last_reading_date_and_count(self):
        sensor = self._make_sensor(
            "total",
            MinvandforsyningData([self._reading("300.000", "10")]),
        )
        attrs = sensor.extra_state_attributes
        assert attrs == {
            "last_reading_date": "2026-05-08T12:00:00",
            "readings_count": 1,
        }

    def test_hourly_exposes_last_reading_date(self):
        sensor = self._make_sensor(
            "hourly",
            MinvandforsyningData([self._reading("300.000", "10")]),
        )
        assert sensor.extra_state_attributes == {
            "last_reading_date": "2026-05-08T12:00:00",
        }

    def test_daily_exposes_last_reading_date(self):
        sensor = self._make_sensor(
            "daily",
            MinvandforsyningData([self._reading("300.000", "10")]),
        )
        assert sensor.extra_state_attributes == {
            "last_reading_date": "2026-05-08T12:00:00",
        }

    def test_last_reading_date_is_none_when_readings_empty(self):
        for key in ("total", "hourly", "daily"):
            sensor = self._make_sensor(key, MinvandforsyningData([]))
            attrs = sensor.extra_state_attributes
            assert attrs is not None, key
            assert attrs["last_reading_date"] is None, key
