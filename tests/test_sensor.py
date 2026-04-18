"""Tests for MinVandforsyning sensor definitions."""
from homeassistant.components.sensor import SensorDeviceClass, SensorStateClass

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
