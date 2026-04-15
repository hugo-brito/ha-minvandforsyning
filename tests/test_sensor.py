"""Tests for MinVandforsyning sensor definitions."""
from custom_components.minvandforsyning.sensor import (
    MinvandforsyningSensor,
    SENSOR_DESCRIPTIONS,
)


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
        total = next(d for d in SENSOR_DESCRIPTIONS if d.key == "total")
        from homeassistant.components.sensor import SensorStateClass
        assert total.state_class == SensorStateClass.TOTAL_INCREASING

    def test_all_have_water_device_class(self):
        from homeassistant.components.sensor import SensorDeviceClass
        for desc in SENSOR_DESCRIPTIONS:
            assert desc.device_class == SensorDeviceClass.WATER, (
                f"Sensor '{desc.key}' should have device_class=WATER"
            )
