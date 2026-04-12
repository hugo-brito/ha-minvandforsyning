"""Sensor platform for MinVandforsyning."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfVolume
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import MinvandforsyningCoordinator, MinvandforsyningData


@dataclass(frozen=True, kw_only=True)
class MinvandforsyningSensorDescription(SensorEntityDescription):
    """Describes a MinVandforsyning sensor."""

    value_fn: Callable[[MinvandforsyningData], Any]
    extra_attrs_fn: Callable[[MinvandforsyningData], dict[str, Any]] | None = None


SENSOR_DESCRIPTIONS: tuple[MinvandforsyningSensorDescription, ...] = (
    MinvandforsyningSensorDescription(
        key="total",
        translation_key="total",
        device_class=SensorDeviceClass.WATER,
        state_class=SensorStateClass.TOTAL_INCREASING,
        native_unit_of_measurement=UnitOfVolume.CUBIC_METERS,
        suggested_display_precision=3,
        value_fn=lambda data: data.total_m3,
        extra_attrs_fn=lambda data: {
            "last_reading_date": data.latest_reading.date.isoformat() if data.latest_reading else None,
            "readings_count": len(data.readings),
        },
    ),
    MinvandforsyningSensorDescription(
        key="hourly",
        translation_key="hourly",
        device_class=SensorDeviceClass.WATER,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfVolume.LITERS,
        suggested_display_precision=0,
        value_fn=lambda data: data.last_hour_liters,
    ),
    MinvandforsyningSensorDescription(
        key="daily",
        translation_key="daily",
        device_class=SensorDeviceClass.WATER,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfVolume.LITERS,
        suggested_display_precision=0,
        value_fn=lambda data: data.daily_liters(),
    ),
    MinvandforsyningSensorDescription(
        key="min_hourly_consumption",
        translation_key="min_hourly_consumption",
        device_class=SensorDeviceClass.WATER,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfVolume.LITERS,
        suggested_display_precision=0,
        value_fn=lambda data: data.min_hourly_consumption,
    ),
    MinvandforsyningSensorDescription(
        key="latest_zero_consumption",
        translation_key="latest_zero_consumption",
        device_class=SensorDeviceClass.TIMESTAMP,
        value_fn=lambda data: data.latest_zero_consumption,
    ),
    MinvandforsyningSensorDescription(
        key="zero_consumption_hours",
        translation_key="zero_consumption_hours",
        state_class=SensorStateClass.TOTAL,
        native_unit_of_measurement="h",
        suggested_display_precision=0,
        value_fn=lambda data: data.zero_consumption_hours,
    ),
    MinvandforsyningSensorDescription(
        key="high_consumption_hours",
        translation_key="high_consumption_hours",
        state_class=SensorStateClass.TOTAL,
        native_unit_of_measurement="h",
        suggested_display_precision=0,
        value_fn=lambda data: data.high_consumption_hours,
    ),
    MinvandforsyningSensorDescription(
        key="night_consumption_total",
        translation_key="night_consumption_total",
        device_class=SensorDeviceClass.WATER,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfVolume.LITERS,
        suggested_display_precision=0,
        value_fn=lambda data: data.night_consumption_total,
    ),
    MinvandforsyningSensorDescription(
        key="nights_with_continuous_flow",
        translation_key="nights_with_continuous_flow",
        state_class=SensorStateClass.TOTAL,
        suggested_display_precision=0,
        value_fn=lambda data: data.nights_with_continuous_flow,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up MinVandforsyning sensors from a config entry."""
    coordinator: MinvandforsyningCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        MinvandforsyningSensor(coordinator, description, entry)
        for description in SENSOR_DESCRIPTIONS
    )


class MinvandforsyningSensor(
    CoordinatorEntity[MinvandforsyningCoordinator], SensorEntity
):
    """A MinVandforsyning sensor."""

    _attr_has_entity_name = True
    entity_description: MinvandforsyningSensorDescription

    def __init__(
        self,
        coordinator: MinvandforsyningCoordinator,
        description: MinvandforsyningSensorDescription,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"{entry.data['meter_number']}_{description.key}"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, entry.data["meter_number"])},
            "name": f"Water Meter {entry.data['meter_number']}",
            "manufacturer": "Rambøll",
            "model": "MinVandforsyning",
            "entry_type": "service",
        }

    @property
    def native_value(self) -> Any:
        if self.coordinator.data is None:
            return None
        return self.entity_description.value_fn(self.coordinator.data)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        if self.coordinator.data is None or self.entity_description.extra_attrs_fn is None:
            return None
        return self.entity_description.extra_attrs_fn(self.coordinator.data)
