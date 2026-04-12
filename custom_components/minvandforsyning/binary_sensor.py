"""Binary sensor platform for MinVandforsyning."""

from __future__ import annotations

from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import MinvandforsyningCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up MinVandforsyning binary sensors from a config entry."""
    coordinator: MinvandforsyningCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([MeterProblemSensor(coordinator, entry)])


class MeterProblemSensor(
    CoordinatorEntity[MinvandforsyningCoordinator], BinarySensorEntity
):
    """Binary sensor for meter problem detection via InfoCode."""

    _attr_has_entity_name = True
    _attr_device_class = BinarySensorDeviceClass.PROBLEM
    _attr_translation_key = "meter_problem"

    def __init__(
        self,
        coordinator: MinvandforsyningCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{entry.data['meter_number']}_meter_problem"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, entry.data["meter_number"])},
        }

    @property
    def is_on(self) -> bool | None:
        if self.coordinator.data is None:
            return None
        return self.coordinator.data.info_code_active

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        if self.coordinator.data is None:
            return None
        return {
            "info_code": self.coordinator.data.info_code_value,
        }
