"""Config flow for MinVandforsyning."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, OptionsFlow
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api_client import MinvandforsyningClient
from .const import (
    COL_READING,
    CONF_SCAN_INTERVAL,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    MAX_SCAN_INTERVAL,
    MIN_SCAN_INTERVAL,
    READINGS_TABLE_INDEX,
)
from .protobuf_parser import parse_dataset

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required("meter_number"): str,
    }
)


class MinvandforsyningConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for MinVandforsyning."""

    VERSION = 1

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        """Return the options flow handler."""
        return MinvandforsyningOptionsFlow()

    def __init__(self) -> None:
        self._meter_number: str = ""
        self._supplier_id: int = 0
        self._latest_reading: str = ""

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Handle the initial step: enter meter number."""
        errors: dict[str, str] = {}

        if user_input is not None:
            self._meter_number = user_input["meter_number"].strip()

            if not self._meter_number:
                errors["meter_number"] = "invalid_meter_number"
                return self.async_show_form(
                    step_id="user",
                    data_schema=STEP_USER_DATA_SCHEMA,
                    errors=errors,
                )

            # Check for duplicate
            await self.async_set_unique_id(self._meter_number)
            self._abort_if_unique_id_configured()

            return await self.async_step_discover()

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )

    async def async_step_discover(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Discover the supplier ID for the meter."""
        session = async_get_clientsession(self.hass)
        client = MinvandforsyningClient(session)

        try:
            supplier_id = await client.async_discover_supplier_id(self._meter_number)
        except Exception:
            _LOGGER.exception("Error discovering supplier ID")
            return self.async_abort(reason="cannot_connect")

        if supplier_id is None:
            return self.async_abort(reason="meter_not_found")

        self._supplier_id = supplier_id

        # Fetch a quick reading to confirm and show to user
        try:
            now = datetime.now(timezone.utc)
            raw = await client.async_get_meter_data(
                self._meter_number, self._supplier_id,
                now - timedelta(days=7), now,
            )
            tables = parse_dataset(raw)
            if len(tables) > READINGS_TABLE_INDEX:
                rows = tables[READINGS_TABLE_INDEX].rows
                if rows:
                    self._latest_reading = str(rows[-1].get(COL_READING, "?"))
        except Exception:
            _LOGGER.debug("Could not fetch preview reading", exc_info=True)
            self._latest_reading = "?"

        return await self.async_step_confirm()

    async def async_step_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Confirm the discovered meter."""
        if user_input is not None:
            return self.async_create_entry(
                title=f"Meter {self._meter_number}",
                data={
                    "meter_number": self._meter_number,
                    "supplier_id": self._supplier_id,
                },
            )

        return self.async_show_form(
            step_id="confirm",
            description_placeholders={
                "meter_number": self._meter_number,
                "supplier_id": str(self._supplier_id),
                "latest_reading": self._latest_reading,
            },
        )


class MinvandforsyningOptionsFlow(OptionsFlow):
    """Handle options for MinVandforsyning."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Manage the polling interval option."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        current = self.config_entry.options.get(
            CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL
        )

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_SCAN_INTERVAL,
                        default=current // 60,
                    ): vol.All(
                        vol.Coerce(int),
                        vol.Range(
                            min=MIN_SCAN_INTERVAL // 60,
                            max=MAX_SCAN_INTERVAL // 60,
                        ),
                        lambda m: m * 60,
                    ),
                }
            ),
        )
