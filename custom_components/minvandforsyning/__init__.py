"""The MinVandforsyning integration."""

from __future__ import annotations

import logging

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api_client import MinvandforsyningClient
from .const import (
    CONF_IMPORT_STATISTICS,
    CONF_SCAN_INTERVAL,
    DEFAULT_IMPORT_STATISTICS,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    SERVICE_BACKFILL_MAX_DAYS,
    SERVICE_BACKFILL_STATISTICS,
)
from .coordinator import MinvandforsyningCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["sensor"]

SERVICE_BACKFILL_SCHEMA = vol.Schema(
    {
        vol.Optional("meter_number"): cv.string,
        vol.Optional("days"): vol.All(
            vol.Coerce(int),
            vol.Range(min=1, max=SERVICE_BACKFILL_MAX_DAYS),
        ),
        vol.Optional("force_full", default=False): cv.boolean,
    }
)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up MinVandforsyning from a config entry."""
    session = async_get_clientsession(hass)
    client = MinvandforsyningClient(session)

    scan_interval = entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)
    import_statistics = entry.options.get(
        CONF_IMPORT_STATISTICS, DEFAULT_IMPORT_STATISTICS
    )

    coordinator = MinvandforsyningCoordinator(
        hass,
        client,
        meter_number=entry.data["meter_number"],
        supplier_id=entry.data["supplier_id"],
        scan_interval=scan_interval,
        import_statistics=import_statistics,
    )

    await coordinator.async_config_entry_first_refresh()

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Hydrate LTS with supplier history on first install. Idempotent: skips
    # if the stream is already populated, so re-installs and reload-after-
    # options are free. Background task so setup never blocks on the
    # multi-year fetch.
    if import_statistics:
        hass.async_create_background_task(
            coordinator._safe_hydrate(),
            name=f"{DOMAIN}_hydrate_{entry.data['meter_number']}",
        )

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    _async_register_backfill_service(hass)

    return True


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload integration when options change.

    Reload covers both scan_interval and import_statistics changes; the
    latter doesn't strictly need a reload (the next refresh would re-read
    the option) but reloading is simpler and matches existing behaviour.
    """
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)
        if not hass.data[DOMAIN] and hass.services.has_service(
            DOMAIN, SERVICE_BACKFILL_STATISTICS
        ):
            hass.services.async_remove(DOMAIN, SERVICE_BACKFILL_STATISTICS)
    return unload_ok


def _async_register_backfill_service(hass: HomeAssistant) -> None:
    """Register the ``backfill_statistics`` service once per HA instance."""
    if hass.services.has_service(DOMAIN, SERVICE_BACKFILL_STATISTICS):
        return

    async def _handle_backfill(call: ServiceCall) -> None:
        meter_filter = call.data.get("meter_number")
        days = call.data.get("days")
        force_full = call.data.get("force_full", False)

        matched = False
        success_count: int = 0
        for coord in hass.data.get(DOMAIN, {}).values():
            if meter_filter and coord.meter_number != meter_filter:
                continue
            matched = True
            try:
                if days is not None:
                    # Fresh fetch with the requested window (up to ~5y).
                    data = await coord.async_fetch_window(days)
                else:
                    # Replay last-known cached readings (no fresh fetch).
                    data = coord.data
                    if data is None:
                        _LOGGER.warning(
                            "Backfill: no cached data for meter %s yet; "
                            "specify the 'days' field to trigger a fresh fetch",
                            coord.meter_number,
                        )
                        # No cache + no days is a soft skip, not a failure
                        # (the coord just has nothing to replay yet).
                        success_count += 1
                        continue
                await coord.async_import_readings(data, force_full=force_full)
            except Exception:  # noqa: BLE001 - one failing meter must not stop the rest
                _LOGGER.warning(
                    "Backfill failed for meter %s",
                    coord.meter_number,
                    exc_info=True,
                )
            else:
                success_count += 1

        if meter_filter and not matched:
            _LOGGER.warning("No configured meter matches %s", meter_filter)
        if matched and success_count == 0:
            raise HomeAssistantError("All meter backfills failed; see logs")

    hass.services.async_register(
        DOMAIN,
        SERVICE_BACKFILL_STATISTICS,
        _handle_backfill,
        schema=SERVICE_BACKFILL_SCHEMA,
    )

