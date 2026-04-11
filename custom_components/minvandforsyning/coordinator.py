"""DataUpdateCoordinator for MinVandforsyning."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api_client import MinvandforsyningClient
from .const import (
    COL_CONSUMPTION,
    COL_READING,
    COL_READING_DATE,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    QUERY_LOOKBACK_HOURS,
    READINGS_TABLE_INDEX,
)
from .protobuf_parser import parse_dataset

_LOGGER = logging.getLogger(__name__)


class MeterReading:
    """A single hourly meter reading."""

    __slots__ = ("date", "reading", "consumption")

    def __init__(self, date: datetime, reading: Decimal, consumption: Decimal) -> None:
        self.date = date
        self.reading = reading
        self.consumption = consumption


class MinvandforsyningData:
    """Processed data from the API."""

    def __init__(self, readings: list[MeterReading]) -> None:
        self.readings = readings

    @property
    def latest_reading(self) -> MeterReading | None:
        return self.readings[-1] if self.readings else None

    @property
    def total_m3(self) -> Decimal | None:
        latest = self.latest_reading
        return latest.reading if latest else None

    @property
    def last_hour_liters(self) -> Decimal | None:
        latest = self.latest_reading
        return latest.consumption if latest else None

    def daily_liters(self, date: datetime | None = None) -> Decimal:
        """Sum consumption for a given date (default: today)."""
        if date is None:
            date = datetime.now()
        target_date = date.date()
        return sum(
            (r.consumption for r in self.readings if r.date.date() == target_date),
            Decimal(0),
        )


class MinvandforsyningCoordinator(DataUpdateCoordinator[MinvandforsyningData]):
    """Coordinator that fetches meter data from the Rambøll API."""

    def __init__(
        self,
        hass: HomeAssistant,
        client: MinvandforsyningClient,
        meter_number: str,
        supplier_id: int,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=DEFAULT_SCAN_INTERVAL),
        )
        self._client = client
        self._meter_number = meter_number
        self._supplier_id = supplier_id

    async def _async_update_data(self) -> MinvandforsyningData:
        """Fetch and parse meter data."""
        now = datetime.now(timezone.utc)
        date_from = now - timedelta(hours=QUERY_LOOKBACK_HOURS)
        date_to = now

        try:
            raw = await self._client.async_get_meter_data(
                self._meter_number, self._supplier_id, date_from, date_to,
            )
        except Exception as err:
            raise UpdateFailed(f"Error fetching meter data: {err}") from err

        try:
            tables = parse_dataset(raw)
        except Exception as err:
            raise UpdateFailed(f"Error parsing meter data: {err}") from err

        if len(tables) <= READINGS_TABLE_INDEX:
            raise UpdateFailed(
                f"Response has {len(tables)} tables, expected at least {READINGS_TABLE_INDEX + 1}"
            )

        readings_table = tables[READINGS_TABLE_INDEX]
        readings: list[MeterReading] = []
        for row in readings_table.rows:
            reading_date = row.get(COL_READING_DATE)
            reading = row.get(COL_READING)
            consumption = row.get(COL_CONSUMPTION)
            if reading_date is not None and reading is not None and consumption is not None:
                readings.append(MeterReading(reading_date, reading, consumption))

        _LOGGER.debug(
            "Fetched %d readings for meter %s (latest: %s)",
            len(readings),
            self._meter_number,
            readings[-1].date if readings else "none",
        )
        return MinvandforsyningData(readings)
