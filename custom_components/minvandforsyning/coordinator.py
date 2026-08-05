"""DataUpdateCoordinator for MinVandforsyning."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.const import UnitOfVolume
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api_client import MinvandforsyningClient
from .const import (
    COL_CONSUMPTION,
    COL_READING,
    COL_READING_DATE,
    DEFAULT_IMPORT_STATISTICS,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    INITIAL_HYDRATE_DAYS,
    LITERS_PER_CUBIC_METER,
    QUERY_LOOKBACK_HOURS,
    READINGS_TABLE_INDEX,
    STATISTIC_ID_FORMAT,
)
from .protobuf_parser import parse_dataset

# StatisticMeanType / unit_class were introduced after HA 2024.10. Older HA
# releases still rely on the legacy ``has_mean`` boolean. We feature-detect to
# stay compatible across the supported HA range (the production install in
# the wild may be on either side of the transition).
try:
    from homeassistant.components.recorder.models import StatisticMeanType
    _HAS_STATISTIC_MEAN_TYPE = True
except ImportError:  # pragma: no cover - HA < 2024.11
    StatisticMeanType = None  # type: ignore[assignment]
    _HAS_STATISTIC_MEAN_TYPE = False

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
        scan_interval: int = DEFAULT_SCAN_INTERVAL,
        import_statistics: bool = DEFAULT_IMPORT_STATISTICS,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=scan_interval),
        )
        self._client = client
        self._meter_number = meter_number
        self._supplier_id = supplier_id
        self._import_statistics = import_statistics
        # Serializes the read-modify-write of the running sum so that a
        # background import task triggered by a previous refresh can't race
        # the next refresh's import or a backfill service call.
        self._stats_lock = asyncio.Lock()

    @property
    def meter_number(self) -> str:
        """Supplier-side meter identifier."""
        return self._meter_number

    @property
    def statistic_id(self) -> str:
        """Stable external-statistics id derived from the meter number.

        Uses the ``domain:object_id`` form required by
        ``async_add_external_statistics`` — a separate stream from the
        sensor entity, so it never collides with the recorder's auto-LTS.
        """
        return STATISTIC_ID_FORMAT.format(meter_number=self._meter_number)

    async def async_fetch_window(self, days: int) -> MinvandforsyningData:
        """Fetch and parse meter data for the last *days* days.

        Shared by the recurring poll (called with ``QUERY_LOOKBACK_HOURS // 24``)
        and by hydrate / backfill service (called with up to
        ``INITIAL_HYDRATE_DAYS``). Returns parsed :class:`MinvandforsyningData`;
        raises :class:`UpdateFailed` on failure.
        """
        now = datetime.now(timezone.utc)
        date_from = now - timedelta(days=days)
        # DateTo is exclusive in the API — use tomorrow to include today's data
        date_to = now + timedelta(days=1)

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
            "Fetched %d readings for meter %s over %d days (latest: %s)",
            len(readings),
            self._meter_number,
            days,
            readings[-1].date if readings else "none",
        )
        return MinvandforsyningData(readings)

    async def _async_update_data(self) -> MinvandforsyningData:
        """Fetch and parse meter data for the routine poll window."""
        # ceil(QUERY_LOOKBACK_HOURS / 24); preserves the previous 48h window.
        poll_days = max(1, (QUERY_LOOKBACK_HOURS + 23) // 24)
        result = await self.async_fetch_window(poll_days)

        if self._import_statistics:
            self.hass.async_create_background_task(
                self._safe_import_readings(result),
                name=f"{DOMAIN}_import_stats_{self._meter_number}",
            )
        return result

    def reading_start_utc(self, reading_date: datetime) -> datetime:
        """Return the UTC start of the hour a reading accounts for.

        Verified against the live Rambøll API (issue #9): ``ReadingDate`` is a
        UTC timestamp marking the *end* of the hourly consumption interval —
        ``Consumption(T) == Reading(T) - Reading(T-1)``, i.e. the water used
        during ``[T-1, T)``. Home Assistant statistics are keyed by the *start*
        of the hour, so the bucket start is ``ReadingDate`` (as UTC) minus one
        hour. UTC has no DST, so there is no fold or spring-forward gap to
        handle.
        """
        if reading_date.tzinfo is not None:
            reading_date = reading_date.astimezone(timezone.utc)
        else:
            reading_date = reading_date.replace(tzinfo=timezone.utc)
        return reading_date - timedelta(hours=1)

    async def _safe_import_readings(self, data: MinvandforsyningData) -> None:
        """Run ``async_import_readings`` swallowing any failure.

        Statistics import must never break the coordinator update loop. If the
        recorder is unavailable or the call raises, log a warning and move on.
        """
        try:
            await self.async_import_readings(data)
        except Exception:  # noqa: BLE001 - intentional safety net
            _LOGGER.warning(
                "Failed to import long-term statistics for meter %s",
                self._meter_number,
                exc_info=True,
            )

    async def async_hydrate(self, days: int = INITIAL_HYDRATE_DAYS) -> None:
        """One-shot import of multi-year supplier history into LTS.

        Idempotent: if the external statistic stream already contains any data,
        this is a no-op. Safe to call repeatedly. Designed for first-install
        bootstrap when the recorder has no rows for our statistic_id yet.
        """
        if not self._import_statistics:
            _LOGGER.debug(
                "Stats import disabled; skipping hydrate for meter %s",
                self._meter_number,
            )
            return
        # Idempotency: skip if the LTS stream already has anything.
        last_stats = await get_instance(self.hass).async_add_executor_job(
            get_last_statistics, self.hass, 1, self.statistic_id, True, {"sum"},
        )
        if last_stats and self.statistic_id in last_stats:
            _LOGGER.debug(
                "LTS stream %s already populated; skipping hydrate",
                self.statistic_id,
            )
            return
        _LOGGER.info(
            "Hydrating LTS stream %s with up to %d days of supplier history",
            self.statistic_id,
            days,
        )
        data = await self.async_fetch_window(days)
        if not data.readings:
            _LOGGER.warning(
                "Hydrate fetched 0 readings for meter %s; nothing imported",
                self._meter_number,
            )
            return
        # async_import_readings's cutoff lookup will see the empty LTS we just
        # checked, so sum_so_far starts at 0 and every reading gets imported.
        await self.async_import_readings(data)
        _LOGGER.info(
            "Hydrated %d readings into %s (range: %s → %s)",
            len(data.readings),
            self.statistic_id,
            data.readings[0].date,
            data.readings[-1].date,
        )

    async def _safe_hydrate(self, days: int = INITIAL_HYDRATE_DAYS) -> None:
        """Run :meth:`async_hydrate` swallowing any failure. Opportunistic."""
        try:
            await self.async_hydrate(days)
        except Exception:  # noqa: BLE001 - hydrate must never break setup
            _LOGGER.warning(
                "Initial hydrate failed for meter %s; routine polls will continue",
                self._meter_number,
                exc_info=True,
            )

    async def async_import_readings(
        self,
        data: MinvandforsyningData,
        *,
        force_full: bool = False,
    ) -> None:
        """Import meter readings into Home Assistant long-term statistics.

        Writes a separate external-statistics stream
        (``minvandforsyning:water_meter_<n>_total``) at the supplier's
        reported timestamps. A running cumulative ``sum`` in m³ continues
        from the last stored statistic unless ``force_full`` is set.
        """
        if not data.readings:
            return

        async with self._stats_lock:
            await self._do_import(data, force_full=force_full)

    async def _do_import(
        self,
        data: MinvandforsyningData,
        *,
        force_full: bool,
    ) -> None:
        statistic_id = self.statistic_id

        cutoff_ts: float | None = None
        sum_so_far = Decimal(0)
        if force_full:
            first_start_utc = self.reading_start_utc(data.readings[0].date).replace(
                minute=0, second=0, microsecond=0,
            )
            # Preserve continuity when rewriting an overlapping recent window:
            # use the latest stored sum strictly before the first imported
            # bucket as the starting point. Time-bounded query so the cost
            # stays constant and the anchor is reachable regardless of how
            # many rows already sit inside the rewritten window.
            anchor = await get_instance(self.hass).async_add_executor_job(
                statistics_during_period,
                self.hass,
                first_start_utc - timedelta(hours=1),
                first_start_utc,
                {statistic_id},
                "hour",
                None,
                {"sum"},
            )
            prior_rows = anchor.get(statistic_id) if anchor else None
            if prior_rows:
                sum_so_far = Decimal(str(prior_rows[-1].get("sum") or 0))
        else:
            last_stats = await get_instance(self.hass).async_add_executor_job(
                get_last_statistics, self.hass, 1, statistic_id, True, {"sum"},
            )
            if last_stats and statistic_id in last_stats:
                last = last_stats[statistic_id][0]
                cutoff_ts = last["start"]
                sum_so_far = Decimal(str(last.get("sum") or 0))

        statistics: list[StatisticData] = []
        for reading in data.readings:
            start_utc = self.reading_start_utc(reading.date)
            # async_add_external_statistics rejects any timestamp not aligned
            # to the top of the hour; defend against an off-hour reading
            # corrupting the whole batch.
            start_utc = start_utc.replace(minute=0, second=0, microsecond=0)
            if cutoff_ts is not None and start_utc.timestamp() <= cutoff_ts:
                continue
            delta = reading.consumption / Decimal(LITERS_PER_CUBIC_METER)
            if reading.consumption < 0:
                _LOGGER.debug(
                    "Meter %s reported negative consumption %s at %s; "
                    "propagating as decrement",
                    self._meter_number, reading.consumption, reading.date,
                )
            sum_so_far += delta
            statistics.append(StatisticData(
                start=start_utc,
                state=float(reading.reading),
                sum=float(sum_so_far),
            ))

        if not statistics:
            return

        metadata: StatisticMetaData = {
            # has_mean kept for HA < 2025.11 (recorder still reads this key);
            # newer releases also honour mean_type below.
            "has_mean": False,
            "has_sum": True,
            "name": f"Water Meter {self._meter_number} Total Consumption",
            "source": DOMAIN,
            "statistic_id": statistic_id,
            "unit_of_measurement": str(UnitOfVolume.CUBIC_METERS),
        }
        if _HAS_STATISTIC_MEAN_TYPE:
            metadata["mean_type"] = StatisticMeanType.NONE  # type: ignore[typeddict-unknown-key]
        # unit_class is a HA 2025.11+ hint; harmless on older releases that
        # ignore unknown keys, useful on newer ones for unit-conversion picker.
        metadata["unit_class"] = "volume"  # type: ignore[typeddict-unknown-key]

        async_add_external_statistics(self.hass, metadata, statistics)
        _LOGGER.debug(
            "Imported %d external statistic points for %s",
            len(statistics),
            statistic_id,
        )
