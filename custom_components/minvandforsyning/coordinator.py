"""DataUpdateCoordinator for MinVandforsyning."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from aiohttp import ClientResponseError
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
    statistics_during_period,
    valid_statistic_id,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfVolume
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api_client import (
    InvalidResponseError,
    MeterReading,
    MinvandforsyningClient,
    parse_readings,
    retry_after_seconds,
)
from .const import (
    DEFAULT_IMPORT_STATISTICS,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    HISTORY_IMPORT_ATTEMPTS,
    INITIAL_HYDRATE_DAYS,
    LITERS_PER_CUBIC_METER,
    MAX_SCAN_INTERVAL,
    READING_DATE_TZ,
    RECENT_READINGS_DAYS,
    STATISTIC_ID_FORMAT,
)

_LOGGER = logging.getLogger(__name__)
_READING_DATE_ZONE = ZoneInfo(READING_DATE_TZ)
# How long an import waits for the recorder to store its rows.
_RECORDER_COMMIT_TIMEOUT_SECONDS = 300


def _interval_start_utc(reading_date: datetime) -> datetime:
    """Return the UTC start of the hourly interval a reading accounts for.

    ``ReadingDate`` marks the *end* of the interval ``[ReadingDate - 1h,
    ReadingDate)`` and its naive value is UTC (issue #9). The one-hour
    subtraction is interval-label semantics, not a timezone offset. Pure: no
    clamping or logging, so it is safe to share between statistics import and
    the daily sensor.
    """
    if reading_date.tzinfo is not None:
        utc_end = reading_date.astimezone(timezone.utc)
    else:
        utc_end = reading_date.replace(tzinfo=timezone.utc)
    return utc_end - timedelta(hours=1)


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
        """Sum consumption for a local civil day (default: today).

        Each reading is assigned to the Copenhagen local date of its interval
        *start*, matching the hourly statistics bucket so the daily sensor and
        the Energy dashboard agree on which day an hour belongs to. The hour
        ending at local midnight counts toward the day it was used, not the
        next one.
        """
        if date is None:
            date = datetime.now(tz=_READING_DATE_ZONE)
        target_date = date.date()
        return sum(
            (
                r.consumption
                for r in self.readings
                if _interval_start_utc(r.date).astimezone(_READING_DATE_ZONE).date()
                == target_date
            ),
            Decimal(0),
        )


class MinvandforsyningCoordinator(DataUpdateCoordinator[MinvandforsyningData]):
    """Coordinator that fetches meter data from the Rambøll API."""

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        client: MinvandforsyningClient,
        meter_number: str,
        supplier_id: int,
        scan_interval: int = DEFAULT_SCAN_INTERVAL,
        import_statistics: bool = DEFAULT_IMPORT_STATISTICS,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
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
        # History imports that failed in a row; see _safe_import_readings.
        self._failed_history_imports = 0
        self._can_store_statistics = valid_statistic_id(self.statistic_id)
        if import_statistics and not self._can_store_statistics:
            _LOGGER.warning(
                "Not importing the statistics stream %s for meter %s: a statistic "
                "ID may only contain lowercase letters, digits and underscores",
                self.statistic_id,
                meter_number,
            )

    @property
    def meter_number(self) -> str:
        """Supplier-side meter identifier."""
        return self._meter_number

    @property
    def can_store_statistics(self) -> bool:
        """Whether the meter number can be part of a statistic ID."""
        return self._can_store_statistics

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

        Shared by the recurring poll (called with ``RECENT_READINGS_DAYS``)
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
        except ClientResponseError as err:
            # The error's own text has the request URL, with the meter number.
            raise UpdateFailed(
                f"Error fetching meter data: HTTP {err.status}",
                retry_after=self._pause_after_rate_limit(err),
            ) from err
        except Exception as err:
            raise UpdateFailed(f"Error fetching meter data: {err}") from err

        try:
            readings = parse_readings(raw, now)
        except InvalidResponseError as err:
            raise UpdateFailed(f"Error parsing meter data: {err}") from err

        _LOGGER.debug(
            "Fetched %d readings for meter %s over %d days (latest: %s)",
            len(readings),
            self._meter_number,
            days,
            readings[-1].date if readings else "none",
        )
        return MinvandforsyningData(readings)

    def _pause_after_rate_limit(self, err: ClientResponseError) -> float | None:
        """Return how long Home Assistant should wait after a rate limit."""
        if err.status != 429 or (seconds := retry_after_seconds(err.headers)) is None:
            return None
        if self.data is not None and self.update_interval:
            # After setup, Home Assistant polls again after this delay instead
            # of the interval, which must stay the shortest wait.
            seconds = max(seconds, self.update_interval.total_seconds())
        # Home Assistant limits the wait of a setup retry, but not of a poll.
        return min(seconds, MAX_SCAN_INTERVAL)

    async def _async_update_data(self) -> MinvandforsyningData:
        """Fetch and parse meter data for the routine poll window."""
        result = await self.async_fetch_window(RECENT_READINGS_DAYS)

        if self._import_statistics:
            self.hass.async_create_background_task(
                self._safe_import_readings(result),
                name=f"{DOMAIN}_import_stats_{self._meter_number}",
            )
        return result

    @staticmethod
    def reading_start_utc(reading_date: datetime) -> datetime:
        """Return the UTC start of the hour a reading accounts for.

        Verified against the live Rambøll API (issue #9): ``ReadingDate`` is a
        UTC timestamp marking the *end* of the hourly consumption interval —
        ``Consumption(T) == Reading(T) - Reading(T-1)``, i.e. the water used
        during ``[T-1, T)``. Home Assistant statistics are keyed by the *start*
        of the hour, so the bucket start is ``ReadingDate`` (as UTC) minus one
        hour. UTC has no DST, so there is no fold or spring-forward gap to
        handle.
        """
        start = _interval_start_utc(reading_date)
        if start.minute or start.second or start.microsecond:
            clamped = start.replace(minute=0, second=0, microsecond=0)
            _LOGGER.warning(
                "ReadingDate %s is not on the hour; clamping statistic bucket start to %s",
                reading_date, clamped,
            )
            start = clamped
        return start

    async def _safe_import_readings(self, data: MinvandforsyningData) -> None:
        """Import readings into long-term statistics, logging any failure.

        An empty stream gets the supplier's history instead, which includes
        these readings. Storing these first would make the history import
        skip everything older, so after a failed history import they wait for
        the next poll to try again, up to HISTORY_IMPORT_ATTEMPTS in a row.
        """
        if not self._can_store_statistics:
            return
        try:
            imported_history = await self.async_hydrate()
        except Exception:  # noqa: BLE001 - tried again, then the latest readings are imported
            self._failed_history_imports += 1
            if self._failed_history_imports < HISTORY_IMPORT_ATTEMPTS:
                _LOGGER.warning(
                    "Could not import the supplier history for meter %s; "
                    "trying again at the next poll",
                    self._meter_number,
                    exc_info=True,
                )
                return
            _LOGGER.warning(
                "Could not import the supplier history for meter %s after %d attempts, "
                "so only the latest readings are imported; the backfill service can "
                "import it later",
                self._meter_number,
                HISTORY_IMPORT_ATTEMPTS,
                exc_info=True,
            )
            imported_history = False
        self._failed_history_imports = 0
        if imported_history:
            return
        try:
            await self.async_import_readings(data)
        except Exception:  # noqa: BLE001 - intentional safety net
            _LOGGER.warning(
                "Failed to import long-term statistics for meter %s",
                self._meter_number,
                exc_info=True,
            )

    async def async_hydrate(self, days: int = INITIAL_HYDRATE_DAYS) -> bool:
        """Import multi-year supplier history into an empty LTS stream.

        Idempotent: if the external statistic stream already contains any data,
        this is a no-op. Safe to call repeatedly. Returns True if it imported
        history.
        """
        if not self._import_statistics:
            _LOGGER.debug(
                "Stats import disabled; skipping hydrate for meter %s",
                self._meter_number,
            )
            return False
        # Idempotency: skip if the LTS stream already has anything.
        last_stats = await get_instance(self.hass).async_add_executor_job(
            get_last_statistics, self.hass, 1, self.statistic_id, True, {"sum"},
        )
        if last_stats and self.statistic_id in last_stats:
            _LOGGER.debug(
                "LTS stream %s already populated; skipping hydrate",
                self.statistic_id,
            )
            return False
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
            return False
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
        return True

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
            # The next import continues from the last stored sum, so it must
            # not start before the recorder has written these rows.
            if await self._do_import(data, force_full=force_full):
                await self._async_wait_for_recorder()

    async def _async_wait_for_recorder(self) -> None:
        try:
            async with asyncio.timeout(_RECORDER_COMMIT_TIMEOUT_SECONDS):
                await get_instance(self.hass).async_block_till_done()
        except TimeoutError:
            _LOGGER.debug(
                "The recorder hasn't stored the statistics for meter %s within %d s",
                self._meter_number,
                _RECORDER_COMMIT_TIMEOUT_SECONDS,
            )

    async def _do_import(
        self,
        data: MinvandforsyningData,
        *,
        force_full: bool,
    ) -> bool:
        """Queue the readings' statistics; return whether there were any."""
        statistic_id = self.statistic_id

        cutoff_ts: float | None = None
        sum_so_far = Decimal(0)
        if force_full:
            first_start_utc = self.reading_start_utc(data.readings[0].date)
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
            return False

        metadata: StatisticMetaData = {
            "has_sum": True,
            "mean_type": StatisticMeanType.NONE,
            "name": f"Water Meter {self._meter_number} Total Consumption",
            "source": DOMAIN,
            "statistic_id": statistic_id,
            "unit_class": "volume",
            "unit_of_measurement": str(UnitOfVolume.CUBIC_METERS),
        }

        async_add_external_statistics(self.hass, metadata, statistics)
        _LOGGER.debug(
            "Imported %d external statistic points for %s",
            len(statistics),
            statistic_id,
        )
        return True
