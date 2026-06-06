"""Tests for long-term statistics import (issue #3)."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from custom_components.minvandforsyning import (
    SERVICE_BACKFILL_SCHEMA,
    _async_register_backfill_service,
)
from custom_components.minvandforsyning.const import (
    DOMAIN,
    SERVICE_BACKFILL_STATISTICS,
    SUPPLIER_TIMEZONE,
)
from custom_components.minvandforsyning.coordinator import (
    _HAS_STATISTIC_MEAN_TYPE,
    MeterReading,
    MinvandforsyningCoordinator,
    MinvandforsyningData,
    StatisticMeanType,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# TODO: a recorder-mock end-to-end integration test against the real recorder
# (via pytest-homeassistant-custom-component) is intentionally NOT included
# here — adopting that plugin's fixtures pulls in significant test infra into
# the standalone runner. The mocked boundary tests below cover the contract
# we care about (correct call shape, idempotency, lock semantics).

_DK = ZoneInfo(SUPPLIER_TIMEZONE)
_DEFAULT_METER = "12345"
_DEFAULT_STATISTIC_ID = f"{DOMAIN}:water_meter_{_DEFAULT_METER}_total"


def _reading(year: int, month: int, day: int, hour: int, total: str, consumption: str) -> MeterReading:
    """Build a MeterReading with a naive Danish-local datetime."""
    return MeterReading(
        date=datetime(year, month, day, hour),
        reading=Decimal(total),
        consumption=Decimal(consumption),
    )


def _sample_readings() -> list[MeterReading]:
    return [
        _reading(2026, 4, 11, 0, "300.000", "100"),
        _reading(2026, 4, 11, 1, "300.100", "200"),
        _reading(2026, 4, 11, 2, "300.300", "300"),
        _reading(2026, 4, 11, 3, "300.600", "400"),
        _reading(2026, 4, 11, 4, "301.000", "500"),
    ]


def _make_coordinator(*, import_statistics: bool = True, meter: str = _DEFAULT_METER) -> MinvandforsyningCoordinator:
    """Construct a coordinator with mocked hass/client.

    Using a real coordinator instance (rather than a MagicMock) means the
    locked behaviour of ``to_utc`` and the asyncio lock is exercised
    end-to-end.
    """
    hass = MagicMock()
    client = MagicMock()
    coord = MinvandforsyningCoordinator(
        hass, client,
        meter_number=meter, supplier_id=1,
        import_statistics=import_statistics,
    )
    return coord


class _RecorderPatches:
    """Context manager bundling the recorder patches.

    Patches at the coordinator's import site so the module-top imports stay
    intact. Yields a handle so tests can configure ``last_stats`` (cutoff)
    or make the import raise via ``import_side_effect``.
    """

    def __init__(
        self,
        *,
        last_stats: dict[str, list[dict[str, Any]]] | None = None,
        import_side_effect: Exception | None = None,
    ) -> None:
        self._last_stats = last_stats or {}
        self._import_side_effect = import_side_effect
        self._patches: list[Any] = []
        self.import_stats: MagicMock | None = None
        self.last_stats: MagicMock | None = None
        self.get_instance: MagicMock | None = None

    def __enter__(self) -> "_RecorderPatches":
        p_imp = patch(
            "custom_components.minvandforsyning.coordinator.async_add_external_statistics"
        )
        p_last = patch(
            "custom_components.minvandforsyning.coordinator.get_last_statistics",
            return_value=self._last_stats,
        )
        p_inst = patch(
            "custom_components.minvandforsyning.coordinator.get_instance"
        )

        self.import_stats = p_imp.start()
        if self._import_side_effect is not None:
            self.import_stats.side_effect = self._import_side_effect
        self.last_stats = p_last.start()
        self.get_instance = p_inst.start()
        self._patches = [p_imp, p_last, p_inst]

        # async_add_executor_job(func, *args) -> await func(*args)
        recorder = MagicMock()

        def _exec(func: Any, *args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        recorder.async_add_executor_job = AsyncMock(side_effect=_exec)
        self.get_instance.return_value = recorder
        return self

    def __exit__(self, *args: Any) -> None:
        for p in self._patches:
            p.stop()


# ---------------------------------------------------------------------------
# AC #1 — timestamps come from the supplier, not from poll time
# ---------------------------------------------------------------------------


class TestSupplierTimestamps:
    @pytest.mark.asyncio
    async def test_import_uses_supplier_timestamp(self):
        """Every imported StatisticData.start must equal the localized
        ``MeterReading.date`` (in UTC after astimezone conversion)."""
        coord = _make_coordinator()
        data = MinvandforsyningData(_sample_readings())

        with _RecorderPatches() as p:
            await coord.async_import_readings(data)

        assert p.import_stats.call_count == 1
        _meta, stats = p.import_stats.call_args[0][1], p.import_stats.call_args[0][2]
        assert len(stats) == len(data.readings)
        for stat, reading in zip(stats, data.readings):
            expected_utc = (
                reading.date.replace(tzinfo=_DK).astimezone(timezone.utc)
            )
            assert stat["start"].astimezone(timezone.utc) == expected_utc

    @pytest.mark.asyncio
    async def test_naive_datetime_localized_to_supplier_tz(self):
        """A naive 2026-06-05T09:00 reading must become 2026-06-05T07:00Z
        (CEST = UTC+2 in June)."""
        coord = _make_coordinator()
        data = MinvandforsyningData([
            MeterReading(
                date=datetime(2026, 6, 5, 9, 0, 0),
                reading=Decimal("400.000"),
                consumption=Decimal("100"),
            ),
        ])

        with _RecorderPatches() as p:
            await coord.async_import_readings(data)

        stats = p.import_stats.call_args[0][2]
        assert len(stats) == 1
        as_utc = stats[0]["start"].astimezone(timezone.utc)
        assert as_utc == datetime(2026, 6, 5, 7, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# AC #3 — idempotent; skip already-imported buckets
# ---------------------------------------------------------------------------


class TestIdempotency:
    @pytest.mark.asyncio
    async def test_import_skips_already_imported(self):
        """If cutoff matches the 3rd reading, only readings 4..N get imported."""
        coord = _make_coordinator()
        readings = _sample_readings()
        data = MinvandforsyningData(readings)
        cutoff_ts = readings[2].date.replace(tzinfo=_DK).timestamp()
        last_stats = {
            _DEFAULT_STATISTIC_ID: [{"start": cutoff_ts, "sum": 1.0}],
        }

        with _RecorderPatches(last_stats=last_stats) as p:
            await coord.async_import_readings(data)

        assert p.import_stats.call_count == 1
        stats = p.import_stats.call_args[0][2]
        # Readings 1, 2, 3 are <= cutoff → skipped. Readings 4, 5 imported.
        assert len(stats) == 2
        expected_first = readings[3].date.replace(tzinfo=_DK).astimezone(timezone.utc)
        expected_last = readings[4].date.replace(tzinfo=_DK).astimezone(timezone.utc)
        assert stats[0]["start"].astimezone(timezone.utc) == expected_first
        assert stats[-1]["start"].astimezone(timezone.utc) == expected_last

    @pytest.mark.asyncio
    async def test_idempotent_no_new_readings(self):
        """If cutoff equals the latest reading's timestamp, nothing is imported."""
        coord = _make_coordinator()
        readings = _sample_readings()
        data = MinvandforsyningData(readings)
        cutoff_ts = readings[-1].date.replace(tzinfo=_DK).timestamp()
        last_stats = {
            _DEFAULT_STATISTIC_ID: [{"start": cutoff_ts, "sum": 1.5}],
        }

        with _RecorderPatches(last_stats=last_stats) as p:
            await coord.async_import_readings(data)

        assert p.import_stats.call_count == 0


# ---------------------------------------------------------------------------
# AC #4 — force_full / backfill semantics + running sum
# ---------------------------------------------------------------------------


class TestForceFullAndRunningSum:
    @pytest.mark.asyncio
    async def test_force_full_ignores_cutoff_and_preserves_continuity(self):
        """force_full=True rewrites all readings while continuing from the
        latest stored sum strictly before the imported window."""
        coord = _make_coordinator()
        readings = _sample_readings()
        data = MinvandforsyningData(readings)
        first_ts = readings[0].date.replace(tzinfo=_DK).timestamp()
        cutoff_ts = readings[-1].date.replace(tzinfo=_DK).timestamp()
        last_stats = {
            _DEFAULT_STATISTIC_ID: [
                {"start": cutoff_ts, "sum": 99.0},  # inside rewritten window
                {
                    "start": first_ts - timedelta(hours=1).total_seconds(),
                    "sum": 10.0,
                },  # anchor before window
            ],
        }

        with _RecorderPatches(last_stats=last_stats) as p:
            await coord.async_import_readings(data, force_full=True)

        assert p.import_stats.call_count == 1
        stats = p.import_stats.call_args[0][2]
        assert len(stats) == len(readings)
        # Cutoff is ignored (all readings imported), but continuity is preserved
        # by starting from the latest sum before the rewritten window.
        assert stats[0]["sum"] == pytest.approx(10.1)
        assert stats[-1]["sum"] == pytest.approx(11.5)
        assert p.last_stats.call_count == 1

    @pytest.mark.asyncio
    async def test_force_full_starts_from_zero_when_no_prior_stat_exists(self):
        """If no stored bucket exists before the imported window, start at 0."""
        coord = _make_coordinator()
        readings = _sample_readings()
        data = MinvandforsyningData(readings)
        first_ts = readings[0].date.replace(tzinfo=_DK).timestamp()
        last_stats = {
            _DEFAULT_STATISTIC_ID: [
                {"start": first_ts, "sum": 3.0},
                {"start": first_ts + 3600, "sum": 3.2},
            ],
        }

        with _RecorderPatches(last_stats=last_stats) as p:
            await coord.async_import_readings(data, force_full=True)

        stats = p.import_stats.call_args[0][2]
        assert stats[0]["sum"] == pytest.approx(0.1)
        assert stats[-1]["sum"] == pytest.approx(1.5)
        assert p.last_stats.call_count == 1

    @pytest.mark.asyncio
    async def test_running_sum_continues_from_last_stat(self):
        """First new reading's sum = last_stat_sum + consumption_m3."""
        coord = _make_coordinator()
        # One reading: consumption=500L, reading=400.000 m³
        data = MinvandforsyningData([
            MeterReading(
                date=datetime(2026, 4, 11, 10),
                reading=Decimal("400.000"),
                consumption=Decimal("500"),
            ),
        ])
        # Cutoff strictly before the new reading.
        cutoff_ts = datetime(2026, 4, 11, 9, tzinfo=_DK).timestamp()
        last_stats = {
            _DEFAULT_STATISTIC_ID: [{"start": cutoff_ts, "sum": 10.0}],
        }

        with _RecorderPatches(last_stats=last_stats) as p:
            await coord.async_import_readings(data)

        stats = p.import_stats.call_args[0][2]
        assert len(stats) == 1
        assert stats[0]["sum"] == pytest.approx(10.5)

    @pytest.mark.asyncio
    async def test_running_sum_full_sequence(self):
        """_sample_readings() produces cumulative sums [0.1, 0.3, 0.6, 1.0, 1.5]."""
        coord = _make_coordinator()
        data = MinvandforsyningData(_sample_readings())

        with _RecorderPatches() as p:
            await coord.async_import_readings(data, force_full=True)

        stats = p.import_stats.call_args[0][2]
        sums = [s["sum"] for s in stats]
        assert sums == pytest.approx([0.1, 0.3, 0.6, 1.0, 1.5])


# ---------------------------------------------------------------------------
# AC #5 — option flag disables the import path entirely
# ---------------------------------------------------------------------------


class TestImportOptionFlag:
    @pytest.mark.asyncio
    async def test_no_import_when_option_disabled(self):
        """When import_statistics=False, the bg task must not be scheduled."""
        coord = _make_coordinator(import_statistics=False)
        coord._client.async_get_meter_data = AsyncMock(
            return_value=_load_fixture_bytes(),
        )

        with _RecorderPatches() as p:
            result = await coord._async_update_data()

        assert isinstance(result, MinvandforsyningData)
        # Background task scheduler must not have been invoked at all.
        coord.hass.async_create_background_task.assert_not_called()
        # And of course no import happened.
        assert p.import_stats.call_count == 0

    @pytest.mark.asyncio
    async def test_import_scheduled_when_option_enabled(self):
        """When import_statistics=True (default), the bg task is scheduled
        and named per-meter so concurrent imports for different meters are
        independently observable."""
        coord = _make_coordinator(import_statistics=True, meter="MMM1")
        coord._client.async_get_meter_data = AsyncMock(
            return_value=_load_fixture_bytes(),
        )

        with _capture_background_tasks(coord.hass) as captured:
            with _RecorderPatches():
                await coord._async_update_data()

        expected_name = f"{DOMAIN}_import_stats_MMM1"
        assert any(
            call.kwargs.get("name") == expected_name
            for call in coord.hass.async_create_background_task.call_args_list
        ), f"Expected background task named {expected_name!r}"
        # Drain the coroutines so pytest doesn't warn.
        for coro in captured:
            coro.close()


# ---------------------------------------------------------------------------
# Safety — failures and edge cases must never break the update loop
# ---------------------------------------------------------------------------


class TestSafety:
    @pytest.mark.asyncio
    async def test_import_failure_does_not_fail_update(self):
        """If async_add_external_statistics raises, _async_update_data still returns data."""
        coord = _make_coordinator(import_statistics=True)
        coord._client.async_get_meter_data = AsyncMock(
            return_value=_load_fixture_bytes(),
        )

        with _capture_background_tasks(coord.hass) as captured:
            with _RecorderPatches(import_side_effect=RuntimeError("boom")):
                result = await coord._async_update_data()
                # _async_update_data itself returns cleanly.
                assert isinstance(result, MinvandforsyningData)
                assert result.readings, "expected non-empty readings from fixture"

                # Now run the bg task and verify the safety net swallowed it.
                for coro in captured:
                    await coro  # must NOT raise

    @pytest.mark.asyncio
    async def test_no_import_with_empty_readings(self):
        """Empty readings → no recorder calls at all (short-circuit before lock)."""
        coord = _make_coordinator()
        data = MinvandforsyningData([])

        with _RecorderPatches() as p:
            await coord.async_import_readings(data)

        assert p.import_stats.call_count == 0
        assert p.last_stats.call_count == 0


# ---------------------------------------------------------------------------
# External-statistics metadata shape
# ---------------------------------------------------------------------------


class TestExternalStatisticsMetadata:
    @pytest.mark.asyncio
    async def test_metadata_uses_external_statistics_shape(self):
        """Metadata must use the namespaced statistic_id, DOMAIN as source,
        cubic-meter units, and the descriptive name shown in the picker."""
        coord = _make_coordinator()
        data = MinvandforsyningData(_sample_readings())

        with _RecorderPatches() as p:
            await coord.async_import_readings(data)

        metadata = p.import_stats.call_args[0][1]
        assert metadata["statistic_id"] == _DEFAULT_STATISTIC_ID
        assert metadata["source"] == DOMAIN
        assert metadata["unit_of_measurement"] == "m³"
        assert metadata["has_sum"] is True
        assert metadata["name"] == f"Water Meter {_DEFAULT_METER} Total Consumption"
        # unit_class is set for HA 2025.11+ unit-conversion hints.
        assert metadata["unit_class"] == "volume"
        if _HAS_STATISTIC_MEAN_TYPE:
            assert metadata["mean_type"] == StatisticMeanType.NONE

    @pytest.mark.asyncio
    async def test_statistic_id_format_namespaced(self):
        """Locked: statistic_id is `<domain>:water_meter_<n>_total` exactly."""
        coord = _make_coordinator(meter="98765")
        data = MinvandforsyningData(_sample_readings())

        with _RecorderPatches() as p:
            await coord.async_import_readings(data)

        metadata = p.import_stats.call_args[0][1]
        assert metadata["statistic_id"] == "minvandforsyning:water_meter_98765_total"
        assert metadata["source"] == "minvandforsyning"


# ---------------------------------------------------------------------------
# DST handling — autumn fold (ambiguous local time) and spring-forward
# ---------------------------------------------------------------------------


class TestDstHandling:
    @pytest.mark.asyncio
    async def test_autumn_fold_disambiguated_by_order(self):
        """On 2025-10-26 the local clock hits 02:00 twice (CEST→CET).
        Two consecutive readings with naive 02:00 must map to two distinct
        UTC instants exactly one hour apart."""
        coord = _make_coordinator()
        data = MinvandforsyningData([
            MeterReading(
                date=datetime(2025, 10, 26, 2, 0, 0),  # first 02:00 (CEST = UTC+2)
                reading=Decimal("100.000"),
                consumption=Decimal("100"),
            ),
            MeterReading(
                date=datetime(2025, 10, 26, 2, 0, 0),  # second 02:00 (CET = UTC+1)
                reading=Decimal("100.100"),
                consumption=Decimal("200"),
            ),
        ])

        with _RecorderPatches() as p:
            await coord.async_import_readings(data, force_full=True)

        stats = p.import_stats.call_args[0][2]
        assert len(stats) == 2
        first = stats[0]["start"].astimezone(timezone.utc)
        second = stats[1]["start"].astimezone(timezone.utc)
        assert first == datetime(2025, 10, 26, 0, 0, 0, tzinfo=timezone.utc)
        assert second == datetime(2025, 10, 26, 1, 0, 0, tzinfo=timezone.utc)
        assert second - first == timedelta(hours=1)

    @pytest.mark.asyncio
    async def test_spring_forward_unambiguous_hour(self):
        """On 2026-03-29 the local clock skips 02:00 → jumps to 03:00.
        Naive 03:00 is unambiguous and must convert to 01:00 UTC (CEST=UTC+2)."""
        coord = _make_coordinator()
        data = MinvandforsyningData([
            MeterReading(
                date=datetime(2026, 3, 29, 3, 0, 0),
                reading=Decimal("100.000"),
                consumption=Decimal("100"),
            ),
        ])

        with _RecorderPatches() as p:
            await coord.async_import_readings(data, force_full=True)

        stats = p.import_stats.call_args[0][2]
        assert len(stats) == 1
        assert stats[0]["start"].astimezone(timezone.utc) == datetime(
            2026, 3, 29, 1, 0, 0, tzinfo=timezone.utc,
        )


# ---------------------------------------------------------------------------
# Hour alignment defense — async_add_external_statistics rejects non-aligned
# ---------------------------------------------------------------------------


class TestHourAlignment:
    @pytest.mark.asyncio
    async def test_off_hour_reading_truncated_to_top_of_hour(self):
        """A reading at 10:42:15 must be coerced to 10:00:00 in UTC so the
        recorder doesn't reject the whole batch with HomeAssistantError."""
        coord = _make_coordinator()
        data = MinvandforsyningData([
            MeterReading(
                date=datetime(2026, 4, 11, 10, 42, 15),
                reading=Decimal("100.000"),
                consumption=Decimal("100"),
            ),
        ])

        with _RecorderPatches() as p:
            await coord.async_import_readings(data, force_full=True)

        stats = p.import_stats.call_args[0][2]
        assert len(stats) == 1
        start = stats[0]["start"]
        assert start.minute == 0
        assert start.second == 0
        assert start.microsecond == 0


# ---------------------------------------------------------------------------
# Supplier oddities — negative consumption and decreasing reading value
# ---------------------------------------------------------------------------


class TestSupplierAnomalies:
    @pytest.mark.asyncio
    async def test_negative_consumption_decrements_sum(self):
        """A negative consumption (supplier correction) must be passed through:
        the running sum decreases rather than being clamped or skipped."""
        coord = _make_coordinator()
        data = MinvandforsyningData([
            MeterReading(
                date=datetime(2026, 4, 11, 0),
                reading=Decimal("100.000"),
                consumption=Decimal("100"),
            ),
            MeterReading(
                date=datetime(2026, 4, 11, 1),
                reading=Decimal("99.950"),
                consumption=Decimal("-50"),
            ),
        ])

        with _RecorderPatches() as p:
            await coord.async_import_readings(data, force_full=True)

        stats = p.import_stats.call_args[0][2]
        assert len(stats) == 2
        # 100L → 0.1 m³; -50L → -0.05 m³ → cumulative sum 0.05
        assert stats[0]["sum"] == pytest.approx(0.1)
        assert stats[1]["sum"] == pytest.approx(0.05)

    @pytest.mark.asyncio
    async def test_decreasing_reading_value_passes_through(self):
        """A meter swap may cause reading[1].reading < reading[0].reading.
        The integration must not raise and must use the supplier value as
        the ``state`` field verbatim (no normalization)."""
        coord = _make_coordinator()
        data = MinvandforsyningData([
            MeterReading(
                date=datetime(2026, 4, 11, 0),
                reading=Decimal("500.000"),
                consumption=Decimal("0"),
            ),
            MeterReading(
                date=datetime(2026, 4, 11, 1),
                reading=Decimal("10.000"),  # new meter installed
                consumption=Decimal("100"),
            ),
        ])

        with _RecorderPatches() as p:
            await coord.async_import_readings(data, force_full=True)

        stats = p.import_stats.call_args[0][2]
        assert len(stats) == 2
        assert stats[0]["state"] == pytest.approx(500.0)
        assert stats[1]["state"] == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# Concurrency — asyncio.Lock serializes overlapping imports
# ---------------------------------------------------------------------------


class TestConcurrencyLock:
    @pytest.mark.asyncio
    async def test_parallel_imports_serialized_by_lock(self):
        """Two ``async_import_readings`` calls scheduled in parallel must
        be serialized: the second sees the cutoff committed by the first
        and skips re-importing the same readings."""
        coord = _make_coordinator()
        readings = _sample_readings()
        data = MinvandforsyningData(readings)

        # Mock get_last_statistics to flip from "empty" → "cutoff at last read"
        # after the first import call commits. This simulates the recorder
        # state propagating between the two serialized iterations.
        empty: dict[str, list[dict[str, Any]]] = {}
        cutoff_after_first = {
            _DEFAULT_STATISTIC_ID: [
                {
                    "start": readings[-1].date.replace(tzinfo=_DK).timestamp(),
                    "sum": 1.5,
                }
            ]
        }

        with _RecorderPatches() as p:
            call_index = {"n": 0}

            def _last_stats_side_effect(*_args: Any, **_kwargs: Any):
                # First .async_add_executor_job call uses empty; after the
                # first import has run, return the cutoff so the second
                # iteration sees nothing new.
                idx = call_index["n"]
                call_index["n"] += 1
                return empty if idx == 0 else cutoff_after_first

            p.last_stats.side_effect = _last_stats_side_effect

            await asyncio.gather(
                coord.async_import_readings(data),
                coord.async_import_readings(data),
            )

        # The first call imports the whole batch; the second sees the
        # cutoff and short-circuits before reaching the recorder.
        assert p.import_stats.call_count == 1


# ---------------------------------------------------------------------------
# Service — backfill_statistics registration and dispatch
# ---------------------------------------------------------------------------


class TestBackfillService:
    def test_backfill_service_registered(self):
        """_async_register_backfill_service registers when not present."""
        hass = MagicMock()
        hass.services.has_service = MagicMock(return_value=False)
        hass.services.async_register = MagicMock()

        _async_register_backfill_service(hass)

        hass.services.async_register.assert_called_once()
        args = hass.services.async_register.call_args
        assert args.args[0] == DOMAIN
        assert args.args[1] == SERVICE_BACKFILL_STATISTICS

    def test_backfill_service_register_is_idempotent(self):
        """Already-registered → no-op (don't double-register across entries)."""
        hass = MagicMock()
        hass.services.has_service = MagicMock(return_value=True)
        hass.services.async_register = MagicMock()

        _async_register_backfill_service(hass)

        hass.services.async_register.assert_not_called()

    @pytest.mark.asyncio
    async def test_backfill_service_filters_by_meter_number(self):
        """A service call with meter_number only targets that coordinator."""
        coord_a = _make_coordinator(meter="AAA")
        coord_a.data = MinvandforsyningData(_sample_readings())
        coord_a.async_import_readings = AsyncMock()

        coord_b = _make_coordinator(meter="BBB")
        coord_b.data = MinvandforsyningData(_sample_readings())
        coord_b.async_import_readings = AsyncMock()

        hass = MagicMock()
        hass.services.has_service = MagicMock(return_value=False)
        captured_handler: list[Any] = []
        hass.services.async_register = MagicMock(
            side_effect=lambda *a, **kw: captured_handler.append(a[2]),
        )
        hass.data = {DOMAIN: {"e1": coord_a, "e2": coord_b}}

        _async_register_backfill_service(hass)
        assert captured_handler, "service handler not captured"

        call = MagicMock()
        call.data = {"meter_number": "AAA"}
        await captured_handler[0](call)

        coord_a.async_import_readings.assert_awaited_once()
        # No force_full passed in the call → default False propagates to import.
        assert coord_a.async_import_readings.call_args.kwargs.get("force_full") is False
        coord_b.async_import_readings.assert_not_called()

    @pytest.mark.asyncio
    async def test_backfill_service_targets_all_when_no_filter(self):
        """Omitting meter_number backfills every configured coordinator."""
        coord_a = _make_coordinator(meter="AAA")
        coord_a.data = MinvandforsyningData(_sample_readings())
        coord_a.async_import_readings = AsyncMock()

        coord_b = _make_coordinator(meter="BBB")
        coord_b.data = MinvandforsyningData(_sample_readings())
        coord_b.async_import_readings = AsyncMock()

        hass = MagicMock()
        hass.services.has_service = MagicMock(return_value=False)
        captured_handler: list[Any] = []
        hass.services.async_register = MagicMock(
            side_effect=lambda *a, **kw: captured_handler.append(a[2]),
        )
        hass.data = {DOMAIN: {"e1": coord_a, "e2": coord_b}}

        _async_register_backfill_service(hass)
        call = MagicMock()
        call.data = {}
        await captured_handler[0](call)

        coord_a.async_import_readings.assert_awaited_once()
        coord_b.async_import_readings.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_backfill_unknown_meter_warns_and_no_import(self, caplog):
        """meter_number filter with no match must log a warning and not
        invoke any coordinator's import path."""
        coord = _make_coordinator(meter="AAA")
        coord.data = MinvandforsyningData(_sample_readings())
        coord.async_import_readings = AsyncMock()

        hass = MagicMock()
        hass.services.has_service = MagicMock(return_value=False)
        captured_handler: list[Any] = []
        hass.services.async_register = MagicMock(
            side_effect=lambda *a, **kw: captured_handler.append(a[2]),
        )
        hass.data = {DOMAIN: {"e1": coord}}

        _async_register_backfill_service(hass)
        call = MagicMock()
        call.data = {"meter_number": "DOES_NOT_EXIST"}
        with caplog.at_level("WARNING", logger="custom_components.minvandforsyning"):
            await captured_handler[0](call)

        coord.async_import_readings.assert_not_called()
        assert any(
            "No configured meter matches DOES_NOT_EXIST" in rec.message
            for rec in caplog.records
        ), f"expected warning about unknown meter, got: {[r.message for r in caplog.records]}"

    @pytest.mark.asyncio
    async def test_backfill_continues_on_per_meter_failure(self, caplog):
        """If one coordinator's import raises, remaining ones must still run."""
        good = _make_coordinator(meter="GOOD")
        good.data = MinvandforsyningData(_sample_readings())
        good.async_import_readings = AsyncMock()

        bad = _make_coordinator(meter="BAD")
        bad.data = MinvandforsyningData(_sample_readings())
        bad.async_import_readings = AsyncMock(side_effect=RuntimeError("boom"))

        hass = MagicMock()
        hass.services.has_service = MagicMock(return_value=False)
        captured_handler: list[Any] = []
        hass.services.async_register = MagicMock(
            side_effect=lambda *a, **kw: captured_handler.append(a[2]),
        )
        # Order: bad first, good second — proves the loop survives the bad one.
        hass.data = {DOMAIN: {"e1": bad, "e2": good}}

        _async_register_backfill_service(hass)
        call = MagicMock()
        call.data = {}
        with caplog.at_level("WARNING", logger="custom_components.minvandforsyning"):
            await captured_handler[0](call)

        bad.async_import_readings.assert_awaited_once()
        good.async_import_readings.assert_awaited_once()
        assert any("Backfill failed for meter BAD" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# Hydrate — one-shot multi-year LTS import on first install
# ---------------------------------------------------------------------------


class TestHydrate:
    """The hydrate path imports up to 5y of supplier history exactly once,
    and is skipped on every subsequent call thanks to the LTS-empty check."""

    @pytest.mark.asyncio
    async def test_hydrate_fetches_when_lts_empty(self):
        """Empty LTS stream → fetch full window and import every reading."""
        from custom_components.minvandforsyning.const import INITIAL_HYDRATE_DAYS

        coord = _make_coordinator()
        sample = MinvandforsyningData(_sample_readings())
        coord.async_fetch_window = AsyncMock(return_value=sample)

        with _RecorderPatches(last_stats={}) as p:
            await coord.async_hydrate()

        coord.async_fetch_window.assert_awaited_once_with(INITIAL_HYDRATE_DAYS)
        assert p.import_stats.call_count == 1
        metadata = p.import_stats.call_args[0][1]
        assert metadata["statistic_id"] == _DEFAULT_STATISTIC_ID

    @pytest.mark.asyncio
    async def test_hydrate_skips_when_lts_populated(self):
        """LTS stream already has data → no fetch, no import."""
        coord = _make_coordinator()
        coord.async_fetch_window = AsyncMock()
        cutoff_ts = datetime(2026, 4, 11, tzinfo=_DK).timestamp()
        last_stats = {_DEFAULT_STATISTIC_ID: [{"start": cutoff_ts, "sum": 99.0}]}

        with _RecorderPatches(last_stats=last_stats) as p:
            await coord.async_hydrate()

        coord.async_fetch_window.assert_not_awaited()
        assert p.import_stats.call_count == 0

    @pytest.mark.asyncio
    async def test_hydrate_uses_5_year_window_by_default(self):
        """Default window is INITIAL_HYDRATE_DAYS (1825) — the API ceiling."""
        from custom_components.minvandforsyning.const import INITIAL_HYDRATE_DAYS

        coord = _make_coordinator()
        coord.async_fetch_window = AsyncMock(
            return_value=MinvandforsyningData(_sample_readings()),
        )

        with _RecorderPatches(last_stats={}):
            await coord.async_hydrate()

        # Asserts both the value and the ceiling constant.
        assert INITIAL_HYDRATE_DAYS == 1825
        coord.async_fetch_window.assert_awaited_once_with(1825)

    @pytest.mark.asyncio
    async def test_hydrate_respects_custom_days(self):
        """Caller-provided days is forwarded unchanged to async_fetch_window."""
        coord = _make_coordinator()
        coord.async_fetch_window = AsyncMock(
            return_value=MinvandforsyningData(_sample_readings()),
        )

        with _RecorderPatches(last_stats={}):
            await coord.async_hydrate(days=365)

        coord.async_fetch_window.assert_awaited_once_with(365)

    @pytest.mark.asyncio
    async def test_hydrate_skipped_when_import_statistics_disabled(self):
        """import_statistics=False short-circuits before any recorder lookup."""
        coord = _make_coordinator(import_statistics=False)
        coord.async_fetch_window = AsyncMock()

        with _RecorderPatches(last_stats={}) as p:
            await coord.async_hydrate()

        # Must NOT touch the recorder or the fetch path.
        assert p.last_stats.call_count == 0
        coord.async_fetch_window.assert_not_awaited()
        assert p.import_stats.call_count == 0

    @pytest.mark.asyncio
    async def test_safe_hydrate_swallows_exceptions(self, caplog):
        """_safe_hydrate must never propagate — hydrate is opportunistic."""
        coord = _make_coordinator()
        coord.async_hydrate = AsyncMock(side_effect=RuntimeError("kaboom"))

        with caplog.at_level("WARNING", logger="custom_components.minvandforsyning.coordinator"):
            await coord._safe_hydrate()  # must NOT raise

        coord.async_hydrate.assert_awaited_once()
        assert any(
            "Initial hydrate failed" in rec.message for rec in caplog.records
        )

    @pytest.mark.asyncio
    async def test_hydrate_with_no_readings_logs_warning_and_does_not_import(
        self, caplog,
    ):
        """If the supplier returns no readings, log a warning and skip import."""
        coord = _make_coordinator()
        coord.async_fetch_window = AsyncMock(return_value=MinvandforsyningData([]))

        with _RecorderPatches(last_stats={}) as p:
            with caplog.at_level(
                "WARNING", logger="custom_components.minvandforsyning.coordinator",
            ):
                await coord.async_hydrate()

        assert p.import_stats.call_count == 0
        assert any(
            "Hydrate fetched 0 readings" in rec.message for rec in caplog.records
        )


# ---------------------------------------------------------------------------
# Backfill service — new fresh-fetch semantics when ``days`` is provided
# ---------------------------------------------------------------------------


class TestBackfillServiceFreshFetch:
    """When ``days`` is set, the service triggers a fresh supplier fetch via
    ``async_fetch_window`` and imports the result. When ``days`` is omitted,
    the previous cached-replay behavior is preserved."""

    @staticmethod
    def _register_handler(hass: MagicMock) -> Any:
        """Register the service against ``hass`` and return the handler."""
        captured: list[Any] = []
        hass.services.async_register = MagicMock(
            side_effect=lambda *a, **kw: captured.append(a[2]),
        )
        _async_register_backfill_service(hass)
        assert captured, "service handler was not captured"
        return captured[0]

    @pytest.mark.asyncio
    async def test_service_with_days_triggers_fresh_fetch(self):
        """days=365 → coord.async_fetch_window(365) awaited, result imported."""
        coord = _make_coordinator(meter="AAA")
        fetched = MinvandforsyningData(_sample_readings())
        coord.async_fetch_window = AsyncMock(return_value=fetched)
        coord.async_import_readings = AsyncMock()

        hass = MagicMock()
        hass.services.has_service = MagicMock(return_value=False)
        hass.data = {DOMAIN: {"e1": coord}}
        handler = self._register_handler(hass)

        call = MagicMock()
        call.data = {"days": 365}
        await handler(call)

        coord.async_fetch_window.assert_awaited_once_with(365)
        coord.async_import_readings.assert_awaited_once()
        assert coord.async_import_readings.call_args.args[0] is fetched

    @pytest.mark.asyncio
    async def test_service_without_days_uses_cache_unchanged_behavior(self):
        """No days → no fresh fetch; replay coord.data verbatim."""
        coord = _make_coordinator(meter="AAA")
        coord.data = MinvandforsyningData(_sample_readings())
        coord.async_fetch_window = AsyncMock()
        coord.async_import_readings = AsyncMock()

        hass = MagicMock()
        hass.services.has_service = MagicMock(return_value=False)
        hass.data = {DOMAIN: {"e1": coord}}
        handler = self._register_handler(hass)

        call = MagicMock()
        call.data = {}
        await handler(call)

        coord.async_fetch_window.assert_not_awaited()
        coord.async_import_readings.assert_awaited_once()
        assert coord.async_import_readings.call_args.args[0] is coord.data

    @pytest.mark.asyncio
    async def test_service_force_full_passes_through(self):
        """force_full=True propagates to async_import_readings as a kwarg."""
        coord = _make_coordinator(meter="AAA")
        coord.data = MinvandforsyningData(_sample_readings())
        coord.async_import_readings = AsyncMock()

        hass = MagicMock()
        hass.services.has_service = MagicMock(return_value=False)
        hass.data = {DOMAIN: {"e1": coord}}
        handler = self._register_handler(hass)

        call = MagicMock()
        call.data = {"force_full": True}
        await handler(call)

        coord.async_import_readings.assert_awaited_once()
        assert coord.async_import_readings.call_args.kwargs.get("force_full") is True

    @pytest.mark.asyncio
    async def test_service_without_days_and_no_cache_warns_and_skips(self, caplog):
        """No days + coord.data is None → warning, no import, no exception."""
        coord = _make_coordinator(meter="AAA")
        coord.data = None  # coordinator hasn't completed first refresh
        coord.async_fetch_window = AsyncMock()
        coord.async_import_readings = AsyncMock()

        hass = MagicMock()
        hass.services.has_service = MagicMock(return_value=False)
        hass.data = {DOMAIN: {"e1": coord}}
        handler = self._register_handler(hass)

        call = MagicMock()
        call.data = {}
        with caplog.at_level("WARNING", logger="custom_components.minvandforsyning"):
            await handler(call)  # must NOT raise

        coord.async_fetch_window.assert_not_awaited()
        coord.async_import_readings.assert_not_awaited()
        assert any(
            "no cached data" in rec.message for rec in caplog.records
        ), f"expected 'no cached data' warning, got: {[r.message for r in caplog.records]}"


# ---------------------------------------------------------------------------
# Schema — quick guard on service signature
# ---------------------------------------------------------------------------


class TestServiceSchema:
    def test_schema_accepts_empty(self):
        # ``force_full`` defaults to False, so the empty input round-trips
        # with the default applied.
        assert SERVICE_BACKFILL_SCHEMA({}) == {"force_full": False}

    def test_schema_accepts_max_days(self):
        validated = SERVICE_BACKFILL_SCHEMA({"meter_number": "12345", "days": 1825})
        assert validated == {
            "meter_number": "12345",
            "days": 1825,
            "force_full": False,
        }

    def test_schema_accepts_full(self):
        """Every field round-trips, including the boolean force_full."""
        validated = SERVICE_BACKFILL_SCHEMA(
            {"meter_number": "12345", "days": 365, "force_full": True}
        )
        assert validated == {
            "meter_number": "12345",
            "days": 365,
            "force_full": True,
        }

    def test_schema_rejects_zero_days(self):
        import voluptuous as vol
        with pytest.raises(vol.Invalid):
            SERVICE_BACKFILL_SCHEMA({"days": 0})

    def test_schema_rejects_above_cap(self):
        """The supplier API ceiling caps days at 1825 — anything higher is invalid."""
        import voluptuous as vol
        with pytest.raises(vol.Invalid):
            SERVICE_BACKFILL_SCHEMA({"days": 1826})


# ---------------------------------------------------------------------------
# Fixture / utility helpers
# ---------------------------------------------------------------------------


def _load_fixture_bytes() -> bytes:
    """Load the recorded protobuf response fixture."""
    with open("tests/fixtures/meter_data.bin", "rb") as f:
        return f.read()


class _capture_background_tasks:
    """Context manager that captures coros passed to ``hass.async_create_background_task``.

    The coros are NOT awaited — tests can choose to drain or close them.
    Without this, MagicMock would record the call and leak the coroutine,
    triggering a "never awaited" warning.
    """

    def __init__(self, hass: MagicMock) -> None:
        self._hass = hass
        self.captured: list[Any] = []

    def __enter__(self) -> list[Any]:
        def _schedule(coro: Any, name: str | None = None) -> None:
            self.captured.append(coro)

        self._hass.async_create_background_task = MagicMock(side_effect=_schedule)
        return self.captured

    def __exit__(self, *args: Any) -> None:
        # Best-effort: close any coroutines we never awaited.
        for coro in self.captured:
            if asyncio.iscoroutine(coro):
                coro.close()
