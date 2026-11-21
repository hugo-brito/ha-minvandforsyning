"""Tests for the MinvandforsyningData coordinator data class."""
import logging
import threading
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
from aiohttp import ClientResponseError, ConnectionTimeoutError, RequestInfo
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from multidict import CIMultiDict, CIMultiDictProxy
from pytest_homeassistant.common import MockConfigEntry, async_fire_time_changed
from yarl import URL

from custom_components.minvandforsyning.api_client import parse_readings
from custom_components.minvandforsyning.const import (
    BROKER_API_URL,
    DOMAIN,
    METER_DATA_PATH,
    READING_DATE_TZ,
)
from custom_components.minvandforsyning.coordinator import MeterReading, MinvandforsyningCoordinator, MinvandforsyningData

_METER = "12345678"
_FIXTURE = Path(__file__).parent / "fixtures" / "meter_data.bin"


def _reading(year: int, month: int, day: int, hour: int, total: str, consumption: str) -> MeterReading:
    return MeterReading(
        date=datetime(year, month, day, hour),
        reading=Decimal(total),
        consumption=Decimal(consumption),
    )


@pytest.fixture
def sample_data() -> MinvandforsyningData:
    return MinvandforsyningData([
        _reading(2026, 4, 11, 0, "300.000", "10"),
        _reading(2026, 4, 11, 1, "300.010", "10"),
        _reading(2026, 4, 11, 2, "300.030", "20"),
        _reading(2026, 4, 12, 0, "300.050", "20"),
        _reading(2026, 4, 12, 1, "300.080", "30"),
    ])


class TestMinvandforsyningData:
    def test_latest_reading(self, sample_data: MinvandforsyningData):
        assert sample_data.latest_reading is not None
        assert sample_data.latest_reading.reading == Decimal("300.080")

    def test_latest_reading_empty(self):
        data = MinvandforsyningData([])
        assert data.latest_reading is None

    def test_total_m3(self, sample_data: MinvandforsyningData):
        assert sample_data.total_m3 == Decimal("300.080")

    def test_total_m3_empty(self):
        assert MinvandforsyningData([]).total_m3 is None

    def test_last_hour_liters(self, sample_data: MinvandforsyningData):
        assert sample_data.last_hour_liters == Decimal("30")

    def test_last_hour_liters_empty(self):
        assert MinvandforsyningData([]).last_hour_liters is None

    def test_daily_liters_specific_date(self, sample_data: MinvandforsyningData):
        apr11 = datetime(2026, 4, 11)
        assert sample_data.daily_liters(apr11) == Decimal("40")  # 10 + 10 + 20

    def test_daily_liters_another_date(self, sample_data: MinvandforsyningData):
        apr12 = datetime(2026, 4, 12)
        assert sample_data.daily_liters(apr12) == Decimal("50")  # 20 + 30

    def test_daily_liters_no_data_for_date(self, sample_data: MinvandforsyningData):
        jan1 = datetime(2026, 1, 1)
        assert sample_data.daily_liters(jan1) == Decimal("0")

    def test_daily_liters_empty_readings(self):
        data = MinvandforsyningData([])
        assert data.daily_liters(datetime(2026, 4, 12)) == Decimal("0")

    def test_daily_liters_summer_hour_ending_at_local_midnight_counts_previous_day(self):
        """Summer (CEST=UTC+2): the reading ending 22:00Z covers [21:00Z, 22:00Z)
        = 23:00-00:00 CEST on Jun 5, so it belongs to Jun 5, not Jun 6. This
        matches its statistics bucket (start 21:00Z, shown at 23:00 local)."""
        data = MinvandforsyningData([
            _reading(2026, 6, 5, 21, "300.000", "10"),  # [20:00Z, 21:00Z) = 22:00-23:00 CEST Jun 5
            _reading(2026, 6, 5, 22, "300.010", "20"),  # [21:00Z, 22:00Z) = 23:00-00:00 CEST Jun 5
        ])
        jun5 = datetime(2026, 6, 5, tzinfo=ZoneInfo(READING_DATE_TZ))
        jun6 = datetime(2026, 6, 6, tzinfo=ZoneInfo(READING_DATE_TZ))
        assert data.daily_liters(jun5) == Decimal("30")
        assert data.daily_liters(jun6) == Decimal("0")

    def test_daily_liters_winter_hour_ending_at_local_midnight_counts_previous_day(self):
        """Winter (CET=UTC+1): the reading ending 23:00Z covers [22:00Z, 23:00Z)
        = 23:00-00:00 CET on Jan 15, so it belongs to Jan 15, not Jan 16."""
        data = MinvandforsyningData([
            _reading(2026, 1, 15, 22, "300.000", "15"),  # [21:00Z, 22:00Z) = 22:00-23:00 CET Jan 15
            _reading(2026, 1, 15, 23, "300.015", "25"),  # [22:00Z, 23:00Z) = 23:00-00:00 CET Jan 15
        ])
        local_jan15 = datetime(2026, 1, 15, tzinfo=ZoneInfo(READING_DATE_TZ))
        local_jan16 = datetime(2026, 1, 16, tzinfo=ZoneInfo(READING_DATE_TZ))
        assert data.daily_liters(local_jan15) == Decimal("40")
        assert data.daily_liters(local_jan16) == Decimal("0")

    def test_daily_liters_returns_zero_for_missing_local_day(self):
        data = MinvandforsyningData([
            _reading(2026, 6, 5, 22, "300.000", "20"),  # interval start 21:00Z = 23:00 CEST Jun 5
        ])
        assert data.daily_liters(datetime(2026, 6, 5, tzinfo=ZoneInfo(READING_DATE_TZ))) == Decimal("20")
        assert data.daily_liters(datetime(2026, 6, 7, tzinfo=ZoneInfo(READING_DATE_TZ))) == Decimal("0")

    def test_daily_grouping_agrees_with_statistics_bucket(self):
        """The daily-sensor local day equals the local day of the statistics
        bucket start for the same reading, in both winter and summer, so the
        daily sensor and the Energy dashboard never disagree on ownership."""
        for month in (1, 6):  # winter (CET) and summer (CEST)
            reading = _reading(2026, month, 15, 22, "300.000", "10")
            bucket_local_day = (
                MinvandforsyningCoordinator.reading_start_utc(reading.date)
                .astimezone(ZoneInfo(READING_DATE_TZ))
                .date()
            )
            data = MinvandforsyningData([reading])
            local_midnight = datetime(
                bucket_local_day.year, bucket_local_day.month, bucket_local_day.day,
                tzinfo=ZoneInfo(READING_DATE_TZ),
            )
            assert data.daily_liters(local_midnight) == Decimal("10")


class TestCoordinatorDateRange:
    """Regression: DateTo must be tomorrow to include today's intraday data.

    The API treats DateTo as exclusive — passing today's date excludes all of
    today's readings.  This was the root cause of sensors showing midnight-stale
    data while the website had up-to-the-hour values.
    """

    @pytest.mark.asyncio
    async def test_date_to_is_tomorrow(self):
        """Coordinator must pass DateTo = now + 1 day, not DateTo = now."""
        hass = MagicMock()
        hass.async_add_executor_job = AsyncMock(side_effect=lambda func, *args: func(*args))
        client = MagicMock()
        # Make async_get_meter_data capture the args it's called with
        captured_args = {}

        async def capture_meter_data(meter, supplier, date_from, date_to):
            captured_args["date_from"] = date_from
            captured_args["date_to"] = date_to
            # Return the test fixture binary
            with open("tests/fixtures/meter_data.bin", "rb") as f:
                return f.read()

        client.async_get_meter_data = AsyncMock(side_effect=capture_meter_data)

        coordinator = MinvandforsyningCoordinator(
            hass, MockConfigEntry(domain=DOMAIN), client,
            meter_number="99999999", supplier_id=1,
            import_statistics=False,
        )

        frozen_now = datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)
        with patch(
            "custom_components.minvandforsyning.coordinator.datetime",
        ) as mock_dt:
            mock_dt.now.return_value = frozen_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            await coordinator._async_update_data()

        assert "date_to" in captured_args
        date_to = captured_args["date_to"]
        # DateTo must be at least tomorrow relative to "now"
        assert date_to.date() > frozen_now.date(), (
            f"DateTo ({date_to.date()}) must be after today ({frozen_now.date()}) "
            "because the API treats DateTo as exclusive"
        )


def test_coordinator_belongs_to_its_config_entry():
    """Home Assistant uses the entry to stop polling when the entry unloads."""
    entry = MockConfigEntry(domain=DOMAIN)

    coordinator = MinvandforsyningCoordinator(
        MagicMock(), entry, MagicMock(), meter_number="99999999", supplier_id=1,
    )

    assert coordinator.config_entry is entry


def _rate_limited(retry_after: int) -> ClientResponseError:
    """A 429 as aiohttp raises it, with the request's URL and the API's pause."""
    url = URL(f"{BROKER_API_URL}{METER_DATA_PATH}").with_query(
        MeterNumber=_METER, SupplierID="1",
    )
    return ClientResponseError(
        RequestInfo(url, "GET", CIMultiDictProxy(CIMultiDict()), url),
        (),
        status=429,
        message="Too Many Requests",
        headers=CIMultiDictProxy(CIMultiDict({"Retry-After": str(retry_after)})),
    )


@pytest.fixture
def meter_api() -> Generator[MagicMock]:
    """Mock the supplier API client that the integration creates."""
    with patch(
        "custom_components.minvandforsyning.MinvandforsyningClient", autospec=True,
    ) as client_cls:
        client_cls.return_value.async_get_meter_data.return_value = _FIXTURE.read_bytes()
        yield client_cls.return_value


def _add_meter(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=_METER,
        data={"meter_number": _METER, "supplier_id": 1},
        options={"import_statistics": False},
    )
    entry.add_to_hass(hass)
    return entry


async def _set_up_meter(hass: HomeAssistant) -> MinvandforsyningCoordinator:
    entry = _add_meter(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return hass.data[DOMAIN][entry.entry_id]


async def _rate_limit_a_poll(
    hass: HomeAssistant, meter_api: MagicMock, retry_after: int,
) -> int:
    """Poll once while the API limits requests; return the requests so far."""
    coordinator = await _set_up_meter(hass)
    meter_api.async_get_meter_data.side_effect = _rate_limited(retry_after)
    await coordinator.async_refresh()
    assert not coordinator.last_update_success
    meter_api.async_get_meter_data.side_effect = None
    return meter_api.async_get_meter_data.await_count


async def _requests_after(hass: HomeAssistant, meter_api: MagicMock, delay: timedelta) -> int:
    async_fire_time_changed(hass, dt_util.utcnow() + delay)
    # Scheduled polls run as background tasks.
    await hass.async_block_till_done(wait_background_tasks=True)
    return meter_api.async_get_meter_data.await_count


@pytest.mark.usefixtures("recorder_mock")
@pytest.mark.parametrize(
    ("retry_after", "quiet_for", "polled_after"),
    [
        pytest.param(
            7200, timedelta(hours=1, minutes=1), timedelta(hours=2, minutes=1),
            id="longer_than_the_interval",
        ),
        pytest.param(
            30 * 86400, timedelta(hours=23), timedelta(hours=24, minutes=1),
            id="at_most_a_day",
        ),
    ],
)
async def test_a_rate_limited_poll_waits_as_long_as_the_api_asks(
    hass: HomeAssistant,
    meter_api: MagicMock,
    caplog: pytest.LogCaptureFixture,
    retry_after: int,
    quiet_for: timedelta,
    polled_after: timedelta,
) -> None:
    requests = await _rate_limit_a_poll(hass, meter_api, retry_after)

    assert await _requests_after(hass, meter_api, quiet_for) == requests
    assert await _requests_after(hass, meter_api, polled_after) == requests + 1
    # Home Assistant logs the failure, without the request and its meter number.
    errors = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors == [f"Error fetching {DOMAIN} data: Error fetching meter data: HTTP 429"]


@pytest.mark.usefixtures("recorder_mock")
async def test_a_short_rate_limit_does_not_poll_sooner_than_usual(
    hass: HomeAssistant, meter_api: MagicMock,
) -> None:
    """Home Assistant polls again after the API's pause instead of the interval."""
    requests = await _rate_limit_a_poll(hass, meter_api, 120)

    assert await _requests_after(hass, meter_api, timedelta(minutes=59)) == requests
    assert await _requests_after(hass, meter_api, timedelta(hours=1, minutes=1)) == requests + 1


@pytest.mark.usefixtures("recorder_mock")
async def test_the_longest_pause_does_not_make_a_poll_come_sooner(
    hass: HomeAssistant, meter_api: MagicMock,
) -> None:
    """The polling interval stays the shortest wait, even above the longest pause."""
    with patch(
        "custom_components.minvandforsyning.coordinator.RATE_LIMIT_MAX_PAUSE_SECONDS", 1800,
    ):
        requests = await _rate_limit_a_poll(hass, meter_api, 30 * 86400)

    assert await _requests_after(hass, meter_api, timedelta(minutes=59)) == requests
    assert await _requests_after(hass, meter_api, timedelta(hours=1, minutes=1)) == requests + 1


@pytest.mark.usefixtures("recorder_mock")
async def test_a_rate_limited_setup_is_retried_when_the_api_asks(
    hass: HomeAssistant, meter_api: MagicMock,
) -> None:
    meter_api.async_get_meter_data.side_effect = _rate_limited(120)
    entry = _add_meter(hass)

    assert not await hass.config_entries.async_setup(entry.entry_id)

    assert entry.state is ConfigEntryState.SETUP_RETRY
    # The integrations page shows this.
    assert entry.reason == "Error fetching meter data: HTTP 429"
    meter_api.async_get_meter_data.side_effect = None
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=110))
    await hass.async_block_till_done(wait_background_tasks=True)
    assert entry.state is ConfigEntryState.SETUP_RETRY
    # Setup retries run as background tasks.
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=125))
    await hass.async_block_till_done(wait_background_tasks=True)
    assert entry.state is ConfigEntryState.LOADED


@pytest.mark.parametrize(
    ("error", "shown"),
    [
        pytest.param(
            ConnectionTimeoutError(
                "Connection timeout to host "
                f"{URL(f'{BROKER_API_URL}{METER_DATA_PATH}').with_query(MeterNumber=_METER)}"
            ),
            f"Connection timeout to host {BROKER_API_URL}{METER_DATA_PATH}",
            id="text_with_the_request",
        ),
        pytest.param(TimeoutError(), "TimeoutError", id="no_text"),
    ],
)
@pytest.mark.usefixtures("recorder_mock")
async def test_a_connection_error_message_leaves_out_the_meter_number(
    hass: HomeAssistant,
    meter_api: MagicMock,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
    shown: str,
) -> None:
    """Home Assistant logs this message, and shows it on the integrations page when setup fails.

    aiohttp's text for some errors has the request URL, with the meter number.
    """
    coordinator = await _set_up_meter(hass)
    meter_api.async_get_meter_data.side_effect = error

    await coordinator.async_refresh()

    errors = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors == [f"Error fetching {DOMAIN} data: Error fetching meter data: {shown}"]


@pytest.mark.usefixtures("recorder_mock")
async def test_readings_are_parsed_outside_the_event_loop(
    hass: HomeAssistant, meter_api: MagicMock,
) -> None:
    """Years of history take seconds to parse on a small computer."""
    coordinator = await _set_up_meter(hass)
    event_loop_thread = threading.current_thread()
    threads: list[threading.Thread] = []

    def parse(raw: bytes, now: datetime) -> list[MeterReading]:
        threads.append(threading.current_thread())
        return parse_readings(raw, now)

    # A plain function, because Home Assistant's test instance runs a mock
    # given to the executor right away, in the event loop.
    with patch("custom_components.minvandforsyning.coordinator.parse_readings", new=parse):
        await coordinator.async_refresh()

    assert coordinator.last_update_success
    assert threads
    assert event_loop_thread not in threads


@pytest.mark.usefixtures("recorder_mock")
async def test_a_poll_without_the_reading_columns_fails(
    hass: HomeAssistant, meter_api: MagicMock,
) -> None:
    """Setup rejects a response like this, so a poll reports it too."""
    coordinator = await _set_up_meter(hass)
    meter_api.async_get_meter_data.return_value = b"\x0b\x0c" * 7

    await coordinator.async_refresh()

    assert not coordinator.last_update_success
