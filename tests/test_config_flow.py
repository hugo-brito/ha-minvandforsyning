"""Tests for the MinVandforsyning config flow, run through Home Assistant."""
import asyncio
import json
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlsplit

import pytest
from aiohttp import ClientConnectionError, ClientResponseError
from homeassistant import config_entries
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.data_entry_flow import (
    EVENT_DATA_ENTRY_FLOW_PROGRESS_UPDATE,
    FlowResult,
    FlowResultType,
    InvalidData,
)
from pytest_homeassistant.common import MockConfigEntry
from pytest_homeassistant.test_util.aiohttp import AiohttpClientMocker
from yarl import URL

from custom_components.minvandforsyning.api_client import (
    InvalidResponseError,
    SupplierMatch,
    TokenRequestError,
)
from custom_components.minvandforsyning.const import (
    BROKER_API_URL,
    DOMAIN,
    METER_DATA_PATH,
    TOKEN_GENERATOR_URL,
    TOKEN_PATH,
)
from custom_components.minvandforsyning.suppliers import KNOWN_SUPPLIERS

METER = "12345678"
METER_DATA_URL = URL(f"{BROKER_API_URL}{METER_DATA_PATH}")
# Taken before the known_suppliers fixture replaces the list in every test.
SHIPPED_SUPPLIERS = dict(KNOWN_SUPPLIERS)


def _http_error(status: int) -> ClientResponseError:
    return ClientResponseError(
        request_info=MagicMock(), history=(), status=status, message="error",
    )


@pytest.fixture(autouse=True)
def integration(mock_setup_entry: AsyncMock, recorder_mock: object) -> None:
    """Run every test with an in-memory recorder.

    The setup stubs come first, so they are still active when Home Assistant
    unloads the entries at the end of a test.
    """


@pytest.fixture
def mock_setup_entry() -> Generator[AsyncMock]:
    """Create entries without setting up the integration.

    Unloading is stubbed too, because the real unload expects the data that
    setup stores.
    """
    with patch(
        "custom_components.minvandforsyning.async_setup_entry", return_value=True,
    ) as mock, patch(
        "custom_components.minvandforsyning.async_unload_entry", return_value=True,
    ):
        yield mock


@pytest.fixture(autouse=True)
def known_suppliers() -> Generator[None]:
    """Replace the real supplier list with one example supplier."""
    with patch.dict(KNOWN_SUPPLIERS, {42: "Example Vandværk"}, clear=True):
        yield


@pytest.fixture
def client() -> Generator[MagicMock]:
    with patch(
        "custom_components.minvandforsyning.config_flow.MinvandforsyningClient",
        autospec=True,
    ) as client_cls:
        client = client_cls.return_value
        client.async_get_latest_reading.return_value = Decimal("103.052")
        client.async_find_supplier.return_value = None
        yield client


@pytest.fixture
def api(aioclient_mock: AiohttpClientMocker) -> AiohttpClientMocker:
    """Answer the real client's token request; tests add the meter data answers."""
    expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    aioclient_mock.post(
        f"{TOKEN_GENERATOR_URL}{TOKEN_PATH}",
        json={
            "success": True,
            "payload": {
                "anonymousUserContextToken": "context",
                "easyAuthToken": "token",
                "expiry": expiry.isoformat(),
            },
        },
    )
    return aioclient_mock


@pytest.fixture
def long_waits() -> Generator[list[float]]:
    """Record waits of a second or more instead of waiting."""
    real_sleep = asyncio.sleep
    waits: list[float] = []

    async def sleep(delay: float, *args: Any, **kwargs: Any) -> Any:
        if delay >= 1:
            waits.append(delay)
            return None
        return await real_sleep(delay, *args, **kwargs)

    with patch("asyncio.sleep", sleep):
        yield waits


async def _configure(
    hass: HomeAssistant, result: FlowResult, user_input: dict | None = None,
) -> FlowResult:
    return await hass.config_entries.flow.async_configure(result["flow_id"], user_input)


async def _supplier_menu(hass: HomeAssistant) -> FlowResult:
    """Start a setup and enter the meter number."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER},
    )
    return await _configure(hass, result, {"meter_number": METER})


async def _choose(hass: HomeAssistant, option: str) -> FlowResult:
    """Choose an option in the supplier menu of a new setup."""
    return await _configure(hass, await _supplier_menu(hass), {"next_step_id": option})


async def _finish_search(hass: HomeAssistant, result: FlowResult) -> FlowResult:
    """Let the search task finish and return the step that follows it."""
    assert result["type"] is FlowResultType.SHOW_PROGRESS
    await hass.async_block_till_done()
    return await _configure(hass, result)


def _assert_texts_show(result: FlowResult) -> None:
    """Every value the step passes appears in its text, in every language."""
    component = Path(__file__).parents[1] / "custom_components" / DOMAIN
    for path in (component / "strings.json", *(component / "translations").glob("*.json")):
        strings = json.loads(path.read_text(encoding="utf-8"))
        text = strings["config"]["step"][result["step_id"]]["description"]
        for key in result["description_placeholders"]:
            assert f"{{{key}}}" in text, (path.name, key)


async def test_meter_number_is_required(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER},
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"

    result = await _configure(hass, result, {"meter_number": "  "})
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"meter_number": "invalid_meter_number"}

    result = await _configure(hass, result, {"meter_number": f" {METER} "})
    assert result["type"] is FlowResultType.MENU
    assert result["step_id"] == "supplier_method"
    assert result["menu_options"] == [
        "select_supplier", "manual_supplier", "discover_supplier",
    ]


async def test_menu_hides_an_empty_supplier_list(hass: HomeAssistant) -> None:
    with patch.dict(KNOWN_SUPPLIERS, clear=True):
        result = await _supplier_menu(hass)

    assert result["menu_options"] == ["manual_supplier", "discover_supplier"]


async def test_configured_meter_aborts(hass: HomeAssistant) -> None:
    MockConfigEntry(
        domain=DOMAIN, unique_id=METER, data={"meter_number": METER, "supplier_id": 42},
    ).add_to_hass(hass)

    result = await _supplier_menu(hass)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


async def test_abandoned_setup_does_not_block_a_new_one(
    hass: HomeAssistant, client: MagicMock,
) -> None:
    """A setup left open, for example in a closed browser tab, must not block a retry."""
    await _supplier_menu(hass)

    result = await _supplier_menu(hass)
    assert result["type"] is FlowResultType.MENU
    result = await _configure(hass, result, {"next_step_id": "manual_supplier"})
    result = await _configure(hass, result, {"supplier_id": 900})
    result = await _configure(hass, result, {})

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["result"].unique_id == METER
    # Finishing one setup closes the other one for the same meter.
    assert hass.config_entries.flow.async_progress() == []


async def test_choose_supplier_from_the_list(
    hass: HomeAssistant, client: MagicMock,
) -> None:
    client.async_get_latest_reading.side_effect = [None, Decimal("103.052")]
    result = await _choose(hass, "select_supplier")
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "select_supplier"
    with pytest.raises(InvalidData):
        await _configure(hass, result, {"supplier_id": "7"})

    result = await _configure(hass, result, {"supplier_id": "42"})
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"supplier_id": "meter_not_found_at_supplier"}

    result = await _configure(hass, result, {"supplier_id": "42"})
    assert result["step_id"] == "confirm_known"
    assert result["description_placeholders"] == {
        "meter_number": METER,
        "supplier_id": "42",
        "supplier_name": "Example Vandværk",
        "latest_reading": "103.052",
    }
    _assert_texts_show(result)

    result = await _configure(hass, result, {})
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["result"].unique_id == METER
    assert result["data"] == {"meter_number": METER, "supplier_id": 42}
    assert result["description"] is None


async def test_supplier_list_is_in_danish_order_with_not_listed_last(
    hass: HomeAssistant,
) -> None:
    suppliers = {1: "Åby Vand", 2: "Ørsted Vand", 3: "Æble Vand", 4: "Zealand Vand"}
    with patch.dict(KNOWN_SUPPLIERS, suppliers, clear=True):
        result = await _choose(hass, "select_supplier")

    selector = result["data_schema"].schema["supplier_id"]
    options = selector.config["options"]
    assert [option["label"] for option in options[:-1]] == [
        "Zealand Vand", "Æble Vand", "Ørsted Vand", "Åby Vand",
    ]
    assert options[-1]["value"] == "not_listed"
    # Sorting in the frontend would move the last option.
    assert not selector.config.get("sort")


def test_shipped_supplier_names_are_plain_text() -> None:
    """Names come from public issues, and setup shows them as markdown."""
    assert SHIPPED_SUPPLIERS
    for supplier_id, name in SHIPPED_SUPPLIERS.items():
        assert type(supplier_id) is int and supplier_id >= 1
        assert name == name.strip() and 2 <= len(name) <= 80, name
        assert not set(name) & set("[]<>*_`#|~\\\n"), name


async def test_not_listed_returns_to_the_other_options(hass: HomeAssistant) -> None:
    result = await _choose(hass, "select_supplier")

    result = await _configure(hass, result, {"supplier_id": "not_listed"})

    assert result["type"] is FlowResultType.MENU
    assert result["step_id"] == "supplier_method"
    assert result["menu_options"] == ["manual_supplier", "discover_supplier"]


@pytest.mark.parametrize(
    ("error", "errors"),
    [
        pytest.param(None, {"supplier_id": "meter_not_found_at_supplier"}, id="no_readings"),
        pytest.param(_http_error(429), {"base": "rate_limited"}, id="rate_limited"),
        pytest.param(_http_error(503), {"base": "cannot_connect"}, id="server_error"),
        pytest.param(ClientConnectionError(), {"base": "cannot_connect"}, id="no_connection"),
        pytest.param(TimeoutError(), {"base": "cannot_connect"}, id="timeout"),
        pytest.param(
            TokenRequestError("Token request failed: []"), {"base": "cannot_connect"},
            id="no_token",
        ),
        pytest.param(
            InvalidResponseError("bad payload"), {"base": "invalid_response"}, id="bad_payload",
        ),
        pytest.param(KeyError("payload"), {"base": "unknown"}, id="unexpected"),
        # A bug, not a connection problem, so it's logged as one.
        pytest.param(RuntimeError("bug"), {"base": "unknown"}, id="runtime_error"),
    ],
)
async def test_supplier_id_errors_can_be_retried(
    hass: HomeAssistant,
    client: MagicMock,
    error: Exception | None,
    errors: dict[str, str],
) -> None:
    client.async_get_latest_reading.side_effect = [error, Decimal("103.052")]
    result = await _choose(hass, "manual_supplier")

    result = await _configure(hass, result, {"supplier_id": 900})
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "manual_supplier"
    assert result["errors"] == errors
    # The form keeps the entered ID, so it can be sent again as is.
    [field] = result["data_schema"].schema
    assert field.description == {"suggested_value": 900}

    result = await _configure(hass, result, {"supplier_id": 900})
    assert result["step_id"] == "confirm"
    result = await _configure(hass, result, {})
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["result"].unique_id == METER


async def test_a_rate_limited_supplier_check_fails_at_once(
    hass: HomeAssistant, api: AiohttpClientMocker, long_waits: list[float],
) -> None:
    """The API's Retry-After can be hours, and the form would wait for it."""
    api.get(METER_DATA_URL, status=429, headers={"Retry-After": "86400"})
    result = await _choose(hass, "manual_supplier")

    result = await _configure(hass, result, {"supplier_id": 900})

    assert result["errors"] == {"base": "rate_limited"}
    assert long_waits == []


async def test_a_meter_that_reads_zero_can_be_added(
    hass: HomeAssistant, client: MagicMock,
) -> None:
    """A new meter's first reading can be 0 m³."""
    client.async_get_latest_reading.return_value = Decimal("0")
    result = await _choose(hass, "manual_supplier")

    result = await _configure(hass, result, {"supplier_id": 900})

    assert result["step_id"] == "confirm"
    assert result["description_placeholders"]["latest_reading"] == "0"


async def test_unlisted_supplier_id_offers_a_share_link(
    hass: HomeAssistant, client: MagicMock,
) -> None:
    result = await _choose(hass, "manual_supplier")
    with pytest.raises(InvalidData):
        await _configure(hass, result, {"supplier_id": 0})

    result = await _configure(hass, result, {"supplier_id": 900})
    assert result["step_id"] == "confirm"
    assert result["description_placeholders"] == {
        "meter_number": METER, "supplier_id": "900", "latest_reading": "103.052",
    }
    _assert_texts_show(result)
    client.async_get_latest_reading.assert_awaited_once_with(
        METER, 900, retry_rate_limited=False,
    )

    result = await _configure(hass, result, {})
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["result"].unique_id == METER
    assert result["data"] == {"meter_number": METER, "supplier_id": 900}
    assert result["description"] == "share_supplier"
    url = urlsplit(result["description_placeholders"]["issue_url"])
    assert (url.scheme, url.netloc, url.path) == (
        "https", "github.com", "/hugo-brito/ha-minvandforsyning/issues/new",
    )
    # The link shares the supplier ID, never the meter number.
    assert parse_qs(url.query) == {
        "template": ["add_supplier.yml"],
        "title": ["Add supplier 900"],
        "supplier_id": ["900"],
    }


async def test_search_reports_progress_and_offers_the_next_range(
    hass: HomeAssistant, client: MagicMock,
) -> None:
    progress: list[float] = []

    @callback
    def capture_progress(event: Event) -> None:
        progress.append(event.data["progress"])

    hass.bus.async_listen(EVENT_DATA_ENTRY_FLOW_PROGRESS_UPDATE, capture_progress)

    async def search(meter_number, *, scan_start, scan_end, candidates, on_progress):
        await asyncio.sleep(0)
        # Updates are rounded, and an unchanged value sends no update.
        for checked in (1, 2, 2, 3):
            on_progress(checked, 3)
        return SupplierMatch(612, Decimal("103.052")) if scan_start == 501 else None

    client.async_find_supplier.side_effect = search

    result = await _finish_search(hass, await _choose(hass, "discover_supplier"))
    assert result["type"] is FlowResultType.MENU
    assert result["step_id"] == "discover_failed"
    assert result["menu_options"] == ["discover_next", "manual_supplier"]
    assert result["description_placeholders"] == {
        "meter_number": METER,
        "scan_start": "1",
        "scan_end": "500",
        "next_start": "501",
        "next_end": "1000",
    }

    result = await _configure(hass, result, {"next_step_id": "discover_next"})
    result = await _finish_search(hass, result)
    assert result["step_id"] == "confirm"
    assert result["description_placeholders"]["supplier_id"] == "612"

    result = await _configure(hass, result, {})
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["result"].unique_id == METER
    assert result["data"] == {"meter_number": METER, "supplier_id": 612}
    assert result["description"] == "share_supplier"
    assert [
        (call.kwargs["scan_start"], call.kwargs["scan_end"], call.kwargs["candidates"])
        for call in client.async_find_supplier.await_args_list
    ] == [(1, 500, [42]), (501, 1000, [])]
    assert progress == [0.33, 0.67, 1.0, 0.33, 0.67, 1.0]


async def test_each_further_search_checks_the_next_500_ids(
    hass: HomeAssistant, client: MagicMock,
) -> None:
    result = await _finish_search(hass, await _choose(hass, "discover_supplier"))
    for _ in range(2):
        result = await _configure(hass, result, {"next_step_id": "discover_next"})
        result = await _finish_search(hass, result)

    assert [
        (call.kwargs["scan_start"], call.kwargs["scan_end"])
        for call in client.async_find_supplier.await_args_list
    ] == [(1, 500), (501, 1000), (1001, 1500)]
    assert result["step_id"] == "discover_failed"
    assert result["menu_options"] == ["discover_next", "manual_supplier"]
    assert (
        result["description_placeholders"]["next_start"],
        result["description_placeholders"]["next_end"],
    ) == ("1501", "2000")


async def test_search_can_find_a_listed_supplier(
    hass: HomeAssistant, client: MagicMock,
) -> None:
    async def search(meter_number, **kwargs):
        await asyncio.sleep(0)
        return SupplierMatch(42, Decimal("7.5"))

    client.async_find_supplier.side_effect = search

    result = await _finish_search(hass, await _choose(hass, "discover_supplier"))

    assert result["step_id"] == "confirm_known"
    # The search already has the reading, so it doesn't ask the API again.
    assert result["description_placeholders"]["latest_reading"] == "7.5"
    client.async_get_latest_reading.assert_not_awaited()
    result = await _configure(hass, result, {})
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["result"].unique_id == METER
    assert result["data"] == {"meter_number": METER, "supplier_id": 42}
    assert result["description"] is None


async def test_a_search_that_finishes_at_once_still_asks_for_confirmation(
    hass: HomeAssistant, client: MagicMock,
) -> None:
    client.async_find_supplier.return_value = SupplierMatch(42, Decimal("103.052"))

    result = await _finish_search(hass, await _choose(hass, "discover_supplier"))

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "confirm_known"


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        pytest.param(_http_error(429), "rate_limited", id="rate_limited"),
        pytest.param(ClientConnectionError(), "cannot_connect", id="no_connection"),
        pytest.param(InvalidResponseError("bad payload"), "invalid_response", id="bad_payload"),
        pytest.param(KeyError("payload"), "unknown", id="unexpected"),
    ],
)
async def test_search_errors_abort(
    hass: HomeAssistant, client: MagicMock, error: Exception, reason: str,
) -> None:
    async def search(meter_number, **kwargs):
        await asyncio.sleep(0)
        raise error

    client.async_find_supplier.side_effect = search

    result = await _finish_search(hass, await _choose(hass, "discover_supplier"))

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == reason


async def test_closing_the_dialog_cancels_the_search(
    hass: HomeAssistant, client: MagicMock,
) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def search(meter_number, **kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    client.async_find_supplier.side_effect = search
    result = await _choose(hass, "discover_supplier")
    assert result["type"] is FlowResultType.SHOW_PROGRESS
    # Bounded waits, so a search that keeps running fails the test instead of
    # hanging it.
    async with asyncio.timeout(10):
        await started.wait()

    hass.config_entries.flow.async_abort(result["flow_id"])

    async with asyncio.timeout(10):
        await cancelled.wait()


async def _start_options(hass: HomeAssistant) -> tuple[MockConfigEntry, FlowResult]:
    entry = MockConfigEntry(
        domain=DOMAIN, unique_id=METER, data={"meter_number": METER, "supplier_id": 42},
    )
    entry.add_to_hass(hass)
    return entry, await hass.config_entries.options.async_init(entry.entry_id)


@pytest.mark.parametrize(("minutes", "seconds"), [(10, 600), (1440, 86400)])
async def test_options_store_the_polling_interval_in_seconds(
    hass: HomeAssistant, minutes: int, seconds: int,
) -> None:
    entry, result = await _start_options(hass)
    assert result["type"] is FlowResultType.FORM

    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {"scan_interval": minutes, "import_statistics": False},
    )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options == {"scan_interval": seconds, "import_statistics": False}


@pytest.mark.parametrize("minutes", [9, 1441])
async def test_options_reject_polling_more_often_than_10_minutes_or_less_than_daily(
    hass: HomeAssistant, minutes: int,
) -> None:
    _, result = await _start_options(hass)

    with pytest.raises(InvalidData):
        await hass.config_entries.options.async_configure(
            result["flow_id"], {"scan_interval": minutes, "import_statistics": True},
        )
