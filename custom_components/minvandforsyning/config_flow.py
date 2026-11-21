"""Config flow for MinVandforsyning."""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import urlencode

import voluptuous as vol
from aiohttp import ClientError, ClientResponseError
from homeassistant.config_entries import ConfigEntry, ConfigFlow, OptionsFlow
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from .api_client import (
    InvalidResponseError,
    MinvandforsyningClient,
    SupplierMatch,
    TokenRequestError,
)
from .const import (
    CONF_IMPORT_STATISTICS,
    CONF_SCAN_INTERVAL,
    DEFAULT_IMPORT_STATISTICS,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    MAX_SCAN_INTERVAL,
    MIN_SCAN_INTERVAL,
    SUPPLIER_ID_SCAN_BATCH_SIZE,
    SUPPLIER_ID_SCAN_MAX,
    SUPPLIER_ID_SCAN_MIN,
    SUPPLIER_ISSUE_TEMPLATE,
    SUPPLIER_ISSUE_URL,
)
from .suppliers import KNOWN_SUPPLIERS

_LOGGER = logging.getLogger(__name__)

CONF_SUPPLIER_ID = "supplier_id"
SUPPLIER_NOT_LISTED = "not_listed"
# Danish alphabetical order puts æ, ø and å after z.
_DANISH_ORDER = str.maketrans({"æ": "{", "ø": "|", "å": "}"})

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required("meter_number"): str,
    }
)

STEP_MANUAL_SUPPLIER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_SUPPLIER_ID): vol.All(vol.Coerce(int), vol.Range(min=1)),
    }
)


def _error_key(err: Exception) -> str:
    """Map a supplier lookup failure to a translation key."""
    if isinstance(err, ClientResponseError) and err.status == 429:
        return "rate_limited"
    if isinstance(err, InvalidResponseError):
        return "invalid_response"
    if isinstance(err, (ClientError, asyncio.TimeoutError, TokenRequestError)):
        return "cannot_connect"
    return "unknown"


def _supplier_options() -> list[SelectOptionDict]:
    """List the known suppliers by name, then a way out of the list."""
    suppliers = sorted(
        KNOWN_SUPPLIERS.items(),
        key=lambda supplier: supplier[1].casefold().translate(_DANISH_ORDER),
    )
    return [
        *(
            SelectOptionDict(value=str(supplier_id), label=name)
            for supplier_id, name in suppliers
        ),
        SelectOptionDict(value=SUPPLIER_NOT_LISTED, label="My supplier isn't on the list"),
    ]


def _supplier_issue_url(supplier_id: int) -> str:
    """Return a prefilled issue link that shares only the supplier ID."""
    query = urlencode(
        {
            "template": SUPPLIER_ISSUE_TEMPLATE,
            "title": f"Add supplier {supplier_id}",
            "supplier_id": supplier_id,
        }
    )
    return f"{SUPPLIER_ISSUE_URL}?{query}"


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
        self._client: MinvandforsyningClient | None = None
        self._scan_start = SUPPLIER_ID_SCAN_MIN
        self._scan_end = SUPPLIER_ID_SCAN_MAX
        self._scan_known_suppliers = True
        self._offer_supplier_list = True
        self._discovery_task: asyncio.Task[SupplierMatch | None] | None = None
        self._discovery_error: str | None = None
        self._progress: float | None = None

    def _get_client(self) -> MinvandforsyningClient:
        if self._client is None:
            self._client = MinvandforsyningClient(async_get_clientsession(self.hass))
        return self._client

    def _scan_placeholders(self) -> dict[str, str]:
        return {
            "meter_number": self._meter_number,
            "scan_start": str(self._scan_start),
            "scan_end": str(self._scan_end),
        }

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

            # Check for duplicate. Home Assistant closes other setups of this
            # meter when one finishes, so an abandoned setup can't block it.
            await self.async_set_unique_id(self._meter_number, raise_on_progress=False)
            self._abort_if_unique_id_configured()

            return await self.async_step_supplier_method()

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )

    async def async_step_supplier_method(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Ask how to find the meter's water supplier."""
        menu_options = ["manual_supplier", "discover_supplier"]
        if KNOWN_SUPPLIERS and self._offer_supplier_list:
            menu_options.insert(0, "select_supplier")
        return self.async_show_menu(
            step_id="supplier_method",
            menu_options=menu_options,
            description_placeholders=self._scan_placeholders(),
        )

    async def async_step_select_supplier(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Choose a supplier from the known supplier list."""
        errors: dict[str, str] = {}
        if user_input is not None:
            if user_input[CONF_SUPPLIER_ID] == SUPPLIER_NOT_LISTED:
                self._offer_supplier_list = False
                return await self.async_step_supplier_method()
            errors = await self._async_validate_supplier(int(user_input[CONF_SUPPLIER_ID]))
            if not errors:
                return await self.async_step_confirm()

        schema = vol.Schema(
            {
                vol.Required(CONF_SUPPLIER_ID): SelectSelector(
                    SelectSelectorConfig(
                        options=_supplier_options(),
                        mode=SelectSelectorMode.DROPDOWN,
                        translation_key="supplier",
                    )
                ),
            }
        )
        return self.async_show_form(
            step_id="select_supplier",
            data_schema=self.add_suggested_values_to_schema(schema, user_input),
            errors=errors,
        )

    async def async_step_manual_supplier(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Enter a supplier ID manually."""
        errors: dict[str, str] = {}
        if user_input is not None:
            errors = await self._async_validate_supplier(user_input[CONF_SUPPLIER_ID])
            if not errors:
                return await self.async_step_confirm()

        return self.async_show_form(
            step_id="manual_supplier",
            data_schema=self.add_suggested_values_to_schema(
                STEP_MANUAL_SUPPLIER_SCHEMA, user_input
            ),
            errors=errors,
            description_placeholders={"meter_number": self._meter_number},
        )

    async def _async_validate_supplier(self, supplier_id: int) -> dict[str, str]:
        """Check that the supplier has recent readings for the meter."""
        try:
            # Fail fast: a Retry-After of hours would leave the form waiting.
            reading = await self._get_client().async_get_latest_reading(
                self._meter_number, supplier_id, retry_rate_limited=False,
            )
        except Exception as err:  # noqa: BLE001 - mapped to a form error
            error = _error_key(err)
            if error == "unknown":
                _LOGGER.exception("Unexpected error checking supplier %d", supplier_id)
            else:
                _LOGGER.debug("Supplier %d lookup failed", supplier_id, exc_info=True)
            return {"base": error}

        if reading is None:
            return {CONF_SUPPLIER_ID: "meter_not_found_at_supplier"}

        self._supplier_id = supplier_id
        self._latest_reading = str(reading)
        return {}

    async def async_step_discover_supplier(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Search supplier IDs while showing progress."""
        if self._discovery_task is None:
            self._discovery_error = None
            self._progress = None
            self._discovery_task = self.hass.async_create_task(
                self._async_discover_supplier()
            )
            # Show progress even if the search has already finished. Going
            # straight to progress done makes Home Assistant hand the menu
            # choice to the confirm step, which then adds the meter unasked.
            return self._async_show_search_progress()

        if not self._discovery_task.done():
            return self._async_show_search_progress()

        task, self._discovery_task = self._discovery_task, None
        try:
            match = task.result()
        except Exception as err:  # noqa: BLE001 - reported by discover_failed
            self._discovery_error = _error_key(err)
            if self._discovery_error == "unknown":
                _LOGGER.exception("Unexpected error searching for a supplier")
            else:
                _LOGGER.debug("Supplier search failed", exc_info=True)
            return self.async_show_progress_done(next_step_id="discover_failed")

        if match is None:
            return self.async_show_progress_done(next_step_id="discover_failed")

        self._supplier_id = match.supplier_id
        self._latest_reading = str(match.reading)
        return self.async_show_progress_done(next_step_id="confirm")

    @callback
    def _async_show_search_progress(self) -> dict[str, Any]:
        return self.async_show_progress(
            step_id="discover_supplier",
            progress_action="discover_supplier",
            description_placeholders=self._scan_placeholders(),
            progress_task=self._discovery_task,
        )

    async def _async_discover_supplier(self) -> SupplierMatch | None:
        """Search the flow's supplier IDs for the meter."""
        return await self._get_client().async_find_supplier(
            self._meter_number,
            scan_start=self._scan_start,
            scan_end=self._scan_end,
            candidates=list(KNOWN_SUPPLIERS) if self._scan_known_suppliers else [],
            on_progress=self._report_progress,
        )

    @callback
    def _report_progress(self, checked: int, total: int) -> None:
        progress = round(checked / total, 2)
        if progress != self._progress:
            self._progress = progress
            self.async_update_progress(progress)

    async def async_step_discover_failed(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Offer the next steps after an unsuccessful supplier search."""
        if self._discovery_error is not None:
            return self.async_abort(reason=self._discovery_error)

        next_start = self._scan_end + 1
        return self.async_show_menu(
            step_id="discover_failed",
            menu_options=["discover_next", "manual_supplier"],
            description_placeholders={
                **self._scan_placeholders(),
                "next_start": str(next_start),
                "next_end": str(next_start + SUPPLIER_ID_SCAN_BATCH_SIZE - 1),
            },
        )

    async def async_step_discover_next(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Search the next range of supplier IDs."""
        self._scan_start = self._scan_end + 1
        self._scan_end = self._scan_start + SUPPLIER_ID_SCAN_BATCH_SIZE - 1
        self._scan_known_suppliers = False
        return await self.async_step_discover_supplier()

    async def async_step_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Confirm the meter and supplier."""
        if user_input is not None:
            title = f"Meter {self._meter_number}"
            data = {
                "meter_number": self._meter_number,
                "supplier_id": self._supplier_id,
            }
            if self._supplier_id in KNOWN_SUPPLIERS:
                return self.async_create_entry(title=title, data=data)
            return self.async_create_entry(
                title=title,
                data=data,
                description="share_supplier",
                description_placeholders={
                    "supplier_id": str(self._supplier_id),
                    "issue_url": _supplier_issue_url(self._supplier_id),
                },
            )

        placeholders = {
            "meter_number": self._meter_number,
            "supplier_id": str(self._supplier_id),
            "latest_reading": self._latest_reading,
        }
        supplier_name = KNOWN_SUPPLIERS.get(self._supplier_id)
        if supplier_name is None:
            return self.async_show_form(
                step_id="confirm", description_placeholders=placeholders
            )
        # A separate step gives listed suppliers their own translated sentence.
        return self.async_show_form(
            step_id="confirm_known",
            description_placeholders={**placeholders, "supplier_name": supplier_name},
        )

    async def async_step_confirm_known(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Confirm a meter at a supplier on the list."""
        return await self.async_step_confirm(user_input)


class MinvandforsyningOptionsFlow(OptionsFlow):
    """Handle options for MinVandforsyning."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Manage the polling interval option."""
        if user_input is not None:
            # Convert minutes to seconds for storage
            user_input[CONF_SCAN_INTERVAL] = user_input[CONF_SCAN_INTERVAL] * 60
            return self.async_create_entry(title="", data=user_input)

        current = self.config_entry.options.get(
            CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL
        )
        current_import = self.config_entry.options.get(
            CONF_IMPORT_STATISTICS, DEFAULT_IMPORT_STATISTICS
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
                    ),
                    vol.Required(
                        CONF_IMPORT_STATISTICS,
                        default=current_import,
                    ): bool,
                }
            ),
        )
