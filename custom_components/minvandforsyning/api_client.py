"""API client for the MinVandforsyning / Rambøll FAS Customer Portal."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from aiohttp import ClientSession

from .const import (
    BROKER_API_URL,
    CLIENT_APPLICATION_APP,
    CONTEXT_TOKEN_HEADER,
    METHOD_NAME,
    METER_DATA_PATH,
    SUPPLIER_DISCOVERY_THRESHOLD_BYTES,
    SUPPLIER_ID_SCAN_MAX,
    SUPPLIER_ID_SCAN_MIN,
    TARGET_API,
    TOKEN_GENERATOR_URL,
    TOKEN_PATH,
)

_LOGGER = logging.getLogger(__name__)


class AuthTokens:
    """Holds the anonymous API tokens and their expiry."""

    __slots__ = ("context_token", "easy_auth_token", "expires_at")

    def __init__(self, context_token: str, easy_auth_token: str, expires_at: datetime) -> None:
        self.context_token = context_token
        self.easy_auth_token = easy_auth_token
        self.expires_at = expires_at

    @property
    def expired(self) -> bool:
        return datetime.now(timezone.utc) >= self.expires_at


class MinvandforsyningClient:
    """HTTP client for the Rambøll FAS Customer Portal API."""

    def __init__(self, session: ClientSession) -> None:
        self._session = session
        self._tokens: AuthTokens | None = None

    async def async_get_tokens(self) -> AuthTokens:
        """Fetch anonymous API tokens, using cached tokens if still valid."""
        if self._tokens is not None and not self._tokens.expired:
            return self._tokens

        url = f"{TOKEN_GENERATOR_URL}{TOKEN_PATH}"
        payload = {
            "targetApi": TARGET_API,
            "clientApplication": CLIENT_APPLICATION_APP,
            "methodName": METHOD_NAME,
        }
        async with self._session.post(url, json=payload) as resp:
            resp.raise_for_status()
            data = await resp.json()

        if not data.get("success"):
            errors = data.get("errors", [])
            raise RuntimeError(f"Token request failed: {errors}")

        token_data = data["payload"]
        # Parse expiry from the response; subtract 2 min buffer
        expiry_str = token_data["expiry"]
        expires_at = datetime.fromisoformat(expiry_str.replace("Z", "+00:00")) - timedelta(minutes=2)

        self._tokens = AuthTokens(
            context_token=token_data["anonymousUserContextToken"],
            easy_auth_token=token_data["easyAuthToken"],
            expires_at=expires_at,
        )
        _LOGGER.debug("Refreshed anonymous tokens, expires at %s", expires_at)
        return self._tokens

    async def async_get_meter_data(
        self,
        meter_number: str,
        supplier_id: int,
        date_from: datetime,
        date_to: datetime,
    ) -> bytes:
        """Fetch raw meter data (protobuf-net-data binary)."""
        tokens = await self.async_get_tokens()

        url = f"{BROKER_API_URL}{METER_DATA_PATH}"
        params = {
            "MeterNumber": meter_number,
            "SupplierID": str(supplier_id),
            "DateFrom": date_from.strftime("%Y-%m-%d"),
            "DateTo": date_to.strftime("%Y-%m-%d"),
        }
        headers = {
            "Authorization": f"Bearer {tokens.easy_auth_token}",
            CONTEXT_TOKEN_HEADER: tokens.context_token,
        }
        async with self._session.get(url, params=params, headers=headers) as resp:
            resp.raise_for_status()
            return await resp.read()

    async def async_discover_supplier_id(self, meter_number: str) -> int | None:
        """Scan supplier IDs to find which one has data for the given meter.

        Returns the supplier ID, or None if no match found.
        """
        tokens = await self.async_get_tokens()
        url = f"{BROKER_API_URL}{METER_DATA_PATH}"

        # Use a short date range for discovery (just today)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        headers = {
            "Authorization": f"Bearer {tokens.easy_auth_token}",
            CONTEXT_TOKEN_HEADER: tokens.context_token,
        }

        for supplier_id in range(SUPPLIER_ID_SCAN_MIN, SUPPLIER_ID_SCAN_MAX + 1):
            params = {
                "MeterNumber": meter_number,
                "SupplierID": str(supplier_id),
                "DateFrom": yesterday,
                "DateTo": today,
            }
            try:
                async with self._session.get(url, params=params, headers=headers) as resp:
                    if resp.status == 429:
                        _LOGGER.warning("Rate limited during supplier scan, stopping")
                        return None
                    if resp.status != 200:
                        continue
                    data = await resp.read()
                    if len(data) > SUPPLIER_DISCOVERY_THRESHOLD_BYTES:
                        _LOGGER.info(
                            "Found supplier ID %d for meter %s (%d bytes)",
                            supplier_id, meter_number, len(data),
                        )
                        return supplier_id
            except Exception:  # noqa: BLE001
                _LOGGER.debug("Supplier %d: request failed, skipping", supplier_id)
                continue

            await asyncio.sleep(0.05)

            # Refresh tokens if needed mid-scan
            if self._tokens is not None and self._tokens.expired:
                tokens = await self.async_get_tokens()
                headers = {
                    "Authorization": f"Bearer {tokens.easy_auth_token}",
                    CONTEXT_TOKEN_HEADER: tokens.context_token,
                }

        return None
