"""API client for the MinVandforsyning / Rambøll FAS Customer Portal."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import NamedTuple

from aiohttp import ClientError, ClientResponseError, ClientSession, ContentTypeError

from .const import (
    API_BACKOFF_BASE_SECONDS,
    API_MAX_ATTEMPTS,
    API_RETRY_AFTER_MAX_SECONDS,
    API_TRANSIENT_STATUS_CODES,
    BROKER_API_URL,
    CLIENT_APPLICATION_APP,
    COL_CONSUMPTION,
    COL_READING,
    COL_READING_DATE,
    CONTEXT_TOKEN_HEADER,
    METHOD_NAME,
    METER_DATA_PATH,
    READINGS_TABLE_INDEX,
    RECENT_READINGS_DAYS,
    SUPPLIER_ID_SCAN_BATCH_SIZE,
    SUPPLIER_ID_SCAN_MAX,
    SUPPLIER_ID_SCAN_MIN,
    SUPPLIER_SCAN_DELAY_SECONDS,
    TARGET_API,
    TOKEN_GENERATOR_URL,
    TOKEN_PATH,
)
from .protobuf_parser import parse_dataset

_LOGGER = logging.getLogger(__name__)

_READING_COLUMNS = frozenset({COL_READING_DATE, COL_READING, COL_CONSUMPTION})
# A reading dated outside these limits can't be real. Before 1970, the hour
# arithmetic can fail; after now, it would become the newest statistic, and
# every later import would skip the real readings before it. The tolerance
# allows for a clock that is slightly behind.
_EARLIEST_READING_DATE = datetime(1970, 1, 1, tzinfo=timezone.utc)
_FUTURE_READING_TOLERANCE = timedelta(hours=1)


class InvalidResponseError(ValueError):
    """Raised when the API returns data that is not readable meter data."""


class TokenRequestError(Exception):
    """Raised when the token service doesn't issue anonymous tokens."""


class MeterReading:
    """A single hourly meter reading."""

    __slots__ = ("date", "reading", "consumption")

    def __init__(self, date: datetime, reading: Decimal, consumption: Decimal) -> None:
        self.date = date
        self.reading = reading
        self.consumption = consumption


class SupplierMatch(NamedTuple):
    """A supplier with recent readings for a meter, and the newest reading."""

    supplier_id: int
    reading: Decimal


def _as_utc(value: datetime) -> datetime:
    """Return a reading date in UTC. The API's dates have no zone and are UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_readings(raw: bytes, now: datetime) -> list[MeterReading]:
    """Return the hourly readings in a meter data response, in the API's order.

    Rows without a date, reading and consumption are skipped, and so are rows
    with a date that can't be real or a value that isn't a decimal. Raises
    InvalidResponseError when the response cannot be parsed or lacks the
    readings table.
    """
    try:
        tables = parse_dataset(raw)
    except Exception as err:  # noqa: BLE001 - malformed input fails in several ways
        raise InvalidResponseError("Could not parse the meter data response") from err

    if len(tables) <= READINGS_TABLE_INDEX:
        raise InvalidResponseError(
            f"Response has {len(tables)} tables, expected at least {READINGS_TABLE_INDEX + 1}"
        )
    table = tables[READINGS_TABLE_INDEX]
    if not _READING_COLUMNS.issubset(column.name for column in table.columns):
        raise InvalidResponseError("Response is missing the meter reading columns")

    latest = now + _FUTURE_READING_TOLERANCE
    readings: list[MeterReading] = []
    complete = 0
    for row in table.rows:
        date = row.get(COL_READING_DATE)
        reading = row.get(COL_READING)
        consumption = row.get(COL_CONSUMPTION)
        if date is None or reading is None or consumption is None:
            continue
        complete += 1
        if (
            isinstance(date, datetime)
            and isinstance(reading, Decimal)
            and isinstance(consumption, Decimal)
            and _EARLIEST_READING_DATE <= _as_utc(date) <= latest
        ):
            readings.append(MeterReading(date, reading, consumption))

    if len(readings) < complete:
        _LOGGER.warning(
            "Ignored %d of %d meter readings with an unexpected date or value",
            complete - len(readings),
            complete,
        )
    return readings


def retry_after_seconds(headers: Mapping[str, str] | None) -> int | None:
    """Return the pause a Retry-After header asks for, in seconds.

    Returns None without the header, or for its HTTP date form.
    """
    value = (headers or {}).get("Retry-After", "")
    return int(value) if value.isdecimal() else None


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
            try:
                data = await resp.json()
            except (ContentTypeError, ValueError) as err:
                raise TokenRequestError("The token service didn't answer with JSON") from err

        if not data.get("success"):
            errors = data.get("errors", [])
            raise TokenRequestError(f"Token request failed: {errors}")

        token_data = data["payload"]
        # Parse expiry from the response; subtract 5 min buffer
        expiry_str = token_data["expiry"]
        expires_at = datetime.fromisoformat(expiry_str.replace("Z", "+00:00")) - timedelta(minutes=5)

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
        *,
        retry_rate_limited: bool = True,
    ) -> bytes:
        """Fetch raw meter data (protobuf-net-data binary).

        Retries on transient failures (5xx, 429, network errors, timeouts) with
        bounded exponential backoff. A single 401 response triggers a token
        refresh and counts as one attempt. HTTP 429 is raised immediately with
        ``retry_rate_limited=False``, or when its Retry-After is longer than
        ``API_RETRY_AFTER_MAX_SECONDS``.
        """
        tokens = await self.async_get_tokens()

        url = f"{BROKER_API_URL}{METER_DATA_PATH}"
        params = {
            "MeterNumber": meter_number,
            "SupplierID": str(supplier_id),
            "DateFrom": date_from.strftime("%Y-%m-%d"),
            "DateTo": date_to.strftime("%Y-%m-%d"),
        }
        auth_retried = False

        for attempt in range(API_MAX_ATTEMPTS):
            last_attempt = attempt == API_MAX_ATTEMPTS - 1
            headers = {
                "Authorization": f"Bearer {tokens.easy_auth_token}",
                CONTEXT_TOKEN_HEADER: tokens.context_token,
            }
            try:
                async with self._session.get(url, params=params, headers=headers) as resp:
                    if resp.status == 401:
                        # The API no longer accepts these tokens.
                        self._tokens = None
                        if not auth_retried and not last_attempt:
                            _LOGGER.warning(
                                "Received HTTP 401 from API; invalidating cached tokens and retrying"
                            )
                            tokens = await self.async_get_tokens()
                            auth_retried = True
                            continue
                    if resp.status in API_TRANSIENT_STATUS_CODES and not last_attempt:
                        delay = API_BACKOFF_BASE_SECONDS * (2 ** attempt)
                        if resp.status == 429:
                            retry_after = retry_after_seconds(resp.headers)
                            # Trying again before the pause ends would only get
                            # another 429, so a long pause is left to the caller.
                            if not retry_rate_limited or (
                                retry_after is not None
                                and retry_after > API_RETRY_AFTER_MAX_SECONDS
                            ):
                                resp.raise_for_status()
                            if retry_after is not None:
                                delay = max(delay, retry_after)
                        _LOGGER.warning(
                            "HTTP %d from API, retrying in %.1fs (attempt %d/%d)",
                            resp.status, delay, attempt + 1, API_MAX_ATTEMPTS,
                        )
                        await asyncio.sleep(delay)
                        continue
                    # Success path, or final attempt on a transient status:
                    # raise_for_status turns 4xx/5xx into ClientResponseError,
                    # which the except below propagates.
                    resp.raise_for_status()
                    return await resp.read()
            except ClientResponseError:
                # HTTP status error: fail fast. Transient statuses that reach
                # here did so only because retries were exhausted.
                raise
            except (ClientError, asyncio.TimeoutError) as err:
                if last_attempt:
                    raise
                delay = API_BACKOFF_BASE_SECONDS * (2 ** attempt)
                _LOGGER.warning(
                    "Transient error %r, retrying in %.1fs (attempt %d/%d)",
                    err, delay, attempt + 1, API_MAX_ATTEMPTS,
                )
                await asyncio.sleep(delay)

        # Unreachable: the last attempt returns a payload or raises, because
        # every retry above needs an attempt left.
        raise RuntimeError("async_get_meter_data: retries exhausted without response")

    async def async_get_latest_reading(
        self,
        meter_number: str,
        supplier_id: int,
        *,
        retry_rate_limited: bool = True,
    ) -> Decimal | None:
        """Return the newest recent meter reading, or None if there is none.

        Raises InvalidResponseError when the response cannot be parsed or lacks
        the readings table.
        """
        now = datetime.now(timezone.utc)
        raw = await self.async_get_meter_data(
            meter_number,
            supplier_id,
            now - timedelta(days=RECENT_READINGS_DAYS),
            # DateTo is exclusive, so tomorrow includes today's readings.
            now + timedelta(days=1),
            retry_rate_limited=retry_rate_limited,
        )
        readings = parse_readings(raw, now)
        return readings[-1].reading if readings else None

    async def async_find_supplier(
        self,
        meter_number: str,
        *,
        scan_start: int = SUPPLIER_ID_SCAN_MIN,
        scan_end: int = SUPPLIER_ID_SCAN_MAX,
        candidates: Iterable[int] = (),
        on_progress: Callable[[int, int], None] | None = None,
    ) -> SupplierMatch | None:
        """Find the supplier with recent readings for the given meter.

        Tries ``candidates`` first, then ``scan_start`` to ``scan_end``
        inclusive. Returns the first supplier with readings, with its newest
        reading, or None. HTTP errors propagate, and HTTP 429 stops the search
        without retrying.
        """
        if (
            scan_start < 1
            or scan_end < scan_start
            or scan_end - scan_start + 1 > SUPPLIER_ID_SCAN_BATCH_SIZE
        ):
            raise ValueError(
                f"Invalid supplier ID range {scan_start}-{scan_end}: expected 1 to "
                f"{SUPPLIER_ID_SCAN_BATCH_SIZE} IDs starting at 1 or higher"
            )

        supplier_ids = list(dict.fromkeys([*candidates, *range(scan_start, scan_end + 1)]))
        for checked, supplier_id in enumerate(supplier_ids, start=1):
            if checked > 1:
                await asyncio.sleep(SUPPLIER_SCAN_DELAY_SECONDS)
            reading = await self.async_get_latest_reading(
                meter_number, supplier_id, retry_rate_limited=False,
            )
            if on_progress is not None:
                on_progress(checked, len(supplier_ids))
            if reading is not None:
                _LOGGER.info("Found supplier ID %d for meter %s", supplier_id, meter_number)
                return SupplierMatch(supplier_id, reading)

        return None
