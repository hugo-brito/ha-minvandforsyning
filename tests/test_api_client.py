"""Tests for the MinVandforsyning API client."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import ClientResponseError

from custom_components.minvandforsyning.api_client import (
    AuthTokens,
    MinvandforsyningClient,
)


def _make_response(*, status=200, body=b""):
    resp = AsyncMock()
    resp.status = status
    resp.headers = {}
    resp.raise_for_status = MagicMock()
    if status >= 400:
        resp.raise_for_status.side_effect = ClientResponseError(
            request_info=MagicMock(), history=(), status=status, message="error",
        )
    resp.read = AsyncMock(return_value=body)
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


def _varint(value):
    encoded = bytearray()
    while value > 127:
        encoded.append((value & 127) | 128)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _discovery_payload(reading_date=None, *, padding=0):
    """Build a synthetic DataSet with an optional zero-valued meter reading."""
    columns = [("ReadingDate", 2), ("Reading", 12), ("Consumption", 12)]
    if padding:
        columns.append(("Padding" + "x" * padding, 1))

    data = b"\x0b\x0c" * 6 + b"\x0b"
    for name, proto_type in columns:
        encoded_name = name.encode("ascii")
        data += b"\x13\x0a" + _varint(len(encoded_name)) + encoded_name
        data += b"\x10" + _varint(proto_type) + b"\x14"

    if reading_date is not None:
        seconds = int(reading_date.timestamp())
        data += b"\x1b\x0b\x08" + _varint(seconds * 2) + b"\x10\x03\x0c"
        data += b"\x13\x08\x00\x14\x1b\x08\x00\x1c\x1c"
    return data + b"\x0c"


class TestAuthTokens:
    def test_not_expired(self):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        tokens = AuthTokens("ctx", "auth", future)
        assert not tokens.expired

    def test_expired(self):
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        tokens = AuthTokens("ctx", "auth", past)
        assert tokens.expired


class TestMinvandforsyningClient:
    @pytest.fixture
    def mock_session(self):
        return MagicMock()

    @pytest.fixture
    def client(self, mock_session):
        return MinvandforsyningClient(mock_session)

    @pytest.mark.asyncio
    async def test_get_tokens_success(self, client, mock_session):
        """Test successful token fetch."""
        expiry = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        response_data = {
            "success": True,
            "statusCode": 200,
            "payload": {
                "anonymousUserContextToken": "test_ctx_token",
                "easyAuthToken": "test_auth_token",
                "expiry": expiry,
            },
            "errors": [],
        }

        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=response_data)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.post = MagicMock(return_value=mock_resp)

        tokens = await client.async_get_tokens()
        assert tokens.context_token == "test_ctx_token"
        assert tokens.easy_auth_token == "test_auth_token"
        assert not tokens.expired

    @pytest.mark.asyncio
    async def test_get_tokens_caches(self, client, mock_session):
        """Test that tokens are cached and reused."""
        expiry = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        response_data = {
            "success": True,
            "statusCode": 200,
            "payload": {
                "anonymousUserContextToken": "ctx",
                "easyAuthToken": "auth",
                "expiry": expiry,
            },
            "errors": [],
        }

        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=response_data)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.post = MagicMock(return_value=mock_resp)

        tokens1 = await client.async_get_tokens()
        tokens2 = await client.async_get_tokens()
        assert tokens1 is tokens2
        # post should only have been called once
        assert mock_session.post.call_count == 1

    @pytest.mark.asyncio
    async def test_get_tokens_failure(self, client, mock_session):
        """Test token request that returns success=false."""
        response_data = {
            "success": False,
            "errors": ["Invalid request"],
        }

        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=response_data)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.post = MagicMock(return_value=mock_resp)

        with pytest.raises(RuntimeError, match="Token request failed"):
            await client.async_get_tokens()

    @pytest.mark.asyncio
    async def test_get_meter_data(self, client, mock_session):
        """Test meter data fetch returns binary response."""
        # Pre-set tokens so we don't need to mock the token endpoint
        client._tokens = AuthTokens(
            "ctx", "auth",
            datetime.now(timezone.utc) + timedelta(hours=1),
        )

        fake_binary = b"\x0b\x12\x34"  # dummy protobuf data
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.read = AsyncMock(return_value=fake_binary)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.get = MagicMock(return_value=mock_resp)

        result = await client.async_get_meter_data(
            "12345678", 15,
            datetime(2026, 1, 1), datetime(2026, 4, 11),
        )
        assert result == fake_binary

        # Verify correct headers were used
        call_kwargs = mock_session.get.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")
        assert headers["Authorization"] == "Bearer auth"
        assert headers["X-Context-Token"] == "ctx"

    @pytest.mark.asyncio
    async def test_discover_supplier_stops_on_429(self, client, mock_session):
        """Test that supplier scan stops when rate limited."""
        client._tokens = AuthTokens(
            "ctx", "auth",
            datetime.now(timezone.utc) + timedelta(hours=1),
        )

        mock_session.get = MagicMock(return_value=_make_response(status=429))

        with pytest.raises(ClientResponseError) as exc_info:
            await client.async_discover_supplier_id("12345678")
        assert exc_info.value.status == 429
        assert mock_session.get.call_count == 1


class TestTokenRefreshOn401:
    """Tests for the HTTP 401 token-refresh retry logic."""

    @pytest.fixture
    def mock_session(self):
        return MagicMock()

    @pytest.fixture
    def client(self, mock_session):
        c = MinvandforsyningClient(mock_session)
        c._tokens = AuthTokens(
            "stale_ctx", "stale_auth",
            datetime.now(timezone.utc) + timedelta(hours=1),
        )
        return c

    def _make_resp(self, *, status=200, body=b""):
        """Create a mock response usable as an async context manager."""
        return _make_response(status=status, body=body)

    def _set_fresh_tokens(self, client, mock_session):
        """Mock async_get_tokens to return fresh tokens after invalidation."""
        expiry = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        response_data = {
            "success": True,
            "statusCode": 200,
            "payload": {
                "anonymousUserContextToken": "fresh_ctx",
                "easyAuthToken": "fresh_auth",
                "expiry": expiry,
            },
            "errors": [],
        }
        token_resp = AsyncMock()
        token_resp.raise_for_status = MagicMock()
        token_resp.json = AsyncMock(return_value=response_data)
        token_resp.__aenter__ = AsyncMock(return_value=token_resp)
        token_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.post = MagicMock(return_value=token_resp)

    @pytest.mark.asyncio
    async def test_401_retries_with_fresh_tokens(self, client, mock_session):
        """HTTP 401 invalidates tokens, fetches new ones, and retries successfully."""
        self._set_fresh_tokens(client, mock_session)

        ok_resp = self._make_resp(status=200, body=b"\x0b\x12")
        fail_resp = self._make_resp(status=401)

        call_count = 0
        def get_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return fail_resp if call_count == 1 else ok_resp

        mock_session.get = MagicMock(side_effect=get_side_effect)

        result = await client.async_get_meter_data(
            "12345678", 15,
            datetime(2026, 1, 1), datetime(2026, 4, 11),
        )
        assert result == b"\x0b\x12"
        assert call_count == 2
        # Tokens were invalidated and re-fetched
        assert mock_session.post.call_count == 1

    @pytest.mark.asyncio
    async def test_401_retry_also_fails_raises(self, client, mock_session):
        """If retry after 401 also fails, the error propagates."""
        from aiohttp import ClientResponseError
        self._set_fresh_tokens(client, mock_session)

        fail_resp = self._make_resp(status=401)
        mock_session.get = MagicMock(return_value=fail_resp)

        with pytest.raises(ClientResponseError) as exc_info:
            await client.async_get_meter_data(
                "12345678", 15,
                datetime(2026, 1, 1), datetime(2026, 4, 11),
            )
        assert exc_info.value.status == 401

    @pytest.mark.asyncio
    async def test_401_logs_warning(self, client, mock_session, caplog):
        """HTTP 401 retry path emits a warning-level log."""
        import logging
        self._set_fresh_tokens(client, mock_session)

        ok_resp = self._make_resp(status=200, body=b"\x0b")
        fail_resp = self._make_resp(status=401)

        call_count = 0
        def get_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return fail_resp if call_count == 1 else ok_resp

        mock_session.get = MagicMock(side_effect=get_side_effect)

        with caplog.at_level(logging.WARNING):
            await client.async_get_meter_data(
                "12345678", 15,
                datetime(2026, 1, 1), datetime(2026, 4, 11),
            )

        assert any("401" in r.message for r in caplog.records)
        assert any(r.levelno == logging.WARNING for r in caplog.records if "401" in r.message)

    @pytest.mark.asyncio
    async def test_non_401_4xx_not_retried(self, client, mock_session):
        """Non-401 client errors (e.g. 400) are raised immediately without retry."""
        from aiohttp import ClientResponseError

        fail_resp = self._make_resp(status=400)
        mock_session.get = MagicMock(return_value=fail_resp)

        with pytest.raises(ClientResponseError) as exc_info:
            await client.async_get_meter_data(
                "12345678", 15,
                datetime(2026, 1, 1), datetime(2026, 4, 11),
            )
        assert exc_info.value.status == 400
        # No retry for 400 - client errors are permanent
        assert mock_session.get.call_count == 1


class TestTransientErrorRetry:
    """Retries on transient failures (5xx, 429, network, timeout) with backoff.

    Keeps LTS clean: transient hiccups don't turn into UpdateFailed that marks
    the entity unknown during HA's setup retry window.
    """

    @pytest.fixture
    def mock_session(self):
        return MagicMock()

    @pytest.fixture
    def client(self, mock_session):
        c = MinvandforsyningClient(mock_session)
        c._tokens = AuthTokens(
            "ctx", "auth",
            datetime.now(timezone.utc) + timedelta(hours=1),
        )
        return c

    @pytest.fixture(autouse=True)
    def no_sleep(self):
        """Skip actual backoff delays during tests."""
        with patch(
            "custom_components.minvandforsyning.api_client.asyncio.sleep",
            AsyncMock(return_value=None),
        ) as m:
            yield m

    def _make_resp(self, *, status=200, body=b""):
        return _make_response(status=status, body=body)

    @pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
    @pytest.mark.asyncio
    async def test_transient_status_retried_then_succeeds(
        self, client, mock_session, status
    ):
        """A single transient HTTP error is retried and the second call returns data."""
        fail_resp = self._make_resp(status=status)
        ok_resp = self._make_resp(status=200, body=b"\x0b\xAA")

        call_count = 0

        def get_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return fail_resp if call_count == 1 else ok_resp

        mock_session.get = MagicMock(side_effect=get_side_effect)

        result = await client.async_get_meter_data(
            "12345678", 15,
            datetime(2026, 1, 1), datetime(2026, 4, 11),
        )
        assert result == b"\x0b\xAA"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_transient_status_exhausts_attempts(self, client, mock_session):
        """Repeated 5xx responses eventually propagate the HTTP error."""
        from aiohttp import ClientResponseError
        from custom_components.minvandforsyning.const import API_MAX_ATTEMPTS

        fail_resp = self._make_resp(status=503)
        mock_session.get = MagicMock(return_value=fail_resp)

        with pytest.raises(ClientResponseError) as exc_info:
            await client.async_get_meter_data(
                "12345678", 15,
                datetime(2026, 1, 1), datetime(2026, 4, 11),
            )
        assert exc_info.value.status == 503
        assert mock_session.get.call_count == API_MAX_ATTEMPTS

    @pytest.mark.asyncio
    async def test_client_error_retried_then_succeeds(self, client, mock_session):
        """aiohttp.ClientError is retried."""
        from aiohttp import ClientConnectionError

        ok_resp = self._make_resp(status=200, body=b"\x0b\xBB")

        call_count = 0

        def get_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ClientConnectionError("connection reset")
            return ok_resp

        mock_session.get = MagicMock(side_effect=get_side_effect)

        result = await client.async_get_meter_data(
            "12345678", 15,
            datetime(2026, 1, 1), datetime(2026, 4, 11),
        )
        assert result == b"\x0b\xBB"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_timeout_retried(self, client, mock_session):
        """asyncio.TimeoutError is treated as transient."""
        import asyncio as _aio
        from custom_components.minvandforsyning.const import API_MAX_ATTEMPTS

        mock_session.get = MagicMock(side_effect=_aio.TimeoutError())

        with pytest.raises(_aio.TimeoutError):
            await client.async_get_meter_data(
                "12345678", 15,
                datetime(2026, 1, 1), datetime(2026, 4, 11),
            )
        assert mock_session.get.call_count == API_MAX_ATTEMPTS

    @pytest.mark.asyncio
    async def test_backoff_is_exponential(self, client, mock_session, no_sleep):
        """Delays follow base * 2**attempt."""
        from aiohttp import ClientConnectionError
        from custom_components.minvandforsyning.const import (
            API_BACKOFF_BASE_SECONDS,
        )

        mock_session.get = MagicMock(side_effect=ClientConnectionError("x"))

        with pytest.raises(ClientConnectionError):
            await client.async_get_meter_data(
                "12345678", 15,
                datetime(2026, 1, 1), datetime(2026, 4, 11),
            )

        sleep_calls = [c.args[0] for c in no_sleep.call_args_list]
        assert sleep_calls == [
            API_BACKOFF_BASE_SECONDS * 1,
            API_BACKOFF_BASE_SECONDS * 2,
        ]

    @pytest.mark.asyncio
    async def test_429_honors_retry_after_header(self, client, mock_session, no_sleep):
        """When the server sets Retry-After, we wait at least that long."""
        fail_resp = self._make_resp(status=429)
        fail_resp.headers = {"Retry-After": "7"}
        ok_resp = self._make_resp(status=200, body=b"\x0b")

        call_count = 0

        def get_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return fail_resp if call_count == 1 else ok_resp

        mock_session.get = MagicMock(side_effect=get_side_effect)

        result = await client.async_get_meter_data(
            "12345678", 15,
            datetime(2026, 1, 1), datetime(2026, 4, 11),
        )
        assert result == b"\x0b"
        # First sleep should be at least 7s (Retry-After), not the default 1s.
        first_delay = no_sleep.call_args_list[0].args[0]
        assert first_delay >= 7.0

    @pytest.mark.asyncio
    async def test_429_retry_after_non_numeric_falls_back_to_backoff(
        self, client, mock_session, no_sleep
    ):
        """Malformed Retry-After (e.g. HTTP-date) is ignored; default backoff applies."""
        from custom_components.minvandforsyning.const import API_BACKOFF_BASE_SECONDS

        fail_resp = self._make_resp(status=429)
        fail_resp.headers = {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}
        ok_resp = self._make_resp(status=200, body=b"\x0b")

        call_count = 0

        def get_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return fail_resp if call_count == 1 else ok_resp

        mock_session.get = MagicMock(side_effect=get_side_effect)

        await client.async_get_meter_data(
            "12345678", 15,
            datetime(2026, 1, 1), datetime(2026, 4, 11),
        )
        first_delay = no_sleep.call_args_list[0].args[0]
        assert first_delay == API_BACKOFF_BASE_SECONDS * 1

    def test_token_expiry_buffer_is_five_minutes(self, client):
        """Verify the expiry buffer subtracts 5 minutes, not 2."""
        # The buffer is applied in async_get_tokens; we verify by checking
        # the timedelta used in the source. This is a canary test.
        from custom_components.minvandforsyning.api_client import MinvandforsyningClient
        import inspect
        source = inspect.getsource(MinvandforsyningClient.async_get_tokens)
        assert "timedelta(minutes=5)" in source


class TestSupplierDiscovery:
    @pytest.fixture
    def mock_session(self):
        return MagicMock()

    @pytest.fixture
    def client(self, mock_session):
        client = MinvandforsyningClient(mock_session)
        client._tokens = AuthTokens(
            "ctx", "auth", datetime.now(timezone.utc) + timedelta(hours=1),
        )
        return client

    @pytest.fixture(autouse=True)
    def no_sleep(self):
        with patch(
            "custom_components.minvandforsyning.api_client.asyncio.sleep",
            AsyncMock(),
        ):
            yield

    @pytest.mark.asyncio
    async def test_small_payload_with_zero_reading_is_a_match(self, client, mock_session):
        body = _discovery_payload(datetime.now(timezone.utc))
        assert len(body) <= 910
        mock_session.get.return_value = _make_response(body=body)

        assert await client.async_discover_supplier_id("12345678") == 1
        assert mock_session.get.call_count == 1

    @pytest.mark.asyncio
    async def test_large_empty_schema_is_not_a_match(self, client, mock_session):
        empty = _discovery_payload(padding=1000)
        assert len(empty) > 910
        mock_session.get.side_effect = [
            _make_response(body=empty),
            _make_response(body=_discovery_payload(datetime.now(timezone.utc))),
        ]

        assert await client.async_discover_supplier_id("12345678") == 2
        assert mock_session.get.call_count == 2

    @pytest.mark.parametrize("reading_age_days", [0, 5])
    @pytest.mark.asyncio
    async def test_today_and_delayed_readings_are_discovered(
        self, client, mock_session, reading_age_days,
    ):
        reading_date = datetime.now(timezone.utc) - timedelta(days=reading_age_days)
        populated = _discovery_payload(reading_date, padding=1000)
        empty = _discovery_payload()

        def respond(url, *, params, headers):
            start = datetime.fromisoformat(params["DateFrom"]).date()
            end = datetime.fromisoformat(params["DateTo"]).date()
            body = populated if start <= reading_date.date() < end else empty
            return _make_response(body=body)

        mock_session.get.side_effect = respond

        assert await client.async_discover_supplier_id("12345678") == 1
        assert mock_session.get.call_count == 1

    @pytest.mark.asyncio
    async def test_extended_scan_uses_only_the_requested_range(
        self, client, mock_session,
    ):
        mock_session.get.side_effect = [
            _make_response(body=_discovery_payload()),
            _make_response(body=_discovery_payload(datetime.now(timezone.utc))),
        ]

        result = await client.async_discover_supplier_id(
            "12345678", scan_start=501, scan_end=503,
        )

        assert result == 502
        assert [
            call.kwargs["params"]["SupplierID"]
            for call in mock_session.get.call_args_list
        ] == ["501", "502"]

    @pytest.mark.parametrize("scan_start,scan_end", [(0, 10), (5, 4), (1, 501)])
    @pytest.mark.asyncio
    async def test_invalid_scan_range_sends_no_requests(
        self, client, mock_session, scan_start, scan_end,
    ):
        with pytest.raises(ValueError):
            await client.async_discover_supplier_id(
                "12345678", scan_start=scan_start, scan_end=scan_end,
            )
        mock_session.get.assert_not_called()

    @pytest.mark.parametrize("status", [403, 503])
    @pytest.mark.asyncio
    async def test_http_errors_do_not_look_like_missing_meters(
        self, client, mock_session, status,
    ):
        mock_session.get.return_value = _make_response(status=status)

        with pytest.raises(ClientResponseError) as exc_info:
            await client.async_discover_supplier_id("12345678")

        assert exc_info.value.status == status
        assert {
            call.kwargs["params"]["SupplierID"]
            for call in mock_session.get.call_args_list
        } == {"1"}

    @pytest.mark.asyncio
    async def test_401_refreshes_tokens_without_skipping_the_supplier(
        self, client, mock_session,
    ):
        fresh = AuthTokens(
            "fresh_ctx", "fresh_auth", datetime.now(timezone.utc) + timedelta(hours=1),
        )
        mock_session.get.side_effect = [
            _make_response(status=401),
            _make_response(body=_discovery_payload(datetime.now(timezone.utc))),
        ]
        with patch.object(
            client, "async_get_tokens",
            AsyncMock(side_effect=[client._tokens, fresh]),
        ):
            result = await client.async_discover_supplier_id("12345678")

        assert result == 1
        calls = mock_session.get.call_args_list
        assert [call.kwargs["params"]["SupplierID"] for call in calls] == ["1", "1"]
        assert calls[1].kwargs["headers"]["Authorization"] == "Bearer fresh_auth"

    @pytest.mark.parametrize("body", [b"\x80", b"\x0b\x0c" * 7])
    @pytest.mark.asyncio
    async def test_invalid_payload_stops_discovery(self, client, mock_session, body):
        mock_session.get.return_value = _make_response(body=body)

        with pytest.raises(ValueError):
            await client.async_discover_supplier_id("12345678")
        assert mock_session.get.call_count == 1

    @pytest.mark.asyncio
    async def test_latest_reading_uses_newest_row_in_recent_window(
        self, client, mock_session,
    ):
        body = (Path(__file__).parent / "fixtures" / "meter_data.bin").read_bytes()
        mock_session.get.return_value = _make_response(body=body)

        assert await client.async_get_latest_reading("12345678", 7) == Decimal("103.052")

        params = mock_session.get.call_args.kwargs["params"]
        today = datetime.now(timezone.utc).date()
        assert params["SupplierID"] == "7"
        assert params["DateFrom"] == (today - timedelta(days=7)).isoformat()
        assert params["DateTo"] == (today + timedelta(days=1)).isoformat()

    @pytest.mark.asyncio
    async def test_known_suppliers_are_tried_first_with_progress(
        self, client, mock_session,
    ):
        populated = _discovery_payload(datetime.now(timezone.utc))
        empty = _discovery_payload()
        mock_session.get.side_effect = lambda url, *, params, headers: _make_response(
            body=populated if params["SupplierID"] == "3" else empty
        )
        progress = []

        result = await client.async_discover_supplier_id(
            "12345678",
            scan_start=1,
            scan_end=3,
            candidates=[700, 2],
            on_progress=lambda checked, total: progress.append((checked, total)),
        )

        assert result == 3
        assert [
            call.kwargs["params"]["SupplierID"]
            for call in mock_session.get.call_args_list
        ] == ["700", "2", "1", "3"]
        assert progress == [(1, 4), (2, 4), (3, 4), (4, 4)]
