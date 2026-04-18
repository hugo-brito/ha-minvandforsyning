"""Tests for the MinVandforsyning API client."""
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.minvandforsyning.api_client import (
    AuthTokens,
    MinvandforsyningClient,
)


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

        mock_resp = AsyncMock()
        mock_resp.status = 429
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session.get = MagicMock(return_value=mock_resp)

        result = await client.async_discover_supplier_id("12345678")
        assert result is None
        # Should stop after first 429, not continue all 300
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
        resp = AsyncMock()
        resp.status = status
        resp.raise_for_status = MagicMock()
        if status >= 400:
            from aiohttp import ClientResponseError
            resp.raise_for_status.side_effect = ClientResponseError(
                request_info=MagicMock(), history=(), status=status, message="error",
            )
        resp.read = AsyncMock(return_value=body)
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=False)
        return resp

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
        resp = AsyncMock()
        resp.status = status
        resp.headers = {}
        resp.raise_for_status = MagicMock()
        if status >= 400:
            from aiohttp import ClientResponseError
            resp.raise_for_status.side_effect = ClientResponseError(
                request_info=MagicMock(), history=(), status=status, message="error",
            )
        resp.read = AsyncMock(return_value=body)
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=False)
        return resp

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
