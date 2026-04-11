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
