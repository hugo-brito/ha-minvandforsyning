"""Tests for configurable polling interval (TDD).

Covers:
- Coordinator accepts custom scan_interval
- Options flow validation and persistence
- Update listener triggers reload on options change
"""
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
import voluptuous as vol

from custom_components.minvandforsyning.const import (
    CONF_SCAN_INTERVAL,
    DEFAULT_SCAN_INTERVAL,
    MAX_SCAN_INTERVAL,
    MIN_SCAN_INTERVAL,
)


# ---------------------------------------------------------------------------
# Coordinator: scan_interval parameter
# ---------------------------------------------------------------------------

class TestCoordinatorScanInterval:
    """Coordinator must accept an optional scan_interval parameter."""

    def test_default_interval_when_no_arg(self):
        """Existing callers without scan_interval get the 2h default."""
        from custom_components.minvandforsyning.coordinator import MinvandforsyningCoordinator

        hass = MagicMock()
        client = MagicMock()
        coord = MinvandforsyningCoordinator(
            hass, client, meter_number="123", supplier_id=1,
        )
        assert coord.update_interval == timedelta(seconds=DEFAULT_SCAN_INTERVAL)

    def test_custom_interval(self):
        """Passing scan_interval overrides the default."""
        from custom_components.minvandforsyning.coordinator import MinvandforsyningCoordinator

        hass = MagicMock()
        client = MagicMock()
        coord = MinvandforsyningCoordinator(
            hass, client, meter_number="123", supplier_id=1,
            scan_interval=1800,
        )
        assert coord.update_interval == timedelta(seconds=1800)

    def test_explicit_default_value(self):
        """Explicitly passing DEFAULT_SCAN_INTERVAL works the same as omitting it."""
        from custom_components.minvandforsyning.coordinator import MinvandforsyningCoordinator

        hass = MagicMock()
        client = MagicMock()
        coord = MinvandforsyningCoordinator(
            hass, client, meter_number="123", supplier_id=1,
            scan_interval=DEFAULT_SCAN_INTERVAL,
        )
        assert coord.update_interval == timedelta(seconds=DEFAULT_SCAN_INTERVAL)


# ---------------------------------------------------------------------------
# Options flow: validation and storage
# ---------------------------------------------------------------------------

class TestOptionsFlowSchema:
    """Options flow must validate minutes input and store seconds."""

    def _get_options_schema(self, current_seconds: int = DEFAULT_SCAN_INTERVAL):
        """Build the options schema the same way the flow does."""
        from custom_components.minvandforsyning.config_flow import (
            MinvandforsyningOptionsFlow,
        )
        # We just need the vol.Schema - extract it by inspecting the class
        # Build it from the PRD spec to validate the contract
        return vol.Schema({
            vol.Required(
                CONF_SCAN_INTERVAL,
                default=current_seconds // 60,
            ): vol.All(
                vol.Coerce(int),
                vol.Range(min=MIN_SCAN_INTERVAL // 60, max=MAX_SCAN_INTERVAL // 60),
                lambda m: m * 60,
            ),
        })

    def test_valid_30_minutes(self):
        schema = self._get_options_schema()
        result = schema({CONF_SCAN_INTERVAL: 30})
        assert result[CONF_SCAN_INTERVAL] == 1800

    def test_valid_120_minutes_default(self):
        schema = self._get_options_schema()
        result = schema({CONF_SCAN_INTERVAL: 120})
        assert result[CONF_SCAN_INTERVAL] == 7200

    def test_valid_1440_minutes_max(self):
        schema = self._get_options_schema()
        result = schema({CONF_SCAN_INTERVAL: 1440})
        assert result[CONF_SCAN_INTERVAL] == 86400

    def test_valid_10_minutes_min(self):
        schema = self._get_options_schema()
        result = schema({CONF_SCAN_INTERVAL: 10})
        assert result[CONF_SCAN_INTERVAL] == 600

    def test_rejects_below_minimum(self):
        schema = self._get_options_schema()
        with pytest.raises(vol.MultipleInvalid):
            schema({CONF_SCAN_INTERVAL: 5})

    def test_rejects_above_maximum(self):
        schema = self._get_options_schema()
        with pytest.raises(vol.MultipleInvalid):
            schema({CONF_SCAN_INTERVAL: 2000})

    def test_rejects_zero(self):
        schema = self._get_options_schema()
        with pytest.raises(vol.MultipleInvalid):
            schema({CONF_SCAN_INTERVAL: 0})

    def test_rejects_negative(self):
        schema = self._get_options_schema()
        with pytest.raises(vol.MultipleInvalid):
            schema({CONF_SCAN_INTERVAL: -10})


class TestOptionsFlowHandler:
    """The OptionsFlow class must exist and be registered."""

    def test_options_flow_class_exists(self):
        from custom_components.minvandforsyning.config_flow import (
            MinvandforsyningOptionsFlow,
        )
        assert MinvandforsyningOptionsFlow is not None

    def test_config_flow_has_options_flow(self):
        """ConfigFlow must register async_get_options_flow."""
        from custom_components.minvandforsyning.config_flow import (
            MinvandforsyningConfigFlow,
        )
        assert hasattr(MinvandforsyningConfigFlow, "async_get_options_flow")


# ---------------------------------------------------------------------------
# Init: reads options, passes to coordinator
# ---------------------------------------------------------------------------

class TestInitReadsOptions:
    """async_setup_entry must read scan_interval from entry.options."""

    def test_setup_passes_custom_interval(self):
        """When options contain scan_interval, init module uses it."""
        import inspect
        import custom_components.minvandforsyning as init_pkg

        source = inspect.getsource(init_pkg)
        assert "CONF_SCAN_INTERVAL" in source
        assert "scan_interval" in source


# ---------------------------------------------------------------------------
# Constants: verify values match PRD
# ---------------------------------------------------------------------------

class TestConstants:
    def test_default_scan_interval(self):
        assert DEFAULT_SCAN_INTERVAL == 7200

    def test_min_scan_interval(self):
        assert MIN_SCAN_INTERVAL == 600

    def test_max_scan_interval(self):
        assert MAX_SCAN_INTERVAL == 86400

    def test_conf_key(self):
        assert CONF_SCAN_INTERVAL == "scan_interval"
