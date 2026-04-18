# MinVandforsyning

Home Assistant integration for Danish water meters served by [minvandforsyning.dk](https://minvandforsyning.dk) (Ramboll FAS Customer Portal).

Reads your water meter data directly from the Ramboll API - no scraping, no browser, no credentials required.

## Features

- Automatic supplier discovery from your meter number
- Total consumption (m3) - works with HA's Water dashboard and Long-Term Statistics
- Hourly consumption (liters)
- Daily consumption (liters)
- Configurable polling interval (default: every 2 hours)

## Requirements

- Home Assistant 2024.1.0 or newer
- A water meter number from minvandforsyning.dk (found on the page header or your water bill)

## Installation

### HACS (recommended)

1. Open HACS in Home Assistant
2. Search for "MinVandforsyning" and install it
3. Restart Home Assistant

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=hugo-brito&repository=ha-minvandforsyning)

### Manual

1. Copy `custom_components/minvandforsyning/` into your Home Assistant `config/custom_components/` directory
2. Restart Home Assistant

## Setup

[![Open your Home Assistant instance and start setting up a new integration.](https://my.home-assistant.io/badges/config_flow_start.svg)](https://my.home-assistant.io/redirect/config_flow_start/?domain=minvandforsyning)

Or manually:

1. Go to **Settings** > **Devices & Services** > **Add Integration**
2. Search for **MinVandforsyning**
3. Enter your meter number
4. The integration auto-discovers your water supplier and shows the latest reading for confirmation

## Configuration

After setup, you can adjust the polling interval:

1. Go to **Settings** > **Devices & Services** > **MinVandforsyning** > **Configure**
2. Set the **Polling interval** (10 - 1440 minutes, default 120)

Changes apply immediately without restarting Home Assistant.

## Sensors

| Sensor             | Unit | State class       | Description                          |
|--------------------|------|-------------------|--------------------------------------|
| Total consumption  | m3   | total_increasing  | Cumulative meter reading             |
| Hourly consumption | L    | measurement       | Consumption in the most recent hour  |
| Daily consumption  | L    | measurement       | Consumption so far today             |

The **Total consumption** sensor uses `total_increasing`, so Home Assistant automatically tracks daily, weekly, and monthly statistics. You can add it as a Water source in the Energy dashboard.

## How it works

The integration talks to the same public API that the minvandforsyning.dk website uses. It fetches anonymous access tokens (no login required) and retrieves meter data in a binary protobuf format, which is decoded locally with a pure Python parser - zero external dependencies.

## License

[MIT](LICENSE)
