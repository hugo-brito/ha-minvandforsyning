# MinVandforsyning

Home Assistant integration for Danish water meters served by [minvandforsyning.dk](https://minvandforsyning.dk) (Ramboll FAS Customer Portal).

Reads your water meter data directly from the Ramboll API - no scraping, no browser, no credentials required.

## Features

- Automatic supplier discovery from your meter number
- Total consumption (m3) - works with HA's Water dashboard and Long-Term Statistics
- Hourly consumption (liters)
- Daily consumption (liters)
- Configurable polling interval (default: every hour)

## Requirements

- Home Assistant 2024.1.0 or newer
- A water meter number from minvandforsyning.dk (found on the page header or your water bill)

## Installation

### HACS (recommended)

1. Open HACS in Home Assistant
2. Click the three dots in the top right and select **Custom repositories**
3. Add `https://github.com/hugo-brito/ha-minvandforsyning` with category **Integration**
4. Search for "MinVandforsyning" and install it
5. Restart Home Assistant

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
2. Set the **Polling interval** (10 - 1440 minutes, default 60)

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

## Troubleshooting

### Water meter doesn't show up in the Energy dashboard, or the chart is empty

If you added **Total consumption** as a Water source and the chart stays empty even though the entity shows a valid number, the entity's long-term statistics are likely in a bad state (`units_changed` or similar). This can happen if an earlier install registered the entity during a transient API failure or a device-class change.

To recover:

1. Go to **Settings** > **Developer tools** > **Statistics**.
2. Look for issues listed against:
   - `sensor.water_meter_<meter_number>_total_consumption`
   - `sensor.water_meter_<meter_number>_hourly_consumption`
   - `sensor.water_meter_<meter_number>_daily_consumption`
3. Click **Fix issue** on each and choose **Delete** (or "Delete all long term statistics").
4. Wait for the next poll (up to 1 hour by default). Fresh statistics will be written with the correct unit, and the Energy dashboard chart will start populating.

You don't need to remove or re-add the Water source in the Energy dashboard - it picks up the new statistics automatically.

## License

[MIT](LICENSE)
