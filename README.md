# MinVandforsyning

Home Assistant integration for Danish water meters served by [minvandforsyning.dk](https://minvandforsyning.dk) (Ramboll FAS Customer Portal).

Reads your water meter data directly from the Ramboll API - no scraping, no browser, no credentials required.

## Features

- Automatic supplier discovery from your meter number
- Total consumption (m3) - works with HA's Water dashboard and Long-Term Statistics
- Hourly consumption (liters)
- Daily consumption (liters)
- Configurable polling interval (default: every hour)
- Long-term statistics import: each hourly meter reading is recorded in HA's long-term statistics at the supplier's reported timestamp

## Requirements

- Home Assistant 2024.1.0 or newer
- A water meter number from minvandforsyning.dk (found on the page header or your water bill)

## Installation

### HACS (recommended)

MinVandforsyning is in the default [HACS](https://hacs.xyz) store.

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
2. Set the **Polling interval** (10 - 1440 minutes, default 60)

Changes apply immediately without restarting Home Assistant.

The options screen also exposes **Import historical readings as long-term statistics** (default: on). See [Long-term statistics](#long-term-statistics).

## Sensors

| Sensor             | Unit | State class       | Description                          |
|--------------------|------|-------------------|--------------------------------------|
| Total consumption  | m3   | total_increasing  | Cumulative meter reading             |
| Hourly consumption | L    | measurement       | Consumption in the most recent hour  |
| Daily consumption  | L    | measurement       | Consumption so far today             |

The **Total consumption** sensor uses `total_increasing`, so Home Assistant automatically tracks daily, weekly, and monthly statistics. You can add it as a Water source in the Energy dashboard.

## Long-term statistics

When **Import historical readings as long-term statistics** is enabled (the default), the integration writes an external long-term-statistics stream:

```
minvandforsyning:water_meter_<meter_number>_total
```

Each hourly reading is recorded at the supplier's reported timestamp, not at the time Home Assistant polled. The Energy dashboard's hourly buckets line up with what the supplier reports.

On first install, the integration fetches up to 5 years of supplier history in a single API call and imports every reading. This is idempotent: if the stream already has data, the import is skipped. To force a re-import after manually deleting the stream, restart the integration or call `minvandforsyning.backfill_statistics` with `days: 1825`.

### Using the new stream in the Energy dashboard

After the first poll, the statistic appears in **Settings** > **Dashboards** > **Energy** > **Water consumption** > **Edit**. Pick **Water Meter \<meter_number\> Total Consumption** (under the *MinVandforsyning* group) and save.

The `sensor.water_meter_<meter_number>_total_consumption` entity continues to work and Home Assistant still generates its own long-term statistics from it at poll time. Do not add both to the Energy dashboard at the same time; the values will double-count.

### Backfill service

`minvandforsyning.backfill_statistics` has two modes:

- With `days` set (1 to 1825): fetches that many days of history from the supplier and imports it.
- With `days` omitted: re-imports the most recently cached batch (up to 48 h from the last poll).

```yaml
service: minvandforsyning.backfill_statistics
data:
  days: 365                  # optional; 1-1825
  meter_number: "12345678"   # optional; default = all configured meters
  force_full: false          # optional; rewrite every fetched bucket
```

`force_full` (default `false`) re-imports every fetched reading, including buckets already in long-term statistics. Only set it if the recorder's cumulative is visibly broken. The cumulative sum continues from the bucket stored immediately before the imported window, or starts at zero if no prior history exists. Only the fetched window is rewritten; to repair corruption beyond it, set `days` large enough to cover the affected range, or delete the statistic via **Developer tools** > **Statistics** and let the install-time hydrate rebuild it from supplier history.

### Removing a meter

Deleting the integration's config entry removes the sensor entities but leaves the external statistic stream in the recorder. To clean it up:

1. **Developer tools** > **Statistics**
2. Search for `minvandforsyning:water_meter_<meter_number>_total`
3. Click the trash icon and confirm

## How it works

The integration talks to the same public API that the minvandforsyning.dk website uses. It fetches anonymous access tokens (no login required) and retrieves meter data in a binary protobuf format, which is decoded locally with a pure Python parser - zero external dependencies.

## Troubleshooting

### Hourly/daily sensors show 0 or stay flat for hours

The integration polls the Ramboll API every hour by default, but the API only returns whatever the upstream supplier has published. **Some suppliers only push meter readings once every 24 hours** (often late at night). Until they do, the integration has nothing newer to show:

- **Total consumption** still looks current, because it's a cumulative odometer value from the last published row.
- **Hourly consumption** shows the consumption in the *most recently published* hour, which can be a low-flow hour (e.g. 1 L while you were asleep).
- **Daily consumption** shows 0 L until the supplier publishes at least one row dated today.

Every sensor exposes a `last_reading_date` attribute - check it in **Developer tools** > **States**. If it's many hours behind wall-clock time, your supplier simply hasn't published newer data yet, and there's nothing the integration can do about it.

To confirm what the API returned on the last poll, enable debug logging:

```yaml
logger:
  default: warning
  logs:
    custom_components.minvandforsyning: debug
```

After the next poll, **Settings** > **System** > **Logs** will contain a line like `Fetched N readings for meter <id> (latest: ...)` showing the timestamp of the newest reading available.

### Water meter doesn't show up in the Energy dashboard, or the chart is empty

If you added **Total consumption** as a Water source and the chart stays empty even though the entity shows a valid number, the entity's long-term statistics are likely in a bad state (`units_changed` or similar). This can happen if an earlier install registered the entity during a transient API failure or a device-class change.

To recover:

1. Go to **Settings** > **Developer tools** > **Statistics**.
2. Look for issues listed against:
   - `sensor.water_meter_<meter_number>_total_consumption`
   - `sensor.water_meter_<meter_number>_hourly_consumption`
   - `sensor.water_meter_<meter_number>_daily_consumption`
   - `minvandforsyning:water_meter_<meter_number>_total`
3. Click **Fix issue** on each and choose **Delete** (or "Delete all long term statistics").
4. Wait for the next poll (up to 1 hour by default). Fresh statistics will be written with the correct unit, and the Energy dashboard chart will start populating.

You don't need to remove or re-add the Water source in the Energy dashboard - it picks up the new statistics automatically.

### Reporting issues

Debug and warning log lines include your meter number. If you're sharing logs in a GitHub issue, replace all occurrences of your meter number with `<METER>` first.

## License

[MIT](LICENSE)
