"""Constants for the MinVandforsyning integration."""

DOMAIN = "minvandforsyning"

TOKEN_GENERATOR_URL = "https://rwapitokengenerator.azurewebsites.net"
BROKER_API_URL = "https://rwbrokerapiprod.azurewebsites.net"

TOKEN_PATH = "/api/credentials/anonymous"
METER_DATA_PATH = "/CustomerPortal/CP_GetAnalysisDetailsForMeter"

CLIENT_APPLICATION_APP = 2  # MinvandforsyningApp enum value
TARGET_API = "BrokerAPI"
METHOD_NAME = "CP_GetAnalysisDetailsForMeter"

CONTEXT_TOKEN_HEADER = "X-Context-Token"

CONF_SCAN_INTERVAL = "scan_interval"
MIN_SCAN_INTERVAL = 600    # 10 minutes in seconds
MAX_SCAN_INTERVAL = 86400  # 24 hours in seconds
DEFAULT_SCAN_INTERVAL = 3600  # 1 hour in seconds
QUERY_LOOKBACK_HOURS = 48

SUPPLIER_ID_SCAN_MIN = 1
SUPPLIER_ID_SCAN_MAX = 500
SUPPLIER_DISCOVERY_THRESHOLD_BYTES = 910  # empty schema ~903 bytes

# Retry policy for transient API failures (5xx, 429, network errors, timeouts).
# Bounded so we never monopolize the coordinator's update window.
API_MAX_ATTEMPTS = 3
API_BACKOFF_BASE_SECONDS = 1.0
API_TRANSIENT_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

# Table index for hourly meter readings in the protobuf DataSet
READINGS_TABLE_INDEX = 6

# Rambøll API date semantics (verified via fixture analysis — see tests/test_api_semantics.py):
# ReadingDate is a UTC naive datetime (protobuf Kind=Unspecified, values ARE UTC)
# marking the END of each hourly consumption interval.
# Consumption(T) == Reading(T) - Reading(T-1), i.e. water used during [T-1, T) UTC.
# The bucket start for HA statistics is ReadingDate - 1 hour.
# In winter (CET=UTC+1) the old code was coincidentally correct; in summer (CEST=UTC+2)
# it was 1 hour off — the root cause of issue #9.
READING_DATE_TZ = "Europe/Copenhagen"  # used only for daily_liters() local-day grouping

# Column names in the readings table
COL_READING_DATE = "ReadingDate"
COL_READING = "Reading"
COL_CONSUMPTION = "Consumption"
COL_INFO_CODE = "InfoCode"

# Long-term statistics import
CONF_IMPORT_STATISTICS = "import_statistics"
DEFAULT_IMPORT_STATISTICS = True
LITERS_PER_CUBIC_METER = 1000
SERVICE_BACKFILL_STATISTICS = "backfill_statistics"
# External statistic_id template — uses the ``<domain>:<object_id>`` form
# required by async_add_external_statistics. Keeps the LTS stream isolated
# from the sensor entity's auto-LTS, so there's no current-hour collision.
STATISTIC_ID_FORMAT = DOMAIN + ":water_meter_{meter_number}_total"
# Live API probing (see .agent/probe-api-range-output.md) shows the Rambøll
# endpoint serves up to ~5 years of hourly history in a single ~1.3 s call.
# Beyond ~5 years the response stops growing — that's the effective ceiling.
INITIAL_HYDRATE_DAYS = 1825  # ~5 years; API ceiling observed via probe
# Backfill service caps `days` at the same ceiling so users can repopulate
# the full available supplier history after deleting the LTS stream.
SERVICE_BACKFILL_MAX_DAYS = 1825  # see INITIAL_HYDRATE_DAYS
