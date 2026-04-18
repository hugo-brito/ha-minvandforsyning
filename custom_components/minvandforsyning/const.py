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
DEFAULT_SCAN_INTERVAL = 7200  # 2 hours in seconds
QUERY_LOOKBACK_HOURS = 48

SUPPLIER_ID_SCAN_MIN = 1
SUPPLIER_ID_SCAN_MAX = 300
SUPPLIER_DISCOVERY_THRESHOLD_BYTES = 910  # empty schema ~903 bytes

# Table index for hourly meter readings in the protobuf DataSet
READINGS_TABLE_INDEX = 6

# Column names in the readings table
COL_READING_DATE = "ReadingDate"
COL_READING = "Reading"
COL_CONSUMPTION = "Consumption"
COL_INFO_CODE = "InfoCode"
