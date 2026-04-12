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

# Analysis table indices in the protobuf DataSet
ACUTE_NIGHT_TABLE_INDEX = 2     # Table 3: AcuteNightConsumption
FULL_DAY_TABLE_INDEX = 3        # Table 4: FullDayConsumption
HISTORICAL_NIGHT_TABLE_INDEX = 4 # Table 5: HistoricalNightConsumption
INFO_CODE_TABLE_INDEX = 5       # Table 6: InfoCode readings

# Column names in Table 3 (AcuteNightConsumption)
COL_ZERO_COUNT = "NumberOfZeroConsumptionsInInterval"
COL_HIGH_ALERT_COUNT = "NumberOfHighAlertLevelConsumptions"
COL_REAL_READINGS_COUNT = "NumberOfRealReadingsInInterval"

# Column names in Table 4 (FullDayConsumption)
COL_MIN_HOURLY = "MinimumHourlyConsumption"
COL_LATEST_ZERO = "LatestZeroConsumption"  # String type, not DateTime

# Column names in Table 3/5 (Night consumption)
COL_NIGHTS_CONTINUOUS = "NumberOfNightsWithMoreThan4HoursOfConsumption"
COL_TOTAL_NIGHT = "Summed_TotalNightConsumption"

# Column names in Table 6 (InfoCode)
COL_INFO_CODE_ACTIVE = "InfoCode_Active"
COL_INFO_CODE_VALUE = "InfoCode_Value"
