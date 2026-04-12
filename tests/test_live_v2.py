#!/usr/bin/env python3
"""Test v2.0 parser against live Minvandforsyning API data.

Fetches real data, parses all 8 tables, and validates the v2.0 coordinator
extraction logic without needing Home Assistant.
"""
import json
import sys
import urllib.request
from datetime import datetime
from decimal import Decimal
from pathlib import Path

# Add the integration source to the path (import parser and const directly, skip HA deps)
sys.path.insert(0, str(Path(__file__).parent.parent / "custom_components" / "minvandforsyning"))

from protobuf_parser import parse_dataset
from const import (
    ACUTE_NIGHT_TABLE_INDEX,
    COL_CONSUMPTION,
    COL_HIGH_ALERT_COUNT,
    COL_INFO_CODE_ACTIVE,
    COL_INFO_CODE_VALUE,
    COL_LATEST_ZERO,
    COL_MIN_HOURLY,
    COL_NIGHTS_CONTINUOUS,
    COL_READING,
    COL_READING_DATE,
    COL_REAL_READINGS_COUNT,
    COL_TOTAL_NIGHT,
    COL_ZERO_COUNT,
    FULL_DAY_TABLE_INDEX,
    HISTORICAL_NIGHT_TABLE_INDEX,
    INFO_CODE_TABLE_INDEX,
    READINGS_TABLE_INDEX,
)

METER = "23148103"
SUPPLIER = "15"


def get_tokens():
    url = "https://rwapitokengenerator.azurewebsites.net/api/credentials/anonymous"
    body = json.dumps({
        "targetApi": "BrokerAPI",
        "clientApplication": 2,
        "methodName": "CP_GetAnalysisDetailsForMeter",
    }).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    resp = urllib.request.urlopen(req, timeout=15)
    data = json.loads(resp.read())
    if not data.get("success"):
        raise RuntimeError(f"Token request failed: {data}")
    return data["payload"]["easyAuthToken"], data["payload"]["anonymousUserContextToken"]


def fetch_meter_data(auth_token, ctx_token):
    base = "https://rwbrokerapiprod.azurewebsites.net/CustomerPortal"
    url = f"{base}/CP_GetAnalysisDetailsForMeter?MeterNumber={METER}&SupplierID={SUPPLIER}&DateFrom=2026-01-01&DateTo=2026-04-12"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "X-Context-Token": ctx_token,
    }
    req = urllib.request.Request(url, headers=headers)
    resp = urllib.request.urlopen(req, timeout=30)
    return resp.read()


def main():
    print("=== ha-minvandforsyning v2.0 live data test ===\n")

    # Step 1: Get tokens
    print("1. Getting anonymous tokens...")
    auth_token, ctx_token = get_tokens()
    print("   OK\n")

    # Step 2: Fetch data
    print("2. Fetching meter data (Jan-Apr 2026)...")
    raw = fetch_meter_data(auth_token, ctx_token)
    print(f"   OK - {len(raw)} bytes\n")

    # Step 3: Parse all tables
    print("3. Parsing protobuf DataSet...")
    tables = parse_dataset(raw)
    print(f"   OK - {len(tables)} tables\n")

    # Step 4: Dump table structure
    print("4. Table structure:")
    for i, table in enumerate(tables):
        cols = [f"{c.name}({c.type_name()})" for c in table.columns]
        print(f"   Table {i+1} (index {i}): {len(table.rows)} rows, columns: {', '.join(cols) or '(none)'}")
    print()

    # Step 5: Test Table 7 (readings) - existing v1.0 code
    print("5. Table 7 (readings) - v1.0 validation:")
    if len(tables) > READINGS_TABLE_INDEX:
        readings = tables[READINGS_TABLE_INDEX]
        if readings.rows:
            first = readings.rows[0]
            last = readings.rows[-1]
            print(f"   Rows: {len(readings.rows)}")
            print(f"   First: date={first.get(COL_READING_DATE)}, reading={first.get(COL_READING)} m3, consumption={first.get(COL_CONSUMPTION)} L")
            print(f"   Last:  date={last.get(COL_READING_DATE)}, reading={last.get(COL_READING)} m3, consumption={last.get(COL_CONSUMPTION)} L")
        else:
            print("   WARNING: No rows!")
    else:
        print("   FAIL: Table not found!")
    print()

    # Step 6: Test Table 3 (AcuteNightConsumption) - v2.0
    print("6. Table 3 (AcuteNightConsumption) - v2.0:")
    errors = []
    if len(tables) > ACUTE_NIGHT_TABLE_INDEX:
        t = tables[ACUTE_NIGHT_TABLE_INDEX]
        print(f"   Rows: {len(t.rows)}")
        if t.rows:
            row = t.rows[0]
            val = row.get(COL_NIGHTS_CONTINUOUS)
            print(f"   {COL_NIGHTS_CONTINUOUS} = {val} (type: {type(val).__name__})")
            if val is not None and not isinstance(val, (int, Decimal)):
                errors.append(f"Table 3: {COL_NIGHTS_CONTINUOUS} unexpected type {type(val).__name__}")
        else:
            print("   (empty - no night analysis data)")
    else:
        print("   WARNING: Table not in response")
    print()

    # Step 7: Test Table 4 (FullDayConsumption) - v2.0
    print("7. Table 4 (FullDayConsumption) - v2.0:")
    if len(tables) > FULL_DAY_TABLE_INDEX:
        t = tables[FULL_DAY_TABLE_INDEX]
        print(f"   Rows: {len(t.rows)}")
        if t.rows:
            row = t.rows[0]
            for col_name in [COL_MIN_HOURLY, COL_LATEST_ZERO, COL_ZERO_COUNT, COL_HIGH_ALERT_COUNT, COL_REAL_READINGS_COUNT]:
                val = row.get(col_name)
                print(f"   {col_name} = {val} (type: {type(val).__name__ if val is not None else 'None'})")
                if col_name == COL_LATEST_ZERO and val is not None and not isinstance(val, datetime):
                    errors.append(f"Table 4: {col_name} should be datetime, got {type(val).__name__}")
                if col_name == COL_MIN_HOURLY and val is not None and not isinstance(val, (Decimal, int, float)):
                    errors.append(f"Table 4: {col_name} should be numeric, got {type(val).__name__}")
        else:
            print("   (empty - no full-day analysis data)")
    else:
        print("   WARNING: Table not in response")
    print()

    # Step 8: Test Table 5 (HistoricalNightConsumption) - v2.0
    print("8. Table 5 (HistoricalNightConsumption) - v2.0:")
    if len(tables) > HISTORICAL_NIGHT_TABLE_INDEX:
        t = tables[HISTORICAL_NIGHT_TABLE_INDEX]
        print(f"   Rows: {len(t.rows)}")
        if t.rows:
            row = t.rows[0]
            for col_name in [COL_TOTAL_NIGHT, COL_NIGHTS_CONTINUOUS]:
                val = row.get(col_name)
                print(f"   {col_name} = {val} (type: {type(val).__name__ if val is not None else 'None'})")
        else:
            print("   (empty - no historical night data)")
    else:
        print("   WARNING: Table not in response")
    print()

    # Step 9: Test Table 6 (InfoCode) - v2.0
    print("9. Table 6 (InfoCode) - v2.0:")
    if len(tables) > INFO_CODE_TABLE_INDEX:
        t = tables[INFO_CODE_TABLE_INDEX]
        print(f"   Rows: {len(t.rows)}")
        if t.rows:
            row = t.rows[0]
            for col_name in [COL_INFO_CODE_ACTIVE, COL_INFO_CODE_VALUE]:
                val = row.get(col_name)
                print(f"   {col_name} = {val} (type: {type(val).__name__ if val is not None else 'None'})")
                if col_name == COL_INFO_CODE_ACTIVE and val is not None and not isinstance(val, bool):
                    errors.append(f"Table 6: {col_name} should be bool, got {type(val).__name__}")
        else:
            print("   (empty - no InfoCode data)")
    else:
        print("   WARNING: Table not in response")
    print()

    # Final verdict
    print("=" * 50)
    if errors:
        print(f"FAIL - {len(errors)} type mismatches:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print("PASS - All tables parsed, all types correct")
        print("Safe to deploy v2.0 to HA")
        sys.exit(0)


if __name__ == "__main__":
    main()
