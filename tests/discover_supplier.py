"""Discover supplier ID for a given meter number."""
import urllib.request
import json
import sys
import datetime

# Get anonymous tokens
req = urllib.request.Request(
    "https://rwapitokengenerator.azurewebsites.net/api/credentials/anonymous",
    data=json.dumps({
        "targetApi": "BrokerAPI",
        "clientApplication": 2,
        "methodName": "CP_GetAnalysisDetailsForMeter",
    }).encode(),
    headers={"Content-Type": "application/json"},
)
resp = json.loads(urllib.request.urlopen(req, timeout=15).read())
ctx = resp["payload"]["anonymousUserContextToken"]
auth = resp["payload"]["easyAuthToken"]
expiry = resp["payload"]["expiry"]
print(f"Tokens acquired (expires {expiry})")

# Scan supplier IDs
today = datetime.date.today().isoformat()
yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
meter = "23148103"

for sid in range(1, 301):
    url = (
        f"https://rwbrokerapiprod.azurewebsites.net/CustomerPortal/"
        f"CP_GetAnalysisDetailsForMeter?MeterNumber={meter}"
        f"&SupplierID={sid}&DateFrom={yesterday}&DateTo={today}"
    )
    req2 = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {auth}", "X-Context-Token": ctx},
    )
    try:
        data = urllib.request.urlopen(req2, timeout=10).read()
        if len(data) > 910:
            print(f"FOUND: SupplierID={sid}, response={len(data)} bytes")
            sys.exit(0)
    except Exception:
        pass
    if sid % 50 == 0:
        print(f"  Scanned 1-{sid}...")

print("Not found in range 1-300")
