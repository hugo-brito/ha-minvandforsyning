"""One-shot script to inspect all tables from the Ramboll API."""
import asyncio
import importlib.util
import sys
import os
import types

# Import only the modules we need without triggering __init__.py (which imports homeassistant)
_base = os.path.join(os.path.dirname(__file__), "..", "custom_components", "minvandforsyning")

def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_base, path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

# Load const first (no HA deps), then parser, then api_client
const = _load("custom_components.minvandforsyning.const", "const.py")
protobuf_parser = _load("custom_components.minvandforsyning.protobuf_parser", "protobuf_parser.py")
api_client = _load("custom_components.minvandforsyning.api_client", "api_client.py")

import aiohttp
from datetime import datetime, timedelta, timezone
from custom_components.minvandforsyning.api_client import MinvandforsyningClient
from custom_components.minvandforsyning.protobuf_parser import parse_dataset


async def main():
    async with aiohttp.ClientSession() as session:
        client = MinvandforsyningClient(session)
        now = datetime.now(timezone.utc)
        raw = await client.async_get_meter_data(
            "23148103", 15,
            now - timedelta(days=90),
            now + timedelta(days=1),
        )
        tables = parse_dataset(raw)

        print(f"Total tables: {len(tables)}")
        print(f"Response size: {len(raw)} bytes")
        print()

        for i, table in enumerate(tables):
            cols = [c.name for c in table.columns]
            print(f"=== Table {i+1} (index {i}) ===")
            print(f"  Columns: {cols}")
            print(f"  Rows: {len(table.rows)}")
            if table.rows:
                print(f"  First row: {dict(table.rows[0])}")
                if len(table.rows) > 1:
                    print(f"  Last row:  {dict(table.rows[-1])}")
            print()


asyncio.run(main())
