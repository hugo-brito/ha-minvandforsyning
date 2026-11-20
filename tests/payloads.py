"""Encode supplier API responses for tests."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal


def varint(value: int) -> bytes:
    """Encode an unsigned protobuf varint."""
    encoded = bytearray()
    while value > 127:
        encoded.append((value & 127) | 128)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _decimal(value: str) -> bytes:
    """Encode a decimal as its unscaled value and its scale."""
    _, digits, exponent = Decimal(value).as_tuple()
    encoded = b"\x08" + varint(int("".join(map(str, digits))))
    return encoded + (b"\x18" + varint(-exponent << 1) if exponent < 0 else b"")


def supplier_response(readings: list[tuple[datetime, str, str]]) -> bytes:
    """Encode readings like the supplier API, which puts them in its 7th table."""
    response = b"\x0b\x0c" * 6 + b"\x0b"
    for name, data_type in (("ReadingDate", 2), ("Reading", 12), ("Consumption", 12)):
        response += (
            b"\x13\x0a" + varint(len(name)) + name.encode()
            + b"\x10" + varint(data_type) + b"\x14"
        )
    for reading_date, reading, consumption in readings:
        seconds = int(reading_date.replace(tzinfo=timezone.utc).timestamp())
        response += (
            b"\x1b\x0b\x08" + varint(seconds * 2) + b"\x10\x03\x0c"
            + b"\x13" + _decimal(reading) + b"\x14"
            + b"\x1b" + _decimal(consumption) + b"\x1c\x1c"
        )
    return response + b"\x0c"
