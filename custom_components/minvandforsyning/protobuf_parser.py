"""Pure Python parser for protobuf-net-data DataSet binary format.

Decodes the binary wire format produced by protobuf-net-data's DataSerializer
into Python dicts. Zero external dependencies.

Wire format reference:
  - Each result set is field 1 (StartGroup)
  - Columns are field 2 (StartGroup): sub-1=name(String), sub-2=type(Varint)
  - Rows are field 3 (StartGroup): sub-fields 1..N (fieldNumber=columnIndex+1)
  - BclHelpers DateTime: group with field 1=value(ZigZag), 2=scale(Varint), 3=kind(Varint)
  - BclHelpers Decimal: group with field 1=lo(Varint/UInt64), 2=hi(Varint/UInt32), 3=signScale(Varint/UInt32)
"""

from __future__ import annotations

import struct
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

# --- Protobuf wire types ---
WIRE_VARINT = 0
WIRE_FIXED64 = 1
WIRE_LENGTH_DELIMITED = 2
WIRE_START_GROUP = 3
WIRE_END_GROUP = 4
WIRE_FIXED32 = 5

# --- ProtoDataType enum (from protobuf-net-data) ---
PROTO_STRING = 1
PROTO_DATETIME = 2
PROTO_INT = 3
PROTO_LONG = 4
PROTO_SHORT = 5
PROTO_BOOL = 6
PROTO_BYTE = 7
PROTO_FLOAT = 8
PROTO_DOUBLE = 9
PROTO_GUID = 10
PROTO_CHAR = 11
PROTO_DECIMAL = 12
PROTO_BYTE_ARRAY = 13
PROTO_CHAR_ARRAY = 14
PROTO_TIMESPAN = 15
PROTO_DATETIME_OFFSET = 16

# --- TimeSpanScale enum (from protobuf-net BclHelpers) ---
SCALE_DAYS = 0
SCALE_HOURS = 1
SCALE_MINUTES = 2
SCALE_SECONDS = 3
SCALE_MILLISECONDS = 4
SCALE_TICKS = 5
SCALE_MINMAX = 15

_TICKS_PER_SCALE = {
    SCALE_DAYS: 864_000_000_000,        # TimeSpan.TicksPerDay
    SCALE_HOURS: 36_000_000_000,        # TimeSpan.TicksPerHour
    SCALE_MINUTES: 600_000_000,         # TimeSpan.TicksPerMinute
    SCALE_SECONDS: 10_000_000,          # TimeSpan.TicksPerSecond
    SCALE_MILLISECONDS: 10_000,         # TimeSpan.TicksPerMillisecond
    SCALE_TICKS: 1,
}

# .NET epoch used by protobuf-net BclHelpers
_EPOCH = datetime(1970, 1, 1, tzinfo=None)
_TICKS_PER_MICROSECOND = 10  # 1 tick = 100ns = 0.1µs


class ProtobufReader:
    """Low-level protobuf wire-format reader."""

    __slots__ = ("_buf", "_pos", "_end")

    def __init__(self, data: bytes | memoryview, start: int = 0, end: int | None = None) -> None:
        self._buf = data if isinstance(data, memoryview) else memoryview(data)
        self._pos = start
        self._end = end if end is not None else len(data)

    @property
    def pos(self) -> int:
        return self._pos

    @property
    def remaining(self) -> int:
        return self._end - self._pos

    def at_end(self) -> bool:
        return self._pos >= self._end

    def read_varint(self) -> int:
        """Read an unsigned varint (up to 64-bit)."""
        result = 0
        shift = 0
        while True:
            if self._pos >= self._end:
                raise ValueError("Truncated varint")
            b = self._buf[self._pos]
            self._pos += 1
            result |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                return result
            shift += 7
            if shift > 63:
                raise ValueError("Varint too long")

    def read_signed_varint(self) -> int:
        """Read a ZigZag-encoded signed varint."""
        raw = self.read_varint()
        return (raw >> 1) ^ -(raw & 1)

    def read_fixed32(self) -> bytes:
        if self._pos + 4 > self._end:
            raise ValueError("Truncated fixed32")
        val = self._buf[self._pos:self._pos + 4]
        self._pos += 4
        return bytes(val)

    def read_fixed64(self) -> bytes:
        if self._pos + 8 > self._end:
            raise ValueError("Truncated fixed64")
        val = self._buf[self._pos:self._pos + 8]
        self._pos += 8
        return bytes(val)

    def read_bytes(self, length: int) -> bytes:
        if self._pos + length > self._end:
            raise ValueError("Truncated bytes")
        val = self._buf[self._pos:self._pos + length]
        self._pos += length
        return bytes(val)

    def read_tag(self) -> tuple[int, int]:
        """Read a field tag, returning (field_number, wire_type)."""
        raw = self.read_varint()
        return raw >> 3, raw & 0x07

    def read_string(self) -> str:
        length = self.read_varint()
        return self.read_bytes(length).decode("utf-8")

    def read_float(self) -> float:
        return struct.unpack("<f", self.read_fixed32())[0]

    def read_double(self) -> float:
        return struct.unpack("<d", self.read_fixed64())[0]

    def read_int32(self) -> int:
        v = self.read_varint()
        if v > 0x7FFFFFFF:
            v -= 0x100000000
        return v

    def read_uint32(self) -> int:
        return self.read_varint() & 0xFFFFFFFF

    def read_int64(self) -> int:
        v = self.read_varint()
        if v > 0x7FFFFFFFFFFFFFFF:
            v -= 0x10000000000000000
        return v

    def read_uint64(self) -> int:
        return self.read_varint() & 0xFFFFFFFFFFFFFFFF

    def skip_field(self, wire_type: int, _depth: int = 0) -> None:
        """Skip a field value based on its wire type."""
        if wire_type == WIRE_VARINT:
            self.read_varint()
        elif wire_type == WIRE_FIXED64:
            self._pos += 8
        elif wire_type == WIRE_LENGTH_DELIMITED:
            length = self.read_varint()
            self._pos += length
        elif wire_type == WIRE_FIXED32:
            self._pos += 4
        elif wire_type == WIRE_START_GROUP:
            self._skip_group(_depth + 1)
        elif wire_type == WIRE_END_GROUP:
            pass
        else:
            raise ValueError(f"Unknown wire type: {wire_type}")

    _MAX_GROUP_DEPTH = 100

    def _skip_group(self, _depth: int = 0) -> None:
        """Skip until matching EndGroup."""
        if _depth > self._MAX_GROUP_DEPTH:
            raise ValueError(f"Group nesting exceeds maximum depth ({self._MAX_GROUP_DEPTH})")
        while not self.at_end():
            field_num, wire_type = self.read_tag()
            if wire_type == WIRE_END_GROUP:
                return
            self.skip_field(wire_type, _depth)
        raise ValueError("Unterminated group")


# --- BclHelpers decoders ---

def _read_bcl_datetime(reader: ProtobufReader) -> datetime:
    """Read a protobuf-net BclHelpers DateTime (ScaledTicks group).

    Wire format: group with field 1=value(SignedVarint), 2=scale(Varint), 3=kind(Varint).
    DateTime = epoch[kind] + value * ticks_per_scale
    Epoch is 1970-01-01 for all kinds (Unspecified=0, Utc=1, Local=2).
    """
    value: int = 0
    scale: int = SCALE_DAYS  # default
    # kind is not used for date calculation in our case

    while not reader.at_end():
        field_num, wire_type = reader.read_tag()
        if wire_type == WIRE_END_GROUP:
            break
        if field_num == 1:  # value (ZigZag/SignedVarint)
            value = reader.read_signed_varint()
        elif field_num == 2:  # scale
            scale = reader.read_varint()
        elif field_num == 3:  # kind
            reader.read_varint()  # read but ignore
        else:
            reader.skip_field(wire_type)

    if scale == SCALE_MINMAX:
        if value == 1:
            return datetime.max
        if value == -1:
            return datetime.min
        raise ValueError(f"Unknown min/max value: {value}")

    ticks_per = _TICKS_PER_SCALE.get(scale)
    if ticks_per is None:
        raise ValueError(f"Unknown timescale: {scale}")

    total_ticks = value * ticks_per
    # Convert .NET ticks to Python timedelta
    # 1 tick = 100ns = 0.1µs
    total_microseconds = total_ticks // _TICKS_PER_MICROSECOND
    remainder_ticks = total_ticks % _TICKS_PER_MICROSECOND
    # Python datetime has microsecond resolution; remainder is lost (sub-µs)
    return _EPOCH + timedelta(microseconds=total_microseconds)


def _read_bcl_decimal(reader: ProtobufReader) -> Decimal:
    """Read a protobuf-net BclHelpers Decimal group.

    Wire format: group with field 1=lo(UInt64), 2=hi(UInt32), 3=signScale(UInt32).
    lo contains [lo32 | mid32<<32].
    signScale: bit 0 = isNeg, bits 1-8 = scale (number of decimal places).
    Result: decimal(lo32, mid32, hi32, isNeg, scale)
    """
    lo: int = 0
    hi: int = 0
    sign_scale: int = 0

    while not reader.at_end():
        field_num, wire_type = reader.read_tag()
        if wire_type == WIRE_END_GROUP:
            break
        if field_num == 1:  # lo (uint64)
            lo = reader.read_uint64()
        elif field_num == 2:  # hi (uint32)
            hi = reader.read_uint32()
        elif field_num == 3:  # signScale (uint32)
            sign_scale = reader.read_uint32()
        else:
            reader.skip_field(wire_type)

    is_neg = (sign_scale & 0x0001) == 1
    scale = (sign_scale >> 1) & 0xFF

    # Reconstruct the .NET decimal from its 96-bit integer + scale
    lo32 = lo & 0xFFFFFFFF
    mid32 = (lo >> 32) & 0xFFFFFFFF
    hi32 = hi & 0xFFFFFFFF

    # 96-bit integer = hi32 << 64 | mid32 << 32 | lo32
    int_value = (hi32 << 64) | (mid32 << 32) | lo32
    result = Decimal(int_value) / Decimal(10 ** scale)
    if is_neg:
        result = -result
    return result


# --- DataColumn / DataRow types ---

class DataColumn:
    __slots__ = ("name", "proto_type")

    def __init__(self, name: str, proto_type: int) -> None:
        self.name = name
        self.proto_type = proto_type

    def type_name(self) -> str:
        return {
            PROTO_STRING: "String", PROTO_DATETIME: "DateTime", PROTO_INT: "Int32",
            PROTO_LONG: "Int64", PROTO_SHORT: "Int16", PROTO_BOOL: "Boolean",
            PROTO_BYTE: "Byte", PROTO_FLOAT: "Single", PROTO_DOUBLE: "Double",
            PROTO_GUID: "Guid", PROTO_CHAR: "Char", PROTO_DECIMAL: "Decimal",
            PROTO_BYTE_ARRAY: "Byte[]", PROTO_CHAR_ARRAY: "Char[]",
            PROTO_TIMESPAN: "TimeSpan", PROTO_DATETIME_OFFSET: "DateTimeOffset",
        }.get(self.proto_type, f"Unknown({self.proto_type})")


class DataTable:
    __slots__ = ("columns", "rows")

    def __init__(self) -> None:
        self.columns: list[DataColumn] = []
        self.rows: list[dict[str, Any]] = []


# --- DataSet reader ---

def _read_column_value(reader: ProtobufReader, wire_type: int, proto_type: int) -> Any:
    """Read a single column value based on its ProtoDataType."""
    if proto_type == PROTO_STRING:
        return reader.read_string()
    elif proto_type == PROTO_INT:
        return reader.read_int32()
    elif proto_type == PROTO_LONG:
        return reader.read_int64()
    elif proto_type == PROTO_SHORT:
        v = reader.read_varint()
        if v > 0x7FFF:
            v -= 0x10000
        return v
    elif proto_type == PROTO_BOOL:
        return reader.read_varint() != 0
    elif proto_type == PROTO_BYTE:
        return reader.read_varint() & 0xFF
    elif proto_type == PROTO_FLOAT:
        return reader.read_float()
    elif proto_type == PROTO_DOUBLE:
        return reader.read_double()
    elif proto_type == PROTO_CHAR:
        v = reader.read_varint()
        if v > 0x7FFF:
            v -= 0x10000
        return chr(v)
    elif proto_type == PROTO_DATETIME:
        # StartGroup — read until EndGroup
        return _read_bcl_datetime(reader)
    elif proto_type == PROTO_DECIMAL:
        # StartGroup — read until EndGroup
        return _read_bcl_decimal(reader)
    elif proto_type == PROTO_BYTE_ARRAY:
        length = reader.read_varint()
        return reader.read_bytes(length)
    elif proto_type == PROTO_CHAR_ARRAY:
        return reader.read_string()
    elif proto_type == PROTO_TIMESPAN:
        # Group-delimited, same ScaledTicks format as DateTime but returns timedelta
        value = 0
        scale = SCALE_DAYS
        while not reader.at_end():
            fn, wt = reader.read_tag()
            if wt == WIRE_END_GROUP:
                break
            if fn == 1:
                value = reader.read_signed_varint()
            elif fn == 2:
                scale = reader.read_varint()
            else:
                reader.skip_field(wt)
        ticks_per = _TICKS_PER_SCALE.get(scale, 1)
        total_us = (value * ticks_per) // _TICKS_PER_MICROSECOND
        return timedelta(microseconds=total_us)
    elif proto_type == PROTO_DATETIME_OFFSET:
        return reader.read_string()  # stored as ISO string
    else:
        reader.skip_field(wire_type)
        return None


def _read_columns(reader: ProtobufReader) -> list[DataColumn]:
    """Read column definitions from the current position."""
    columns: list[DataColumn] = []
    # We're inside a result set group. Field 2 = column definitions.
    # The caller has already read the first field 2 tag.
    while True:
        # Read this column group
        name = ""
        proto_type = 0
        while not reader.at_end():
            field_num, wire_type = reader.read_tag()
            if wire_type == WIRE_END_GROUP:
                break
            if field_num == 1:  # column name
                name = reader.read_string()
            elif field_num == 2:  # column type
                proto_type = reader.read_varint()
            else:
                reader.skip_field(wire_type)
        columns.append(DataColumn(name, proto_type))

        # Peek at next field — if it's field 2 (another column), continue
        if reader.at_end():
            break
        tag_pos = reader.pos
        field_num, wire_type = reader.read_tag()
        if field_num == 2 and wire_type == WIRE_START_GROUP:
            continue  # next column
        else:
            # Not a column — put back by rewinding
            reader._pos = tag_pos
            break
    return columns


def _read_row(reader: ProtobufReader, columns: list[DataColumn]) -> dict[str, Any]:
    """Read a single data row group."""
    row: dict[str, Any] = {}
    while not reader.at_end():
        field_num, wire_type = reader.read_tag()
        if wire_type == WIRE_END_GROUP:
            break
        col_index = field_num - 1
        if 0 <= col_index < len(columns):
            col = columns[col_index]
            row[col.name] = _read_column_value(reader, wire_type, col.proto_type)
        else:
            reader.skip_field(wire_type)
    return row


def _read_result_set(reader: ProtobufReader) -> DataTable:
    """Read a single result set (table) group."""
    table = DataTable()

    if reader.at_end():
        return table

    # First field inside the result set should be field 2 (columns)
    field_num, wire_type = reader.read_tag()
    if field_num == 0 and wire_type == WIRE_END_GROUP:
        # Empty result set
        return table

    if field_num == 2 and wire_type == WIRE_START_GROUP:
        table.columns = _read_columns(reader)
    else:
        # No columns — might be end group or something else
        if wire_type == WIRE_END_GROUP:
            return table
        reader.skip_field(wire_type)
        return table

    # Now read rows (field 3) until end of group
    while not reader.at_end():
        tag_pos = reader.pos
        field_num, wire_type = reader.read_tag()
        if wire_type == WIRE_END_GROUP:
            break
        if field_num == 3 and wire_type == WIRE_START_GROUP:
            row = _read_row(reader, table.columns)
            table.rows.append(row)
        elif field_num == 0:
            # End of group
            break
        else:
            reader.skip_field(wire_type)

    return table


def parse_dataset(data: bytes) -> list[DataTable]:
    """Parse a protobuf-net-data serialized DataSet.

    Returns a list of DataTable objects, one per result set in the stream.
    """
    reader = ProtobufReader(data)
    tables: list[DataTable] = []

    while not reader.at_end():
        field_num, wire_type = reader.read_tag()
        if field_num == 0:
            break
        if field_num == 1 and wire_type == WIRE_START_GROUP:
            table = _read_result_set(reader)
            tables.append(table)
        else:
            reader.skip_field(wire_type)

    return tables
