"""Tests for the protobuf-net-data parser."""
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from custom_components.minvandforsyning.protobuf_parser import ProtobufReader, parse_dataset
from custom_components.minvandforsyning.protobuf_parser import WIRE_START_GROUP, WIRE_END_GROUP

FIXTURES = Path(__file__).parent / "fixtures"
METER_DATA = FIXTURES / "meter_data.bin"
GROUND_TRUTH = FIXTURES / "ground_truth.json"


@pytest.fixture(scope="module")
def tables():
    data = METER_DATA.read_bytes()
    return parse_dataset(data)


@pytest.fixture(scope="module")
def ground_truth():
    return json.loads(GROUND_TRUTH.read_text(encoding="utf-8"))


class TestDataSetStructure:
    def test_table_count(self, tables):
        assert len(tables) == 8

    def test_all_tables_have_columns(self, tables):
        for i, t in enumerate(tables):
            assert len(t.columns) > 0, f"Table {i} has no columns"

    @pytest.mark.parametrize(
        "index, expected_cols",
        [
            (0, ["MR_AnalysisID", "CreatedDate"]),
            (1, ["MR_Analysis_ItemID", "MR_AnalysisID", "AnalysisType", "KeyType"]),
            (6, ["TS", "ReadingDate", "Reading", "InfoCode", "TSOfPrior", "Consumption", "OwingNext"]),
            (7, ["AnalysisType", "MeterInAnalysisCount"]),
        ],
    )
    def test_column_names(self, tables, index, expected_cols):
        actual = [c.name for c in tables[index].columns]
        assert actual == expected_cols

    @pytest.mark.parametrize(
        "index, expected_rows",
        [(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 2400), (7, 0)],
    )
    def test_row_counts(self, tables, index, expected_rows):
        assert len(tables[index].rows) == expected_rows


class TestReadingsTable:
    """Tests for Table 6 (hourly meter readings)."""

    def test_first_reading(self, tables):
        row = tables[6].rows[0]
        assert row["TS"] == 227929
        assert row["ReadingDate"] == datetime(2026, 1, 1, 1, 0, 0)
        assert row["Reading"] == Decimal("281.857")
        assert row["Consumption"] == Decimal("37")
        assert row["TSOfPrior"] == 227928

    def test_second_reading(self, tables):
        row = tables[6].rows[1]
        assert row["ReadingDate"] == datetime(2026, 1, 1, 2, 0, 0)
        assert row["Reading"] == Decimal("281.881")
        assert row["Consumption"] == Decimal("24")

    def test_last_reading(self, tables):
        row = tables[6].rows[-1]
        assert row["TS"] == 230328
        assert row["ReadingDate"] == datetime(2026, 4, 11, 0, 0, 0)
        assert row["Reading"] == Decimal("304.272")
        assert row["Consumption"] == Decimal("0")

    def test_all_readings_have_required_columns(self, tables):
        required = {"TS", "ReadingDate", "Reading", "Consumption"}
        for i, row in enumerate(tables[6].rows):
            missing = required - set(row.keys())
            assert not missing, f"Row {i} missing columns: {missing}"

    def test_readings_chronologically_ordered(self, tables):
        dates = [r["ReadingDate"] for r in tables[6].rows]
        assert dates == sorted(dates), "Readings not in chronological order"

    def test_readings_are_monotonically_increasing(self, tables):
        readings = [r["Reading"] for r in tables[6].rows]
        for i in range(1, len(readings)):
            assert readings[i] >= readings[i - 1], (
                f"Reading decreased at row {i}: {readings[i-1]} -> {readings[i]}"
            )

    def test_consumption_is_non_negative(self, tables):
        for i, row in enumerate(tables[6].rows):
            assert row["Consumption"] >= 0, f"Negative consumption at row {i}"


class TestGroundTruthComparison:
    """Cross-verify Python parser output against C# ground truth."""

    def test_table_count_matches(self, tables, ground_truth):
        assert len(tables) == len(ground_truth)

    def test_column_names_match_all_tables(self, tables, ground_truth):
        for gt_table in ground_truth:
            idx = gt_table["index"]
            py_cols = [c.name for c in tables[idx].columns]
            gt_cols = [c["name"] for c in gt_table["columns"]]
            assert py_cols == gt_cols, f"Table {idx} column names differ"

    def test_row_counts_match_all_tables(self, tables, ground_truth):
        for gt_table in ground_truth:
            idx = gt_table["index"]
            assert len(tables[idx].rows) == len(gt_table["rows"]), (
                f"Table {idx} row count: Python={len(tables[idx].rows)}, C#={len(gt_table['rows'])}"
            )

    def test_readings_table_values_match(self, tables, ground_truth):
        gt_table = ground_truth[6]
        for i, (py_row, gt_row) in enumerate(zip(tables[6].rows, gt_table["rows"])):
            # Compare TS
            assert py_row["TS"] == gt_row["TS"], f"Row {i} TS mismatch"
            # Compare ReadingDate (C# outputs ISO with fractional seconds)
            py_dt = py_row["ReadingDate"]
            gt_dt = datetime.fromisoformat(gt_row["ReadingDate"])
            assert py_dt == gt_dt, f"Row {i} ReadingDate: {py_dt} != {gt_dt}"
            # Compare Reading (C# outputs decimal as string like "281.857")
            assert Decimal(gt_row["Reading"]) == py_row["Reading"], \
                f"Row {i} Reading: {py_row['Reading']} != {gt_row['Reading']}"
            # Compare Consumption
            assert Decimal(gt_row["Consumption"]) == py_row["Consumption"], \
                f"Row {i} Consumption: {py_row['Consumption']} != {gt_row['Consumption']}"


class TestGroupDepthLimit:
    """Tests for protobuf parser group nesting depth limit."""

    def test_deeply_nested_groups_rejected(self):
        """Groups nested beyond _MAX_GROUP_DEPTH raise ValueError."""
        # Build a payload with 150 nested StartGroup tags (field 1)
        # Each StartGroup is tag byte 0x0B (field_num=1, wire_type=3)
        depth = 150
        payload = bytes([0x0B] * depth)
        reader = ProtobufReader(payload)
        # Read the first tag manually, then skip the StartGroup
        reader.read_tag()  # consumes first 0x0B
        with pytest.raises(ValueError, match="maximum depth"):
            reader.skip_field(WIRE_START_GROUP)
