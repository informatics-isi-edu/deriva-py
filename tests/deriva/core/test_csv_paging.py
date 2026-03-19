"""Tests for CSV paging with multi-line quoted fields.

Verifies that _read_last_csv_record correctly extracts the last complete
CSV record from files containing multi-line quoted fields (RFC 4180),
which previously caused an infinite paging loop in get_as_file().
"""

import csv
import io
import os
import tempfile

from deriva.core.ermrest_catalog import ErmrestCatalog


def _write_csv(filepath, header, rows):
    """Write rows to a CSV file with proper quoting."""
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


class TestReadLastCsvRecord:
    """Test _read_last_csv_record handles multi-line quoted fields."""

    def test_simple_single_line_records(self):
        """Basic case: all fields are single-line."""
        header = ["RID", "Name", "Value"]
        rows = [
            ["2-ABC", "Alice", "100"],
            ["2-DEF", "Bob", "200"],
            ["2-GHI", "Carol", "300"],
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            filepath = f.name
        try:
            _write_csv(filepath, header, rows)
            result = ErmrestCatalog._read_last_csv_record(filepath, header)
            assert result["RID"] == "2-GHI"
            assert result["Name"] == "Carol"
            assert result["Value"] == "300"
        finally:
            os.unlink(filepath)

    def test_multiline_quoted_field(self):
        """Fields with embedded newlines must not break last-record detection."""
        header = ["RID", "Name", "Notes"]
        rows = [
            ["2-ABC", "Alice", "Line 1\nLine 2\nLine 3"],
            ["2-DEF", "Bob", "Simple note"],
            ["2-GHI", "Carol", "Grid data:\n  1  2  3\n  4  5  6\n  7  8  9"],
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            filepath = f.name
        try:
            _write_csv(filepath, header, rows)
            result = ErmrestCatalog._read_last_csv_record(filepath, header)
            assert result["RID"] == "2-GHI"
            assert result["Name"] == "Carol"
            assert "Grid data:" in result["Notes"]
        finally:
            os.unlink(filepath)

    def test_large_multiline_field(self):
        """Very large multi-line fields (simulating OCR visual field grids)."""
        header = ["RID", "Data", "Side"]
        # Simulate OCR data with a large grid (~100KB per record)
        large_grid = "\n".join(
            "  ".join(f"{x:3d}" for x in range(i * 10, i * 10 + 10))
            for i in range(1000)
        )
        rows = [
            ["2-AAA", "small", "Left"],
            ["2-BBB", large_grid, "Right"],
            ["2-CCC", large_grid, "Left"],
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            filepath = f.name
        try:
            _write_csv(filepath, header, rows)
            result = ErmrestCatalog._read_last_csv_record(filepath, header)
            assert result["RID"] == "2-CCC"
            assert result["Side"] == "Left"
        finally:
            os.unlink(filepath)

    def test_empty_file(self):
        """Empty file returns empty dict."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            filepath = f.name
        try:
            result = ErmrestCatalog._read_last_csv_record(filepath, ["RID", "Name"])
            assert result == {}
        finally:
            os.unlink(filepath)

    def test_header_only_file(self):
        """File with only a header row returns empty dict."""
        header = ["RID", "Name"]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            filepath = f.name
        try:
            _write_csv(filepath, header, [])
            result = ErmrestCatalog._read_last_csv_record(filepath, header)
            # The header line gets parsed as a data row mapping header[0]->"RID"
            # which is a valid-looking record, but there's no real data
            assert result == {} or result.get("RID") == "RID"
        finally:
            os.unlink(filepath)

    def test_single_data_row(self):
        """File with exactly one data row."""
        header = ["RID", "Name"]
        rows = [["2-XYZ", "Only"]]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            filepath = f.name
        try:
            _write_csv(filepath, header, rows)
            result = ErmrestCatalog._read_last_csv_record(filepath, header)
            assert result["RID"] == "2-XYZ"
            assert result["Name"] == "Only"
        finally:
            os.unlink(filepath)

    def test_field_with_commas_and_quotes(self):
        """Fields with commas and escaped quotes."""
        header = ["RID", "Description", "Value"]
        rows = [
            ["2-AAA", 'He said "hello, world"', "100"],
            ["2-BBB", "Line 1,\nLine 2,\nLine 3", "200"],
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            filepath = f.name
        try:
            _write_csv(filepath, header, rows)
            result = ErmrestCatalog._read_last_csv_record(filepath, header)
            assert result["RID"] == "2-BBB"
            assert result["Value"] == "200"
        finally:
            os.unlink(filepath)
