"""Tests for :mod:`deriva.bag.builder`."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Column, ForeignKey, MetaData, String, Table

from deriva.bag.builder import BagBuilder
from deriva.bag.profile import SCHEMA_JSON_PATH
from deriva.bag.sources import IterableDataSource


def _two_table_metadata() -> MetaData:
    """Two-table schema: Image.Subject → Subject.RID."""
    md = MetaData()
    Table(
        "Subject",
        md,
        Column("RID", String, primary_key=True),
        Column("Name", String),
        schema="demo",
    )
    Table(
        "Image",
        md,
        Column("RID", String, primary_key=True),
        Column("Filename", String),
        Column("Subject", String, ForeignKey("demo.Subject.RID")),
        schema="demo",
    )
    return md


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_builder_requires_schema_input(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        BagBuilder(output_dir=tmp_path)


def test_builder_creates_output_dir(tmp_path: Path) -> None:
    out = tmp_path / "missing" / "nested"
    BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    assert out.is_dir()


def test_builder_lists_tables(tmp_path: Path) -> None:
    bb = BagBuilder(
        metadata=_two_table_metadata(), output_dir=tmp_path
    )
    assert bb.list_tables() == ["demo.Image", "demo.Subject"]


# ---------------------------------------------------------------------------
# add_row / add_rows
# ---------------------------------------------------------------------------


def test_builder_add_row_buffers(tmp_path: Path) -> None:
    bb = BagBuilder(
        metadata=_two_table_metadata(), output_dir=tmp_path
    )
    bb.add_row("Subject", {"RID": "S1", "Name": "A"})
    bb.add_row("demo.Subject", {"RID": "S2", "Name": "B"})
    bag = bb.finalize(make_bdbag=False)
    rows = list(
        csv.DictReader(
            (bag / "data" / "demo" / "Subject.csv").open(newline="")
        )
    )
    assert len(rows) == 2
    assert {r["Name"] for r in rows} == {"A", "B"}


def test_builder_add_rows_iterable(tmp_path: Path) -> None:
    bb = BagBuilder(
        metadata=_two_table_metadata(), output_dir=tmp_path
    )
    added = bb.add_rows(
        "Subject",
        iter(
            [
                {"RID": "S1", "Name": "Alice"},
                {"RID": "S2", "Name": "Bob"},
            ]
        ),
    )
    assert added == 2


def test_builder_add_rows_dataframe(tmp_path: Path) -> None:
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"RID": ["S1", "S2"], "Name": ["A", "B"]})
    bb = BagBuilder(
        metadata=_two_table_metadata(), output_dir=tmp_path
    )
    added = bb.add_rows("Subject", df)
    assert added == 2


def test_builder_rejects_unknown_table(tmp_path: Path) -> None:
    bb = BagBuilder(
        metadata=_two_table_metadata(), output_dir=tmp_path
    )
    with pytest.raises(KeyError):
        bb.add_row("Nope", {"RID": "x"})


def test_builder_resolves_qualified_name(tmp_path: Path) -> None:
    """Both 'Subject' and 'demo.Subject' route to the same table buffer."""
    bb = BagBuilder(
        metadata=_two_table_metadata(), output_dir=tmp_path
    )
    bb.add_row("Subject", {"RID": "S1", "Name": "A"})
    bb.add_row("demo.Subject", {"RID": "S2", "Name": "B"})
    bag = bb.finalize(make_bdbag=False)
    rows = list(
        csv.DictReader(
            (bag / "data" / "demo" / "Subject.csv").open(newline="")
        )
    )
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------


def test_builder_add_asset_copies_file(tmp_path: Path) -> None:
    src = tmp_path / "src.png"
    src.write_bytes(b"\x89PNG\r\n\x1a\n")
    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    bb.add_asset("Image", "I1", src)
    bb.finalize(make_bdbag=False)
    expected = out / "data" / "asset" / "Image" / "I1" / "src.png"
    assert expected.is_file()
    assert expected.read_bytes() == b"\x89PNG\r\n\x1a\n"


def test_builder_add_asset_with_custom_filename(tmp_path: Path) -> None:
    src = tmp_path / "src.bin"
    src.write_bytes(b"data")
    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    bb.add_asset("Image", "I1", src, filename="alias.bin")
    bb.finalize(make_bdbag=False)
    assert (
        out / "data" / "asset" / "Image" / "I1" / "alias.bin"
    ).is_file()


def test_builder_add_asset_missing_source(tmp_path: Path) -> None:
    bb = BagBuilder(
        metadata=_two_table_metadata(),
        output_dir=tmp_path / "bag",
    )
    with pytest.raises(FileNotFoundError):
        bb.add_asset("Image", "I1", tmp_path / "missing.bin")


def test_builder_add_asset_duplicate_destination_different_source(
    tmp_path: Path,
) -> None:
    src1 = tmp_path / "a.bin"
    src1.write_bytes(b"1")
    src2 = tmp_path / "b.bin"
    src2.write_bytes(b"2")
    bb = BagBuilder(
        metadata=_two_table_metadata(),
        output_dir=tmp_path / "bag",
    )
    bb.add_asset("Image", "I1", src1, filename="x.bin")
    with pytest.raises(ValueError, match="already populated"):
        bb.add_asset("Image", "I1", src2, filename="x.bin")


def test_builder_add_asset_idempotent_same_source(tmp_path: Path) -> None:
    src = tmp_path / "a.bin"
    src.write_bytes(b"1")
    bb = BagBuilder(
        metadata=_two_table_metadata(),
        output_dir=tmp_path / "bag",
    )
    bb.add_asset("Image", "I1", src)
    # Same source → same destination → idempotent.
    bb.add_asset("Image", "I1", src)


def test_builder_add_assets_bulk(tmp_path: Path) -> None:
    sources: dict[str, Path] = {}
    for rid in ("I1", "I2", "I3"):
        p = tmp_path / f"{rid}.bin"
        p.write_bytes(rid.encode())
        sources[rid] = p
    bb = BagBuilder(
        metadata=_two_table_metadata(),
        output_dir=tmp_path / "bag",
    )
    count = bb.add_assets("Image", sources)
    assert count == 3
    bb.finalize(make_bdbag=False)
    for rid in ("I1", "I2", "I3"):
        assert (
            tmp_path
            / "bag"
            / "data"
            / "asset"
            / "Image"
            / rid
            / f"{rid}.bin"
        ).is_file()


def test_builder_add_asset_reference_records_entry(tmp_path: Path) -> None:
    bb = BagBuilder(
        metadata=_two_table_metadata(),
        output_dir=tmp_path / "bag",
    )
    bb.add_asset_reference(
        table="Image",
        rid="I1",
        filename="img.png",
        url="https://example.com/hatrac/img.png",
        length=1024,
        md5="abc123",
    )
    assert len(bb._fetch_entries) == 1
    entry = next(iter(bb._fetch_entries.values()))
    assert entry["url"] == "https://example.com/hatrac/img.png"
    assert entry["length"] == 1024
    assert entry["md5"] == "abc123"
    assert (
        entry["filename"] == "data/asset/Image/I1/img.png"
    )


def test_builder_add_asset_reference_dedupes_on_url(tmp_path: Path) -> None:
    bb = BagBuilder(
        metadata=_two_table_metadata(),
        output_dir=tmp_path / "bag",
    )
    bb.add_asset_reference(
        table="Image",
        rid="I1",
        filename="x.png",
        url="https://example.com/x",
        length=10,
    )
    # Same URL + same destination → idempotent.
    bb.add_asset_reference(
        table="Image",
        rid="I1",
        filename="x.png",
        url="https://example.com/x",
        length=10,
    )
    assert len(bb._fetch_entries) == 1


def test_builder_add_asset_reference_rejects_url_conflict(
    tmp_path: Path,
) -> None:
    bb = BagBuilder(
        metadata=_two_table_metadata(),
        output_dir=tmp_path / "bag",
    )
    bb.add_asset_reference(
        table="Image",
        rid="I1",
        filename="x.png",
        url="https://example.com/x",
        length=10,
    )
    with pytest.raises(ValueError, match="already referenced"):
        bb.add_asset_reference(
            table="Image",
            rid="I2",
            filename="y.png",
            url="https://example.com/x",
            length=10,
        )


# ---------------------------------------------------------------------------
# write_from_source
# ---------------------------------------------------------------------------


def test_builder_write_from_source(tmp_path: Path) -> None:
    """A DataSource adapter feeds rows into the same buffers."""
    out = tmp_path / "bag"
    bb = BagBuilder(
        metadata=_two_table_metadata(), output_dir=out
    )
    source = IterableDataSource(
        {
            "Subject": [{"RID": "S1", "Name": "A"}],
            "Image": [{"RID": "I1", "Subject": "S1"}],
        }
    )
    counts = bb.write_from_source(source)
    # Both tables matched and got their rows queued.
    assert counts["demo.Subject"] == 1
    assert counts["demo.Image"] == 1


# ---------------------------------------------------------------------------
# Finalize
# ---------------------------------------------------------------------------


def test_builder_finalize_writes_schema_json(tmp_path: Path) -> None:
    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    bag = bb.finalize(make_bdbag=False)
    schema_path = bag / SCHEMA_JSON_PATH
    assert schema_path.exists()
    doc = json.loads(schema_path.read_text())
    assert "schemas" in doc
    assert "demo" in doc["schemas"]
    assert "Subject" in doc["schemas"]["demo"]["tables"]


def test_builder_finalize_writes_provenance(tmp_path: Path) -> None:
    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    bb.finalize(make_bdbag=False)
    prov = out / "metadata" / "deriva-bag-provenance.json"
    assert prov.exists()
    payload = json.loads(prov.read_text())
    assert payload["producer"] == "deriva.bag.builder.BagBuilder"


def test_builder_finalize_writes_in_fk_order(tmp_path: Path) -> None:
    """CSVs land for both tables when added out of FK order."""
    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    # Add Image before Subject — DataLoader's FK ordering swaps
    # them at finalize time.
    bb.add_row("Image", {"RID": "I1", "Subject": "S1", "Filename": "a"})
    bb.add_row("Subject", {"RID": "S1", "Name": "A"})
    bb.finalize(make_bdbag=False)
    assert (out / "data" / "demo" / "Subject.csv").is_file()
    assert (out / "data" / "demo" / "Image.csv").is_file()


def test_builder_finalize_is_terminal(tmp_path: Path) -> None:
    """Mutations after finalize raise."""
    bb = BagBuilder(
        metadata=_two_table_metadata(),
        output_dir=tmp_path / "bag",
    )
    bb.finalize(make_bdbag=False)
    with pytest.raises(RuntimeError, match="finalized"):
        bb.add_row("Subject", {"RID": "S1"})


def test_builder_finalize_archive(tmp_path: Path) -> None:
    """``archive=True`` writes a .zip next to the bag."""
    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    bb.finalize(make_bdbag=False, archive=True)
    assert (tmp_path / "bag.zip").is_file()
