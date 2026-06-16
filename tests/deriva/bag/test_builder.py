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


def test_builder_add_asset_link_mode_hardlinks_source(tmp_path: Path) -> None:
    """``link=True`` hardlinks the source instead of copying.

    Used by transient-staging callers (e.g. ``commit_execution``) that
    want the bag layout but don't pay for an extra disk copy. The
    bag's directory entry points at the same inode as the source —
    zero bytes duplicated. bagit sees a regular file (which it is, by
    every filesystem-level definition); the safety model is satisfied
    because the link lives inside the bag tree.
    """
    src = tmp_path / "src.bin"
    src.write_bytes(b"original bytes")
    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    bb.add_asset("Image", "I1", src, link=True)
    bb.finalize(make_bdbag=False)

    dest = out / "data" / "asset" / "Image" / "I1" / "src.bin"
    assert dest.is_file()
    # Hardlink — not a symlink, but shares an inode with source.
    assert not dest.is_symlink(), f"hardlink mode must not symlink: {dest}"
    assert dest.stat().st_ino == src.stat().st_ino, (
        f"hardlink should share an inode with source"
    )
    assert dest.read_bytes() == b"original bytes"


def test_builder_add_asset_link_mode_resolves_symlink_source(
    tmp_path: Path,
) -> None:
    """link=True must embed the REAL file as a regular file in the bag even when
    the source is a SYMLINK.

    asset_file_path() in deriva-ml stages assets as absolute symlinks. os.link()
    follows the symlink on macOS/BSD (hardlinks the real file) but hardlinks the
    SYMLINK INODE on Linux — putting a symlink-to-an-external-path in the bag
    payload, which corrupts the bag and trips bagit's _path_is_dangerous(). The
    bag payload must contain the real file's bytes as a regular file on every
    platform.
    """
    real = tmp_path / "real.bin"
    real.write_bytes(b"real bytes")
    link = tmp_path / "link.bin"
    link.symlink_to(real.resolve())  # absolute symlink, as asset_file_path makes
    assert link.is_symlink()

    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    bb.add_asset("Image", "I1", link, link=True)
    bb.finalize(make_bdbag=False)

    dest = out / "data" / "asset" / "Image" / "I1" / "link.bin"
    assert dest.is_file()
    # The load-bearing assertion: the bag payload must not be a symlink.
    # Fails on Linux pre-fix (hardlink to the symlink inode), passes after.
    assert not dest.is_symlink(), f"bag payload must not contain a symlink: {dest}"
    assert dest.read_bytes() == b"real bytes"


def test_builder_add_asset_link_mode_survives_source_deletion(
    tmp_path: Path,
) -> None:
    """Hardlinks keep the file alive after the source is unlinked.

    Important for the ``commit_execution`` workflow: post-upload
    cleanup may remove the flat asset storage, but the bag has
    already been consumed by the loader. The hardlinked bag entry
    behaves like a normal file even after the original directory
    entry is gone — relevant if any post-finalize step still needs
    to read from the bag.
    """
    src = tmp_path / "src.bin"
    src.write_bytes(b"keep me alive")
    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    bb.add_asset("Image", "I1", src, link=True)
    bb.finalize(make_bdbag=False)

    dest = out / "data" / "asset" / "Image" / "I1" / "src.bin"
    src.unlink()  # remove the original directory entry
    # Hardlink keeps the data alive — same inode, different name.
    assert dest.is_file()
    assert dest.read_bytes() == b"keep me alive"


def test_builder_add_asset_link_mode_passes_bagit_validation(
    tmp_path: Path,
) -> None:
    """Hardlinked bag passes ``bdb.make_bag`` validation.

    bagit's ``_validate_bag_contents`` rejects manifest entries that
    resolve outside the bag root for security reasons. Symlinks
    pointing at external storage fail this check. Hardlinks live
    inside the bag tree as ordinary directory entries; bagit sees
    nothing unusual. This test pins that behavior — if a future
    refactor switches to symlinks, ``make_bdbag=True`` will start
    raising ``bagit.BagError("... is unsafe")`` and this test will
    surface it.
    """
    src = tmp_path / "src.txt"
    src.write_bytes(b"hello bagit\n")
    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    bb.add_asset("Image", "I1", src, link=True)
    # make_bdbag=True invokes bagit's full validation.
    bb.finalize(make_bdbag=True)

    # If we got here, validation passed. Spot-check the manifest:
    manifest = (out / "manifest-md5.txt").read_text()
    assert "asset/Image/I1/src.txt" in manifest


def test_builder_add_asset_link_mode_md5_matches_content(
    tmp_path: Path,
) -> None:
    """The bag's MD5 manifest digest equals MD5 of the source content.

    Hardlinks share storage, so ``hashlib`` reading the bag entry
    digests the exact same bytes as the source. This pins the
    contract: the manifest is authoritative for the file content
    regardless of whether the storage came from a copy or a link.
    """
    import hashlib

    src = tmp_path / "src.txt"
    content = b"hello world\n"
    src.write_bytes(content)
    expected_md5 = hashlib.md5(content).hexdigest()

    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    bb.add_asset("Image", "I1", src, link=True)
    bb.finalize(make_bdbag=True)

    manifest = (out / "manifest-md5.txt").read_text()
    asset_lines = [
        line for line in manifest.splitlines()
        if "asset/Image/I1/src.txt" in line
    ]
    assert len(asset_lines) == 1, manifest
    digest = asset_lines[0].split()[0]
    assert digest == expected_md5


def test_builder_add_asset_link_mode_default_is_copy(tmp_path: Path) -> None:
    """Without ``link=True``, ``add_asset`` still copies the file.

    Backward compatibility: existing callers (clone-via-bag,
    constructive bag-building tests) get the copy semantics they've
    always had. Hardlinks are opt-in for transient-staging callers.
    """
    src = tmp_path / "src.bin"
    src.write_bytes(b"content")
    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    bb.add_asset("Image", "I1", src)  # link= defaults to False
    bb.finalize(make_bdbag=False)

    dest = out / "data" / "asset" / "Image" / "I1" / "src.bin"
    assert dest.is_file()
    # Copy mode: different inode (independent storage).
    assert dest.stat().st_ino != src.stat().st_ino


def test_builder_add_assets_bulk_link_mode(tmp_path: Path) -> None:
    """``add_assets(..., link=True)`` propagates the flag to each call.

    Bulk loop just delegates to per-asset ``add_asset(link=...)``;
    this test pins the propagation so a future refactor can't drop
    the kwarg.
    """
    sources: dict[str, Path] = {}
    for rid in ("I1", "I2"):
        p = tmp_path / f"{rid}.bin"
        p.write_bytes(rid.encode())
        sources[rid] = p

    out = tmp_path / "bag"
    bb = BagBuilder(metadata=_two_table_metadata(), output_dir=out)
    bb.add_assets("Image", sources, link=True)
    bb.finalize(make_bdbag=False)

    for rid in ("I1", "I2"):
        src = sources[rid]
        dest = (
            out / "data" / "asset" / "Image" / rid / f"{rid}.bin"
        )
        # Hardlinked — shares inode with source.
        assert dest.stat().st_ino == src.stat().st_ino


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


def test_builder_finalize_make_bdbag_preserves_data_layout(
    tmp_path: Path,
) -> None:
    """``make_bdbag=True`` adds scaffolding without doubly-nesting ``data/``.

    bdbag's ``make_bag(update=False)`` path moves *everything* in
    the directory into a fresh ``data/``. BagBuilder writes
    ``data/schema.json``, ``data/{schema}/...csv``, and
    ``data/asset/{table}/{rid}/{filename}`` directly, so naively
    letting bdbag run its create path produces ``data/data/...``.
    The fix: pre-seed minimal ``bagit.txt``, ``bag-info.txt``,
    and ``manifest-*.txt`` so bdbag detects the directory as an
    existing bag and runs the update path (which doesn't move
    files around).

    This test pins the layout — if anything regresses, the bag's
    schema.json moves to the wrong place and the downstream
    loader can't find it.
    """
    src = tmp_path / "src.bin"
    src.write_bytes(b"asset bytes\n")
    out = tmp_path / "bag"
    with BagBuilder(metadata=_two_table_metadata(), output_dir=out) as bb:
        bb.add_row("Subject", {"RID": "S1", "Name": "Alice"})
        bb.add_asset("Image", "I1", src)
        bb.finalize(make_bdbag=True)

    # Schema.json lands at data/schema.json — NOT data/data/schema.json.
    assert (out / "data" / "schema.json").is_file()
    assert not (out / "data" / "data" / "schema.json").exists(), (
        "bdbag's create path moved everything into data/data/"
    )
    # Asset bytes land at data/asset/Image/I1/src.bin.
    assert (
        out / "data" / "asset" / "Image" / "I1" / "src.bin"
    ).is_file()
    # CSV lands at data/demo/Subject.csv.
    assert (out / "data" / "demo" / "Subject.csv").is_file()
    # bagit scaffolding is at the bag root.
    assert (out / "bagit.txt").is_file()
    assert (out / "bag-info.txt").is_file()
    assert (out / "manifest-md5.txt").is_file()


# =============================================================================
# hatrac_url_for — canonical asset URL builder
# =============================================================================


def test_hatrac_url_for_canonical_shape():
    """The helper produces ``/hatrac/{table}/{md5}.{filename}``.

    This is the convention the upload pipeline + bag-commit path
    both follow when writing an asset row's ``URL`` column.
    """
    from deriva.bag.builder import hatrac_url_for

    url = hatrac_url_for("Image", "abc123", "scan.png")
    assert url == "/hatrac/Image/abc123.scan.png"


def test_hatrac_url_for_preserves_filename_dots():
    """Filenames with multiple dots (e.g. ``uv.lock``) round-trip cleanly.

    The convention puts the MD5 first and the filename second,
    joined by ``.``. A filename like ``uv.lock`` has its own
    internal dot — the helper must preserve it so the upload's
    content-disposition reconstructs the original name.
    """
    from deriva.bag.builder import hatrac_url_for

    url = hatrac_url_for("Execution_Metadata", "deadbeef", "uv.lock")
    assert url == "/hatrac/Execution_Metadata/deadbeef.uv.lock"


def test_hatrac_url_for_table_with_underscores():
    """Asset-table names with underscores aren't quoted/escaped.

    The path is built positionally; no URL-encoding is applied to
    the table name. Tables named ``Execution_Asset``, ``Image_File``
    etc. should pass through unchanged.
    """
    from deriva.bag.builder import hatrac_url_for

    url = hatrac_url_for("Execution_Asset", "abc", "f.txt")
    assert url == "/hatrac/Execution_Asset/abc.f.txt"
