"""Tests for :mod:`deriva.bag.profile`.

Covers the profile-identifier surface, path conventions, the
provenance writer, the archive helper, and structural validity of the
bundled profile JSON document.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from deriva.bag.profile import (
    ASSET_FILE_TEMPLATE,
    BAG_SCHEMA_VERSION,
    BAGIT_PROFILE_IDENTIFIER,
    METADATA_DIR,
    PROFILE_JSON_PATH,
    PROVENANCE_FILENAME,
    SCHEMA_JSON_PATH,
    TABLE_CSV_TEMPLATE,
    archive_bag,
    write_provenance,
)


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------


def test_profile_identifier_is_versioned_url() -> None:
    """The profile identifier is a fully-qualified raw GitHub URL."""
    assert BAGIT_PROFILE_IDENTIFIER.startswith("https://")
    assert "deriva-py" in BAGIT_PROFILE_IDENTIFIER
    assert BAGIT_PROFILE_IDENTIFIER.endswith("deriva-bag-profile.json")


def test_profile_json_path_exists() -> None:
    """The bundled profile JSON document is shipped with the package."""
    assert PROFILE_JSON_PATH.exists()
    assert PROFILE_JSON_PATH.suffix == ".json"


def test_profile_json_is_valid_bagit_profile() -> None:
    """The profile JSON parses and carries the well-known BagIt fields."""
    data = json.loads(PROFILE_JSON_PATH.read_text())
    # BagIt-Profile-Info is the spec-required wrapper.
    assert "BagIt-Profile-Info" in data
    info = data["BagIt-Profile-Info"]
    assert info["BagIt-Profile-Identifier"] == BAGIT_PROFILE_IDENTIFIER
    assert "BagIt-Profile-Version" in info
    assert "Version" in info
    # Manifests are required by BDBag profile; we want at least md5.
    assert "md5" in data["Manifests-Required"]
    # Allow-Fetch.txt is critical: our holey-bag flow depends on it.
    assert data["Allow-Fetch.txt"] is True
    # Bag-Info must include the profile identifier as a required field
    # so consumers see our profile declaration before reading data.
    assert "BagIt-Profile-Identifier" in data["Bag-Info"]
    assert data["Bag-Info"]["BagIt-Profile-Identifier"]["required"] is True


def test_path_templates_format_as_expected() -> None:
    """Path templates render to the documented strings."""
    assert SCHEMA_JSON_PATH == "data/schema.json"
    assert (
        TABLE_CSV_TEMPLATE.format(schema="deriva-ml", table="Dataset")
        == "data/deriva-ml/Dataset.csv"
    )
    assert (
        ASSET_FILE_TEMPLATE.format(
            table="Image", rid="ABC123", filename="cat.png"
        )
        == "data/asset/Image/ABC123/cat.png"
    )
    assert METADATA_DIR == "metadata"


def test_bag_schema_version_is_positive_integer() -> None:
    """Schema version is an integer ≥ 1 (0 would mean ``never set``)."""
    assert isinstance(BAG_SCHEMA_VERSION, int)
    assert BAG_SCHEMA_VERSION >= 1


# ---------------------------------------------------------------------------
# write_provenance
# ---------------------------------------------------------------------------


def test_write_provenance_creates_metadata_dir(tmp_path: Path) -> None:
    """Writer creates the ``metadata/`` directory if absent."""
    bag = tmp_path / "bag"
    bag.mkdir()
    assert not (bag / "metadata").exists()
    out = write_provenance(bag, producer="example.Producer")
    assert out == bag / "metadata" / PROVENANCE_FILENAME
    assert out.exists()
    assert (bag / "metadata").is_dir()


def test_write_provenance_payload_shape(tmp_path: Path) -> None:
    """The provenance JSON carries the well-known fields."""
    bag = tmp_path / "bag"
    bag.mkdir()
    out = write_provenance(
        bag,
        producer="test.Producer",
        anchors=[{"kind": "RIDAnchor", "table": "Dataset", "rids": ["1"]}],
        policy={"asset_mode": "rows_only"},
        extra={"build_host": "ci"},
    )
    payload = json.loads(out.read_text())

    assert payload["profile_identifier"] == BAGIT_PROFILE_IDENTIFIER
    assert payload["producer"] == "test.Producer"
    assert payload["anchors"] == [
        {"kind": "RIDAnchor", "table": "Dataset", "rids": ["1"]}
    ]
    assert payload["policy"] == {"asset_mode": "rows_only"}
    assert payload["extra"] == {"build_host": "ci"}
    # built_at should be present and ISO-formatted (contains 'T' or
    # 'Z'/'+00:00' depending on Python version).
    assert "built_at" in payload
    assert "T" in payload["built_at"]


def test_write_provenance_defaults(tmp_path: Path) -> None:
    """Optional fields default to empty containers, never null."""
    bag = tmp_path / "bag"
    bag.mkdir()
    out = write_provenance(bag, producer="x")
    payload = json.loads(out.read_text())
    assert payload["anchors"] == []
    assert payload["extra"] == {}
    # ``policy`` defaults to None so the JSON sentinel for "no policy"
    # is null, not an empty object (which would be ambiguous with
    # "policy with no fields set").
    assert payload["policy"] is None


def test_write_provenance_is_deterministic(tmp_path: Path) -> None:
    """Sorted keys + same inputs produce byte-identical output."""
    bag1 = tmp_path / "bag1"
    bag2 = tmp_path / "bag2"
    bag1.mkdir()
    bag2.mkdir()

    p1 = write_provenance(
        bag1,
        producer="p",
        anchors=[{"a": 1}, {"b": 2}],
        policy={"asset_mode": "rows_only", "max_depth": 3},
    )
    p2 = write_provenance(
        bag2,
        producer="p",
        anchors=[{"a": 1}, {"b": 2}],
        policy={"max_depth": 3, "asset_mode": "rows_only"},  # different dict
    )
    d1 = json.loads(p1.read_text())
    d2 = json.loads(p2.read_text())
    # Built-at timestamps differ, so compare everything else.
    d1.pop("built_at")
    d2.pop("built_at")
    assert d1 == d2


# ---------------------------------------------------------------------------
# archive_bag
# ---------------------------------------------------------------------------


def test_archive_bag_default_output_path(tmp_path: Path) -> None:
    """Default archive path is ``{bag_dir}.zip`` next to the source."""
    bag = tmp_path / "demo-bag"
    bag.mkdir()
    (bag / "data").mkdir()
    (bag / "data" / "schema.json").write_text("{}")

    archive = archive_bag(bag)
    assert archive == tmp_path / "demo-bag.zip"
    assert archive.exists()


def test_archive_bag_custom_output_path(tmp_path: Path) -> None:
    """Explicit ``archive_path`` overrides the default."""
    bag = tmp_path / "src"
    bag.mkdir()
    (bag / "f.txt").write_text("hi")

    out = tmp_path / "out" / "custom.zip"
    archive = archive_bag(bag, out)
    assert archive == out
    assert archive.exists()


def test_archive_bag_preserves_directory_structure(tmp_path: Path) -> None:
    """Archived files unpack to recreate the bag directory."""
    bag = tmp_path / "demo"
    (bag / "data" / "domain").mkdir(parents=True)
    (bag / "data" / "schema.json").write_text('{"snaptime": null}')
    (bag / "data" / "domain" / "Subject.csv").write_text("RID,Name\nS1,A\n")
    (bag / "metadata").mkdir()
    (bag / "metadata" / "provenance.json").write_text("{}")

    archive = archive_bag(bag)

    with zipfile.ZipFile(archive) as zf:
        names = sorted(zf.namelist())

    # Files should be archived with paths relative to the bag's parent,
    # i.e. all entries start with the bag directory name.
    assert all(n.startswith("demo/") for n in names)
    assert "demo/data/schema.json" in names
    assert "demo/data/domain/Subject.csv" in names
    assert "demo/metadata/provenance.json" in names


def test_archive_bag_round_trips(tmp_path: Path) -> None:
    """A zipped bag can be extracted to the original content."""
    bag = tmp_path / "round"
    bag.mkdir()
    (bag / "data").mkdir()
    (bag / "data" / "schema.json").write_text('{"snaptime": null}\n')

    archive = archive_bag(bag)
    extract_dir = tmp_path / "extract"
    extract_dir.mkdir()

    with zipfile.ZipFile(archive) as zf:
        zf.extractall(extract_dir)

    restored = extract_dir / "round" / "data" / "schema.json"
    assert restored.exists()
    assert restored.read_text() == '{"snaptime": null}\n'


def test_archive_bag_rejects_missing_directory(tmp_path: Path) -> None:
    """Pointing at a non-existent path raises ``FileNotFoundError``."""
    with pytest.raises(FileNotFoundError):
        archive_bag(tmp_path / "does-not-exist")


def test_archive_bag_rejects_regular_file(tmp_path: Path) -> None:
    """Passing a file instead of a directory raises ``FileNotFoundError``.

    The function only operates on directories. Files are caught by the
    same is_dir check that catches missing paths.
    """
    f = tmp_path / "not-a-bag.txt"
    f.write_text("hi")
    with pytest.raises(FileNotFoundError):
        archive_bag(f)
