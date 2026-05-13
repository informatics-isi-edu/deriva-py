"""Unit tests for the ``nondefaults`` asset-mapping key on _uploadTable.

The ``nondefaults`` key on a ``asset_type: "table"`` asset-mapping
plumbs an ERMrest ``&nondefaults=...`` URL parameter into the bulk
``POST /entity/T`` insert that :meth:`DerivaUpload._uploadTable`
performs. It mirrors the same-named parameter on
:meth:`_catalogRecordCreate` (the single-row insert path).

The typical use case is ``nondefaults: ["RID"]``: a caller has already
leased RIDs via ``ERMrest_RID_Lease`` and wants the bulk insert to
honor those pre-allocated values rather than letting the server
auto-assign new RIDs. Without ``nondefaults``, the server assigns a
fresh RID to every row regardless of what's in the CSV, which breaks
any FK reference in another table that points at the leased value.
"""
from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def uploader():
    """GenericUploader with mocked catalog, identity, and on-disk file."""
    from deriva.transfer.upload.deriva_upload import GenericUploader
    inst = GenericUploader.__new__(GenericUploader)
    inst.metadata = {}
    inst.catalog = MagicMock()
    # Default-columns lookup gets stubbed per-test as needed.
    inst.catalog.getDefaultColumns = MagicMock(return_value=[])
    inst.catalog.post = MagicMock(return_value=MagicMock(status_code=200))
    inst.identity = {"id": "anon", "display_name": "a", "full_name": "A", "email": "a@b"}
    inst.processor_output = {}
    inst.cancelled = False
    inst.transfer_state_fh = None
    inst.transfer_state = {}
    inst.transfer_state_locks = {}
    inst.getCatalogTable = MagicMock(return_value="S:T")
    inst.getFileDisplayName = MagicMock(return_value="rows.csv")
    inst.getFileSize = MagicMock(return_value=1024)
    inst._execute_processors = MagicMock()
    return inst


def _run_upload_table(uploader, asset_mapping, *, file_ext="csv"):
    """Drive _uploadTable with a dummy file path and CSV body.

    Returns the URL string the uploader called ``catalog.post`` with so
    tests can assert on the defaults/nondefaults parameters.

    Real callers feed ``file_ext`` into ``match_groupdict`` via the
    ``ext_pattern`` regex capture (per the default config, e.g.
    ``"^.*[.](?P<file_ext>json|csv)$"``). We simulate that here so
    ``_uploadTable``'s ``file_ext == 'csv'`` branch resolves correctly.
    """
    match_groupdict = {"file_ext": file_ext}
    fake_open = patch("builtins.open", return_value=io.BytesIO(b"RID,Name\n1-ABC,foo\n"))
    with fake_open:
        uploader._uploadTable("/tmp/rows.csv", asset_mapping, match_groupdict)
    # _uploadTable calls catalog.post(url, fp, headers=...).
    call = uploader.catalog.post.call_args
    return call.args[0]


def test_nondefaults_absent_legacy_behavior(uploader):
    """No ``nondefaults`` key → URL has only ``?defaults=...``.

    Existing asset_mappings (every current caller) shouldn't see any
    change. Verifies the new key is genuinely opt-in.
    """
    asset_mapping = {
        "asset_type": "table",
        "default_columns": ["RCB", "RMB", "RCT", "RMT"],
    }
    url = _run_upload_table(uploader, asset_mapping)
    assert url == "/entity/S:T?defaults=RCB,RMB,RCT,RMT"


def test_nondefaults_present_appended_to_defaults(uploader):
    """Both ``default_columns`` and ``nondefaults`` → defaults+nondefaults in URL."""
    asset_mapping = {
        "asset_type": "table",
        "default_columns": ["RCB", "RMB", "RCT", "RMT"],
        "nondefaults": ["RID"],
    }
    url = _run_upload_table(uploader, asset_mapping)
    assert url == "/entity/S:T?defaults=RCB,RMB,RCT,RMT&nondefaults=RID"


def test_nondefaults_present_no_defaults(uploader):
    """``nondefaults`` alone (no default_columns) → URL starts with ``?nondefaults=``."""
    asset_mapping = {
        "asset_type": "table",
        "default_columns": [],
        "nondefaults": ["RID"],
    }
    url = _run_upload_table(uploader, asset_mapping)
    assert url == "/entity/S:T?nondefaults=RID"


def test_nondefaults_dedups_against_default_columns(uploader):
    """Column in both ``default_columns`` and ``nondefaults`` → removed from defaults.

    The asset_mapping's intent for an overlap is "explicit wins": if a
    column is named in both lists, the caller is opting out of the
    server-side default for that column. The framework reconciles by
    dropping the overlap from defaults.
    """
    asset_mapping = {
        "asset_type": "table",
        "default_columns": ["RID", "RCB", "RMB", "RCT", "RMT"],
        "nondefaults": ["RID"],
    }
    url = _run_upload_table(uploader, asset_mapping)
    # RID dropped from defaults; nondefaults preserves it.
    assert url == "/entity/S:T?defaults=RCB,RMB,RCT,RMT&nondefaults=RID"


def test_nondefaults_multi_column(uploader):
    """Multiple columns in ``nondefaults`` → comma-joined in URL."""
    asset_mapping = {
        "asset_type": "table",
        "default_columns": ["RCB", "RMB"],
        "nondefaults": ["RID", "RCT", "RMT"],
    }
    url = _run_upload_table(uploader, asset_mapping)
    assert url == "/entity/S:T?defaults=RCB,RMB&nondefaults=RID,RCT,RMT"


def test_nondefaults_with_implicit_default_columns(uploader):
    """When ``default_columns`` is omitted, framework fetches from catalog.

    The fetched list will typically include ``RID`` (every column the
    table declares). If the caller adds ``nondefaults=["RID"]``, the
    dedup logic strips ``RID`` from the implicit defaults before
    building the URL — same as the explicit-default_columns case.
    """
    # Stub getDefaultColumns to return all the table's columns.
    uploader.catalog.getDefaultColumns = MagicMock(return_value=["RID", "RCB", "RMB"])
    asset_mapping = {
        "asset_type": "table",
        # No default_columns supplied; framework will fetch via getDefaultColumns.
        "nondefaults": ["RID"],
    }
    url = _run_upload_table(uploader, asset_mapping)
    # RID dropped from the auto-fetched defaults.
    assert url == "/entity/S:T?defaults=RCB,RMB&nondefaults=RID"
