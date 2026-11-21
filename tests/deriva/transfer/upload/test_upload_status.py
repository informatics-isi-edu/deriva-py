"""Tests for per-file status accounting in DerivaUpload.uploadFiles (no live server).

A chunked upload only writes transfer state after its first chunk completes. If the first chunk exhausts its retries,
hatrac_store raises HatracJobTimeout while no transfer state exists. These tests check that such a file is reported as
a failure rather than being left marked in-progress, which previously excluded it from both the success and failure
counts so that the run reported success.
"""

import logging
import unittest
from collections import OrderedDict

import requests

from deriva.core import HatracJobTimeout
from deriva.transfer.upload.deriva_upload import DerivaUpload, UploadEntry, UploadState


def _make_uploader(paths, upload_side_effect):
    # Bypass __init__, which requires a server, configuration, and credentials.
    uploader = object.__new__(DerivaUpload)
    uploader.identity = {"id": "test"}
    uploader.cancelled = False
    uploader.skipped_files = set()
    uploader.transfer_state = dict()
    uploader.transfer_state_fh = None
    uploader.file_status = OrderedDict()
    uploader.file_list = OrderedDict({"group": OrderedDict(
        (p, UploadEntry("group", {}, {}, p)) for p in paths)})
    uploader.uploadFile = upload_side_effect
    return uploader


class TestUploadStatusAccounting(unittest.TestCase):

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_first_chunk_timeout_is_reported_as_failure(self):
        def upload(path, *args, **kwargs):
            if path.endswith(".am"):
                raise HatracJobTimeout(requests.ConnectionError("The write operation timed out"))
            return {"ok": True}

        uploader = _make_uploader(["a.am", "a.png"], upload)
        with self.assertRaises(RuntimeError) as cm:
            uploader.uploadFiles()
        self.assertIn("1 file(s) failed", str(cm.exception))
        self.assertEqual(uploader.file_status["a.am"]["State"], UploadState.Timeout)
        self.assertIn("write operation timed out", uploader.file_status["a.am"]["Status"])
        self.assertEqual(uploader.file_status["a.png"]["State"], UploadState.Success)

    def test_later_chunk_timeout_reports_progress_and_keeps_transfer_state(self):
        def upload(path, *args, **kwargs):
            uploader.transfer_state[path] = {"completed": 1, "total": 4}
            raise HatracJobTimeout(requests.ConnectionError("timed out"))

        uploader = _make_uploader(["b.am"], upload)
        with self.assertRaises(RuntimeError):
            uploader.uploadFiles()
        self.assertEqual(uploader.file_status["b.am"]["State"], UploadState.Timeout)
        self.assertIn("25% complete", uploader.file_status["b.am"]["Status"])
        # transfer state is retained so a later run can resume
        self.assertIn("b.am", uploader.transfer_state)

    def test_no_entry_left_in_progress(self):
        def upload(path, *args, **kwargs):
            raise HatracJobTimeout(requests.ConnectionError("timed out"))

        uploader = _make_uploader(["c1.am", "c2.am"], upload)
        with self.assertRaises(RuntimeError):
            uploader.uploadFiles()
        states = {v["State"] for v in uploader.file_status.values()}
        self.assertNotIn(UploadState.Running, states)


if __name__ == "__main__":
    unittest.main()
