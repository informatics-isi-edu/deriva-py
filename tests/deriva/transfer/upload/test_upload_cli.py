"""Tests for deriva-upload-cli session config overrides (no live server).

DerivaUploadCLI.upload builds per-invocation session config overrides from --connect-timeout, --read-timeout and
--no-stream-chunks and passes them to the uploader. These tests capture what the uploader receives.
"""

import unittest
from unittest.mock import patch

from deriva.core import DEFAULT_REQUESTS_TIMEOUT
from deriva.transfer.upload.deriva_upload import DerivaUpload, GenericUploader
from deriva.transfer.upload.deriva_upload_cli import DerivaUploadCLI


class _Captured(Exception):
    pass


class _CapturingUploader(DerivaUpload):
    captured = None

    def __init__(self, config_file=None, credential_file=None, server=None, dcctx_cid=None,
                 session_config_overrides=None, chunk_size=None):
        # Record the constructor arguments and stop before any network activity.
        _CapturingUploader.captured = {"session_config_overrides": session_config_overrides, "chunk_size": chunk_size}
        raise _Captured()

    def __del__(self):
        pass


class TestUploadCliOverrides(unittest.TestCase):

    def _overrides(self, **kwargs):
        with self.assertRaises(_Captured):
            DerivaUploadCLI.upload(_CapturingUploader, "/tmp/data", "example.org", **kwargs)
        return _CapturingUploader.captured["session_config_overrides"]

    def test_no_options_means_no_overrides(self):
        self.assertIsNone(self._overrides())

    def test_no_stream_chunks_override(self):
        self.assertEqual(self._overrides(stream_chunks=False), {"stream_chunked_uploads": False})

    def test_timeouts_and_no_stream_chunks_combine(self):
        self.assertEqual(self._overrides(connect_timeout=120, stream_chunks=False), {
            "timeout": [120, DEFAULT_REQUESTS_TIMEOUT[1]],
            "stream_chunked_uploads": False,
        })

    def test_timeouts_only(self):
        self.assertEqual(self._overrides(read_timeout=90), {"timeout": [DEFAULT_REQUESTS_TIMEOUT[0], 90]})

    def test_chunk_size_must_be_positive(self):
        cli = DerivaUploadCLI(GenericUploader, "desc", "epilog")
        self.assertEqual(cli.parser.parse_args(["--chunk-size", "1048576", "example.org", "/tmp/data"]).chunk_size,
                         1048576)
        for value in ("0", "-5", "abc"):
            with self.subTest(value=value):
                with self.assertRaises(SystemExit):
                    with patch("sys.stderr"):
                        cli.parser.parse_args(["--chunk-size", value, "example.org", "/tmp/data"])

    def test_no_stream_chunks_flag_parses(self):
        cli = DerivaUploadCLI(GenericUploader, "desc", "epilog")
        for argv, expected in ((["example.org", "/tmp/data"], False),
                               (["--no-stream-chunks", "example.org", "/tmp/data"], True)):
            with self.subTest(argv=argv):
                self.assertIs(cli.parser.parse_args(argv).no_stream_chunks, expected)


if __name__ == "__main__":
    unittest.main()
