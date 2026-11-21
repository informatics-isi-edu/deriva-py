"""Tests for the chunk size HatracStore.create_upload_job requests from hatrac (no live server).

create_upload_job uses calculate_optimal_transfer_shape to choose the job's chunk-length. The requested chunk size is
capped at HARD_MAX_CHUNK_SIZE and raised when the file would otherwise need more than max_chunk_limit chunks (S3 allows
at most 10,000 multipart upload parts, and hatrac's S3 backend sends each chunk as one part). Previously the result was
only used to cap the size, so for example a 300 GB file at the default 25 MiB was sent as 11,445 chunks.

The job POST is stubbed and os.path.getsize is patched, so no file of that size is needed.
"""

import logging
import unittest
from unittest.mock import MagicMock, patch

from deriva.core import HatracStore
from deriva.core.utils.core_utils import DEFAULT_SESSION_CONFIG, DEFAULT_CHUNK_SIZE, DEFAULT_MAX_CHUNK_LIMIT, \
    HARD_MAX_CHUNK_SIZE, Megabyte

GB = 1000 ** 3


def _job_request(file_size, chunk_size, session_config=DEFAULT_SESSION_CONFIG):
    """Call create_upload_job for a file of file_size bytes; return the JSON body POSTed to hatrac."""
    store = HatracStore.__new__(HatracStore)
    store._session = None  # DerivaBinding.__del__ closes the session; there is none here
    store.session_config = session_config
    store.post = MagicMock(return_value=MagicMock(text="/hatrac/ns/f.bin;upload/JOB\n"))
    with patch("deriva.core.hatrac_store.os.path.getsize", return_value=file_size):
        job_id = store.create_upload_job("/hatrac/ns/f.bin", "f.bin", None, None, chunk_size=chunk_size,
                                         content_type="application/octet-stream")
    assert job_id == "JOB"
    return store.post.call_args.kwargs["json"]


def _chunks(file_size, chunk_size):
    return -(-file_size // chunk_size)


class TestCreateUploadJobChunkSize(unittest.TestCase):

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_requested_size_kept_when_within_limit(self):
        for file_size, chunk_size in ((100 * GB, 10 * Megabyte), (200 * GB, DEFAULT_CHUNK_SIZE),
                                      (30 * Megabyte, DEFAULT_CHUNK_SIZE)):
            with self.subTest(file_size=file_size, chunk_size=chunk_size):
                job = _job_request(file_size, chunk_size)
                self.assertEqual(job["chunk-length"], chunk_size)
                self.assertEqual(job["content-length"], file_size)

    def test_raised_to_stay_within_chunk_limit(self):
        for file_size, chunk_size in ((300 * GB, DEFAULT_CHUNK_SIZE), (1000 * GB, DEFAULT_CHUNK_SIZE),
                                      (200 * GB, 10 * Megabyte)):
            with self.subTest(file_size=file_size, chunk_size=chunk_size):
                self.assertGreater(_chunks(file_size, chunk_size), DEFAULT_MAX_CHUNK_LIMIT)  # precondition
                job = _job_request(file_size, chunk_size)
                self.assertGreater(job["chunk-length"], chunk_size)
                self.assertLessEqual(_chunks(file_size, job["chunk-length"]), DEFAULT_MAX_CHUNK_LIMIT)
                self.assertEqual(job["chunk-length"] % (64 * 1024), 0)  # aligned, as calculated

    def test_capped_at_hard_maximum(self):
        job = _job_request(10 * GB, HARD_MAX_CHUNK_SIZE * 2)
        self.assertEqual(job["chunk-length"], HARD_MAX_CHUNK_SIZE)

    def test_session_chunk_limit_honoured(self):
        # e.g. a non-S3 deployment configured to allow more chunks keeps the requested size
        config = dict(DEFAULT_SESSION_CONFIG, max_chunk_limit=20000)
        job = _job_request(300 * GB, DEFAULT_CHUNK_SIZE, session_config=config)
        self.assertEqual(job["chunk-length"], DEFAULT_CHUNK_SIZE)

    def test_empty_file_keeps_requested_size(self):
        job = _job_request(0, 10 * Megabyte)
        self.assertEqual(job["chunk-length"], 10 * Megabyte)
        self.assertEqual(job["content-length"], 0)

    def test_increase_is_logged(self):
        logging.disable(logging.NOTSET)
        with self.assertLogs(level="INFO") as logs:
            _job_request(300 * GB, DEFAULT_CHUNK_SIZE)
        self.assertTrue(any("will be increased" in line for line in logs.output))


if __name__ == "__main__":
    unittest.main()
