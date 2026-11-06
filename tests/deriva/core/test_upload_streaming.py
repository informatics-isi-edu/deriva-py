"""Tests for streamed upload chunk bodies and the send block size (no live server).

hatrac_store.put_obj_chunked sends each chunk as a stream (io.BytesIO) rather than a bytes object, so that urllib3
applies the connect timeout to each block written rather than to the whole chunk. These tests check that:

- the chunk body handed to the session is a stream with the right content, unless the "stream_chunked_uploads"
  session config setting is false, in which case it is a bytes object as before;
- TimeoutHTTPAdapter passes the send block size to urllib3 2.x pools (including proxy managers) and omits it under
  urllib3 1.26, which does not accept it;
- urllib3 rewinds a stream body when it retries, both after a connection reset part way through the body and after a
  retryable status, so the retried request carries the full body. These run against a raw-socket HTTP server on
  127.0.0.1 using a deriva session from get_new_requests_session.
"""

import hashlib
import io
import os
import socket
import struct
import tempfile
import threading
import unittest
from unittest.mock import MagicMock

from deriva.core import HatracStore
from deriva.core.utils.core_utils import DEFAULT_SESSION_CONFIG, DEFAULT_SEND_BLOCKSIZE, \
    URLLIB3_SUPPORTS_BLOCKSIZE, TimeoutHTTPAdapter, get_new_requests_session


def _chunked_bodies(session_config):
    """Run put_obj_chunked on a 2.1-chunk file with a stubbed session; return (content, [(url, body, headers)])."""
    chunk_size = 1024
    content = os.urandom(chunk_size * 2 + 100)
    store = HatracStore.__new__(HatracStore)
    store._session = None  # DerivaBinding.__del__ closes the session; there is none here
    store._server_uri = "https://example.org"
    store.session_config = session_config
    store.get_upload_job = MagicMock(return_value=MagicMock(json=MagicMock(return_value={"chunk-length": chunk_size})))
    store._response_raise_for_status = MagicMock()
    bodies = []

    def fake_put(url, data=None, headers=None):
        bodies.append((url, data, headers))
        return MagicMock()
    store.put = fake_put

    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "f.bin")
        with open(path, "wb") as f:
            f.write(content)
        store.put_obj_chunked("/hatrac/ns/f.bin", path, "JOB", chunk_size=chunk_size)
    return content, bodies


class TestPutObjChunkedBody(unittest.TestCase):

    def _check(self, session_config, body_type):
        content, bodies = _chunked_bodies(session_config)
        self.assertEqual([u for u, _, _ in bodies], ["/hatrac/ns/f.bin;upload/JOB/%d" % i for i in range(3)])
        sent = b""
        for _, body, headers in bodies:
            self.assertIsInstance(body, body_type)
            data = body.getvalue() if isinstance(body, io.BytesIO) else body
            self.assertEqual(headers["Content-Length"], str(len(data)))
            sent += data
        self.assertEqual(sent, content)

    def test_sends_stream_bodies(self):
        cases = {
            "default": DEFAULT_SESSION_CONFIG,
            # e.g. a session config saved in a servers.json before the setting existed
            "key-absent": {k: v for k, v in DEFAULT_SESSION_CONFIG.items() if k != "stream_chunked_uploads"},
            "true": dict(DEFAULT_SESSION_CONFIG, stream_chunked_uploads=True),
        }
        for name, session_config in cases.items():
            with self.subTest(name):
                self._check(session_config, io.BytesIO)

    def test_streaming_disabled_sends_bytes(self):
        for value in (False, "false"):
            with self.subTest(value=value):
                self._check(dict(DEFAULT_SESSION_CONFIG, stream_chunked_uploads=value), bytes)


class TestSendBlocksize(unittest.TestCase):

    def test_adapter_blocksize_on_pool_manager(self):
        adapter = TimeoutHTTPAdapter(timeout=(6, 63))
        self.assertEqual(adapter.blocksize, DEFAULT_SEND_BLOCKSIZE)
        if URLLIB3_SUPPORTS_BLOCKSIZE:
            self.assertEqual(adapter.poolmanager.connection_pool_kw.get("blocksize"), DEFAULT_SEND_BLOCKSIZE)
        else:
            self.assertNotIn("blocksize", adapter.poolmanager.connection_pool_kw)

    def test_adapter_blocksize_override_and_disable(self):
        adapter = TimeoutHTTPAdapter(blocksize=64 * 1024)
        disabled = TimeoutHTTPAdapter(blocksize=None)
        if URLLIB3_SUPPORTS_BLOCKSIZE:
            self.assertEqual(adapter.poolmanager.connection_pool_kw.get("blocksize"), 64 * 1024)
        self.assertNotIn("blocksize", disabled.poolmanager.connection_pool_kw)

    def test_adapter_blocksize_on_proxy_manager(self):
        manager = TimeoutHTTPAdapter().proxy_manager_for("http://127.0.0.1:3128")
        if URLLIB3_SUPPORTS_BLOCKSIZE:
            self.assertEqual(manager.connection_pool_kw.get("blocksize"), DEFAULT_SEND_BLOCKSIZE)
        else:
            self.assertNotIn("blocksize", manager.connection_pool_kw)

    def test_session_pools_get_blocksize(self):
        session = get_new_requests_session()
        try:
            for adapter in session.adapters.values():
                self.assertIsInstance(adapter, TimeoutHTTPAdapter)
                self.assertEqual("blocksize" in adapter.poolmanager.connection_pool_kw, URLLIB3_SUPPORTS_BLOCKSIZE)
        finally:
            session.close()


class _LoopbackServer:
    """Minimal HTTP/1.1 server on 127.0.0.1 that records what each connection received.

    Per attempt (connection) it can reset the connection after reading part of the body, or answer 503 after reading
    the whole body. Otherwise it answers 200 with "<length> <sha256>" of the body received.
    """

    RESET_AFTER = 256 * 1024

    def __init__(self, reset_attempts=(), status_503_attempts=()):
        self.reset_attempts = set(reset_attempts)
        self.status_503_attempts = set(status_503_attempts)
        self.attempts = []
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.url = "http://127.0.0.1:%d/obj" % self.sock.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def close(self):
        self.sock.close()

    def _serve(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            self._handle(conn)

    def _handle(self, conn):
        idx = len(self.attempts)
        rec = {"received": 0, "complete": False}
        self.attempts.append(rec)
        try:
            buf = b""
            while b"\r\n\r\n" not in buf:
                d = conn.recv(65536)
                if not d:
                    return
                buf += d
            head, body = buf.split(b"\r\n\r\n", 1)
            length = 0
            for line in head.split(b"\r\n")[1:]:
                k, v = line.split(b":", 1)
                if k.strip().lower() == b"content-length":
                    length = int(v.strip())
            h = hashlib.sha256(body)
            got = len(body)
            while got < length:
                if idx in self.reset_attempts and got >= self.RESET_AFTER:
                    rec["received"] = got
                    # abortive close: the client sees a connection reset while still sending the body
                    conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                    return
                d = conn.recv(65536)
                if not d:
                    break
                h.update(d)
                got += len(d)
            rec.update(received=got, complete=(got == length))
            if idx in self.status_503_attempts:
                conn.sendall(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            else:
                msg = b"%d %s" % (got, h.hexdigest().encode())
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s" % (len(msg), msg))
        except OSError:
            pass
        finally:
            conn.close()


class TestRetryRewind(unittest.TestCase):

    def setUp(self):
        self.payload = os.urandom(8 * 1024 * 1024)

    def _put(self, server, body):
        config = dict(DEFAULT_SESSION_CONFIG, retry_backoff_factor=0.01)
        session = get_new_requests_session(session_config=config)
        try:
            return session.put(server.url, data=body, headers={"Content-Type": "application/octet-stream",
                                                               "Content-Length": str(len(self.payload))})
        finally:
            session.close()

    def _assert_full_body(self, r):
        self.assertEqual(r.status_code, 200)
        length, digest = r.text.split()
        self.assertEqual(int(length), len(self.payload))
        self.assertEqual(digest, hashlib.sha256(self.payload).hexdigest())

    def test_rewound_after_connection_reset(self):
        server = _LoopbackServer(reset_attempts={0, 1})
        try:
            self._assert_full_body(self._put(server, io.BytesIO(self.payload)))
            self.assertEqual(len(server.attempts), 3)
            self.assertTrue(server.attempts[-1]["complete"])
        finally:
            server.close()

    def test_rewound_to_initial_offset(self):
        # A stream that does not start at 0 must be rewound to where it started, not to 0.
        body = io.BytesIO(b"X" * 1000 + self.payload)
        body.seek(1000)
        server = _LoopbackServer(reset_attempts={0})
        try:
            self._assert_full_body(self._put(server, body))
        finally:
            server.close()

    def test_rewound_after_retryable_status(self):
        server = _LoopbackServer(status_503_attempts={0})
        try:
            self._assert_full_body(self._put(server, io.BytesIO(self.payload)))
            self.assertEqual(len(server.attempts), 2)
            self.assertTrue(all(a["complete"] for a in server.attempts))
        finally:
            server.close()


if __name__ == "__main__":
    unittest.main()
