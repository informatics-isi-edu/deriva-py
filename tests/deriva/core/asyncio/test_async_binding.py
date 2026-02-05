# Tests for the async_binding module.
#
# These tests verify the AsyncDerivaBinding class functionality.
# Unit tests use mocks; integration tests require a test server.
#
# Environment variables:
#  DERIVA_PY_TEST_HOSTNAME: hostname of the test server
#  DERIVA_PY_TEST_CREDENTIAL: user credential
#  DERIVA_PY_TEST_VERBOSE: set for verbose logging output

import asyncio
import logging
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from deriva.core.asyncio.async_binding import (
    AsyncDerivaBinding,
    AsyncHTTPError,
    _raise_for_status_async,
)

logger = logging.getLogger(__name__)
if os.getenv("DERIVA_PY_TEST_VERBOSE"):
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

hostname = os.getenv("DERIVA_PY_TEST_HOSTNAME")


class TestRaiseForStatusAsync(unittest.TestCase):
    """Unit tests for _raise_for_status_async helper."""

    def test_success_response_returns_response(self):
        """200 OK response should be returned as-is."""
        response = MagicMock(spec=httpx.Response)
        response.status_code = 200
        result = _raise_for_status_async(response)
        self.assertEqual(result, response)

    def test_client_error_raises(self):
        """4xx errors should raise AsyncHTTPError."""
        response = MagicMock(spec=httpx.Response)
        response.status_code = 404
        response.url = "https://example.org/test"
        response.text = "Not Found"

        with self.assertRaises(AsyncHTTPError) as ctx:
            _raise_for_status_async(response)

        self.assertEqual(ctx.exception.status_code, 404)
        self.assertIn("Client Error", str(ctx.exception))

    def test_server_error_raises(self):
        """5xx errors should raise AsyncHTTPError."""
        response = MagicMock(spec=httpx.Response)
        response.status_code = 500
        response.url = "https://example.org/test"
        response.text = "Internal Server Error"

        with self.assertRaises(AsyncHTTPError) as ctx:
            _raise_for_status_async(response)

        self.assertEqual(ctx.exception.status_code, 500)
        self.assertIn("Server Error", str(ctx.exception))


class TestAsyncDerivaBindingUnit(unittest.TestCase):
    """Unit tests for AsyncDerivaBinding using mocks."""

    def test_init_sets_attributes(self):
        """Constructor should set basic attributes."""
        binding = AsyncDerivaBinding("https", "example.org", {"bearer-token": "test"})

        self.assertEqual(binding.scheme, "https")
        self.assertEqual(binding.server, "example.org")
        self.assertEqual(binding.credentials, {"bearer-token": "test"})
        self.assertEqual(binding._base_url, "https://example.org")

    def test_init_defaults(self):
        """Constructor should handle missing credentials."""
        binding = AsyncDerivaBinding("https", "example.org")

        self.assertEqual(binding.credentials, {})
        self.assertTrue(binding._caching)

    def test_build_headers_includes_context(self):
        """_build_headers should include client context."""
        binding = AsyncDerivaBinding("https", "example.org")
        headers = binding._build_headers()

        self.assertIn("deriva-client-context", headers)

    def test_build_headers_merges_custom(self):
        """_build_headers should merge custom headers."""
        binding = AsyncDerivaBinding("https", "example.org")
        custom = {"X-Custom": "value"}
        headers = binding._build_headers(custom)

        self.assertIn("X-Custom", headers)
        self.assertEqual(headers["X-Custom"], "value")


class TestAsyncDerivaBindingAsync(unittest.IsolatedAsyncioTestCase):
    """Async unit tests for AsyncDerivaBinding."""

    async def test_get_client_creates_client(self):
        """_get_client should create httpx.AsyncClient."""
        binding = AsyncDerivaBinding("https", "example.org")

        self.assertIsNone(binding._client)
        client = await binding._get_client()

        self.assertIsNotNone(binding._client)
        self.assertIsInstance(client, httpx.AsyncClient)

        await binding.close()

    async def test_get_client_reuses_client(self):
        """_get_client should reuse existing client."""
        binding = AsyncDerivaBinding("https", "example.org")

        client1 = await binding._get_client()
        client2 = await binding._get_client()

        self.assertIs(client1, client2)

        await binding.close()

    async def test_close_cleans_up(self):
        """close() should clean up client."""
        binding = AsyncDerivaBinding("https", "example.org")
        await binding._get_client()

        self.assertIsNotNone(binding._client)
        await binding.close()
        self.assertIsNone(binding._client)

    async def test_context_manager(self):
        """Should work as async context manager."""
        async with AsyncDerivaBinding("https", "example.org") as binding:
            self.assertIsNotNone(binding)
        # After exit, client should be closed
        self.assertIsNone(binding._client)

    async def test_run_sync_executes_function(self):
        """run_sync should execute sync function in thread pool."""
        binding = AsyncDerivaBinding("https", "example.org")

        def sync_fn(x, y):
            return x + y

        result = await binding.run_sync(sync_fn, 1, 2)
        self.assertEqual(result, 3)

        await binding.close()

    async def test_run_sync_with_kwargs(self):
        """run_sync should handle keyword arguments."""
        binding = AsyncDerivaBinding("https", "example.org")

        def sync_fn(x, multiplier=1):
            return x * multiplier

        result = await binding.run_sync(sync_fn, 5, multiplier=3)
        self.assertEqual(result, 15)

        await binding.close()

    @patch.object(httpx.AsyncClient, "get")
    async def test_get_async_makes_request(self, mock_get):
        """get_async should make GET request."""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_get.return_value = mock_response

        binding = AsyncDerivaBinding("https", "example.org")
        await binding._get_client()

        response = await binding.get_async("/test")

        self.assertEqual(response, mock_response)
        mock_get.assert_called_once()

        await binding.close()

    @patch.object(httpx.AsyncClient, "post")
    async def test_post_async_with_json(self, mock_post):
        """post_async should post JSON data."""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        binding = AsyncDerivaBinding("https", "example.org")
        await binding._get_client()

        data = {"key": "value"}
        response = await binding.post_async("/test", json_data=data)

        self.assertEqual(response, mock_response)
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args.kwargs
        self.assertEqual(call_kwargs["json"], data)

        await binding.close()


@unittest.skipUnless(hostname, "Test host not specified")
class TestAsyncDerivaBindingIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for AsyncDerivaBinding against real server."""

    async def asyncSetUp(self):
        from deriva.core import get_credential
        credential = os.getenv("DERIVA_PY_TEST_CREDENTIAL") or get_credential(hostname)
        self.binding = AsyncDerivaBinding("https", hostname, credential)

    async def asyncTearDown(self):
        await self.binding.close()

    async def test_get_async_authn_session(self):
        """Should be able to get auth session info."""
        response = await self.binding.get_async("/authn/session")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("client", data)


if __name__ == "__main__":
    unittest.main()
