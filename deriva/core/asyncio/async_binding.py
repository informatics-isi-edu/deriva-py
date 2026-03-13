"""Async HTTP binding for Deriva servers.

This module provides async HTTP methods using httpx.AsyncClient,
following the same patterns as the synchronous DerivaBinding class.

Retry behavior matches the synchronous DerivaBinding, which uses
urllib3.Retry with configurable connect retries, read retries,
exponential backoff, and a status code forcelist. The async version
implements equivalent application-level retry logic.
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, TypeVar

import httpx

from deriva.core import (
    DEFAULT_HEADERS,
    DEFAULT_SESSION_CONFIG,
    ConcurrentUpdate,
    NotModified,
    urlquote_dcctx,
)
from deriva.core.deriva_binding import DerivaClientContext, _response_raise_for_status

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Default safe methods that can be retried (matches urllib3 Retry.DEFAULT_ALLOWED_METHODS)
_DEFAULT_RETRYABLE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "DELETE", "PUT", "TRACE"})


class AsyncHTTPError(Exception):
    """HTTP error from async requests."""

    def __init__(self, status_code: int, message: str, response: httpx.Response | None = None):
        self.status_code = status_code
        self.response = response
        super().__init__(f"{status_code}: {message}")


def _raise_for_status_async(response: httpx.Response) -> httpx.Response:
    """Raise AsyncHTTPError if response indicates an error."""
    if 400 <= response.status_code < 600:
        try:
            details = response.text[:500]
        except Exception:
            details = ""
        raise AsyncHTTPError(
            response.status_code,
            f"{'Client' if response.status_code < 500 else 'Server'} Error for url: [{response.url}] {details}",
            response,
        )
    return response


class AsyncDerivaBinding:
    """Async HTTP binding for Deriva servers.

    Provides async versions of HTTP methods (get, post, put, delete) using
    httpx.AsyncClient. Also provides run_sync() for executing sync code
    in a thread pool, following SQLAlchemy's pattern.

    Attributes:
        scheme: HTTP scheme ("http" or "https")
        server: Server hostname
        credentials: Authentication credentials dict
        dcctx: Deriva client context for request tracking
    """

    # Thread pool for run_sync operations
    _sync_executor: ThreadPoolExecutor | None = None

    def __init__(
        self,
        scheme: str,
        server: str,
        credentials: dict | None = None,
        caching: bool = True,
        session_config: dict | None = None,
    ):
        """Initialize async binding.

        Args:
            scheme: HTTP scheme ("http" or "https")
            server: Server hostname
            credentials: Authentication credentials dict
            caching: Enable response caching (default: True)
            session_config: Session configuration overrides
        """
        self.scheme = scheme
        self.server = server
        self.credentials = credentials or {}
        self._caching = caching
        self._cache: dict[str, httpx.Response] = {}

        # Merge session config with defaults
        self._session_config = {**DEFAULT_SESSION_CONFIG}
        if session_config:
            self._session_config.update(session_config)

        # Client context for request tracking
        self.dcctx = DerivaClientContext()

        # Base URL
        self._base_url = f"{scheme}://{server}"
        self._auth_uri = f"{self._base_url}/authn/session"

        # Async HTTP client (created lazily)
        self._client: httpx.AsyncClient | None = None

        # Timeout configuration
        timeout = self._session_config.get("timeout", (6, 63))
        if isinstance(timeout, tuple):
            self._timeout = httpx.Timeout(connect=timeout[0], read=timeout[1], write=timeout[1], pool=timeout[0])
        else:
            self._timeout = httpx.Timeout(timeout)

        # Retry configuration (matching sync DerivaBinding via urllib3.Retry)
        self._retry_connect = self._session_config.get("retry_connect", 2)
        self._retry_read = self._session_config.get("retry_read", 4)
        self._retry_backoff_factor = self._session_config.get("retry_backoff_factor", 1.0)
        self._retry_status_forcelist = set(self._session_config.get("retry_status_forcelist", [500, 502, 503, 504]))
        self._retry_on_all_methods = self._session_config.get("allow_retry_on_all_methods", False)

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the async HTTP client."""
        if self._client is None:
            # Build headers
            headers = {}
            if "bearer-token" in self.credentials:
                headers["Authorization"] = f"Bearer {self.credentials['bearer-token']}"

            # Build cookies
            cookies = {}
            if "cookie" in self.credentials:
                cname, cval = self.credentials["cookie"].split("=", 1)
                cookies[cname] = cval

            # Create client with connection pooling
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                headers=headers,
                cookies=cookies,
                timeout=self._timeout,
                follow_redirects=True,
                limits=httpx.Limits(
                    max_connections=100,
                    max_keepalive_connections=20,
                    keepalive_expiry=30.0,
                ),
            )
        return self._client

    def _build_headers(self, headers: dict | None = None) -> dict:
        """Build request headers with client context."""
        result = dict(DEFAULT_HEADERS) if headers is None else dict(headers)
        result["deriva-client-context"] = self.dcctx.encoded()
        return result

    async def _request_with_retry(
        self,
        method: str,
        path: str,
        headers: dict | None = None,
        data: bytes | str | None = None,
        json_data: Any | None = None,
    ) -> httpx.Response:
        """Execute an HTTP request with retry logic.

        Retries on connection errors and configurable status codes, matching
        the sync DerivaBinding's urllib3.Retry behavior:
        - Connection errors: retried up to ``retry_connect`` times
        - Read errors / retryable status codes: retried up to ``retry_read`` times
        - Exponential backoff: ``backoff_factor * 2^(attempt-1)`` seconds
        - Only safe methods (GET, HEAD, PUT, DELETE, OPTIONS, TRACE) are retried
          unless ``allow_retry_on_all_methods`` is set

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            path: Request path
            headers: Request headers
            data: Raw body data
            json_data: JSON-serializable body data

        Returns:
            httpx.Response object

        Raises:
            httpx.ConnectError: After exhausting connect retries
            AsyncHTTPError: After exhausting read/status retries or on non-retryable error
        """
        client = await self._get_client()
        request_headers = headers or {}

        # Determine if this method is retryable
        is_retryable_method = self._retry_on_all_methods or method.upper() in _DEFAULT_RETRYABLE_METHODS
        max_connect_retries = self._retry_connect if is_retryable_method else 0
        max_read_retries = self._retry_read if is_retryable_method else 0

        connect_attempts = 0
        read_attempts = 0
        last_exception = None

        while True:
            try:
                if method.upper() == "GET":
                    response = await client.get(path, headers=request_headers)
                elif method.upper() == "POST":
                    if json_data is not None:
                        response = await client.post(path, json=json_data, headers=request_headers)
                    else:
                        response = await client.post(path, content=data, headers=request_headers)
                elif method.upper() == "PUT":
                    if json_data is not None:
                        response = await client.put(path, json=json_data, headers=request_headers)
                    else:
                        response = await client.put(path, content=data, headers=request_headers)
                elif method.upper() == "DELETE":
                    response = await client.delete(path, headers=request_headers)
                else:
                    response = await client.request(method.upper(), path, headers=request_headers)

                # Check for retryable status codes
                if response.status_code in self._retry_status_forcelist and read_attempts < max_read_retries:
                    read_attempts += 1
                    delay = self._retry_backoff_factor * (2 ** (read_attempts - 1))
                    logger.debug(
                        "Retrying %s %s (status %d, attempt %d/%d, backoff %.1fs)",
                        method, path, response.status_code, read_attempts, max_read_retries, delay,
                    )
                    await asyncio.sleep(delay)
                    continue

                return response

            except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
                last_exception = exc
                if connect_attempts < max_connect_retries:
                    connect_attempts += 1
                    delay = self._retry_backoff_factor * (2 ** (connect_attempts - 1))
                    logger.debug(
                        "Retrying %s %s (connect error, attempt %d/%d, backoff %.1fs): %s",
                        method, path, connect_attempts, max_connect_retries, delay, exc,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise

            except (httpx.ReadError, httpx.ReadTimeout) as exc:
                last_exception = exc
                if read_attempts < max_read_retries:
                    read_attempts += 1
                    delay = self._retry_backoff_factor * (2 ** (read_attempts - 1))
                    logger.debug(
                        "Retrying %s %s (read error, attempt %d/%d, backoff %.1fs): %s",
                        method, path, read_attempts, max_read_retries, delay, exc,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise

    async def get_async(
        self,
        path: str,
        headers: dict | None = None,
        raise_not_modified: bool = False,
    ) -> httpx.Response:
        """Perform async GET request with retry support.

        Args:
            path: Request path
            headers: Optional headers dict
            raise_not_modified: Raise error on 304 response

        Returns:
            httpx.Response object
        """
        request_headers = self._build_headers(headers)

        # Check cache for etag
        cache_key = f"{self._base_url}{path}"
        if self._caching and cache_key in self._cache:
            prev = self._cache[cache_key]
            if "etag" in prev.headers and "if-none-match" not in request_headers:
                request_headers["if-none-match"] = prev.headers["etag"]

        response = await self._request_with_retry("GET", path, headers=request_headers)

        # Handle 304 Not Modified
        if response.status_code == 304:
            if raise_not_modified:
                raise NotModified(response)
            if cache_key in self._cache:
                return self._cache[cache_key]

        _raise_for_status_async(response)

        # Cache successful response
        if self._caching and response.status_code == 200:
            self._cache[cache_key] = response

        return response

    async def post_async(
        self,
        path: str,
        data: bytes | str | None = None,
        json_data: Any | None = None,
        headers: dict | None = None,
    ) -> httpx.Response:
        """Perform async POST request with retry support.

        Note: POST is not retried by default (not idempotent) unless
        ``allow_retry_on_all_methods`` is set in session config.

        Args:
            path: Request path
            data: Raw data to send
            json_data: JSON-serializable data
            headers: Optional headers dict

        Returns:
            httpx.Response object
        """
        request_headers = self._build_headers(headers)

        response = await self._request_with_retry("POST", path, headers=request_headers, data=data, json_data=json_data)

        if response.status_code == 412:
            raise ConcurrentUpdate(response)

        _raise_for_status_async(response)
        return response

    async def put_async(
        self,
        path: str,
        data: bytes | str | None = None,
        json_data: Any | None = None,
        headers: dict | None = None,
        guard_response: httpx.Response | None = None,
    ) -> httpx.Response:
        """Perform async PUT request with retry support.

        Args:
            path: Request path
            data: Raw data to send
            json_data: JSON-serializable data
            headers: Optional headers dict
            guard_response: Previous response for conditional update

        Returns:
            httpx.Response object
        """
        request_headers = self._build_headers(headers)

        # Add If-Match header for conditional updates
        if guard_response is not None and "etag" in guard_response.headers:
            request_headers["if-match"] = guard_response.headers["etag"]

        response = await self._request_with_retry("PUT", path, headers=request_headers, data=data, json_data=json_data)

        if response.status_code == 412:
            raise ConcurrentUpdate(response)

        _raise_for_status_async(response)
        return response

    async def delete_async(
        self,
        path: str,
        headers: dict | None = None,
        guard_response: httpx.Response | None = None,
    ) -> httpx.Response:
        """Perform async DELETE request with retry support.

        Args:
            path: Request path
            headers: Optional headers dict
            guard_response: Previous response for conditional delete

        Returns:
            httpx.Response object
        """
        request_headers = self._build_headers(headers)

        # Add If-Match header for conditional deletes
        if guard_response is not None and "etag" in guard_response.headers:
            request_headers["if-match"] = guard_response.headers["etag"]

        response = await self._request_with_retry("DELETE", path, headers=request_headers)

        if response.status_code == 412:
            raise ConcurrentUpdate(response)

        _raise_for_status_async(response)
        return response

    async def run_sync(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run a synchronous function in a thread pool.

        This follows SQLAlchemy's pattern for running sync code within
        an async context. The function runs in a thread pool executor
        to avoid blocking the event loop.

        Args:
            fn: Synchronous function to execute
            *args: Positional arguments for fn
            **kwargs: Keyword arguments for fn

        Returns:
            Result of fn(*args, **kwargs)

        Example:
            async def main():
                binding = AsyncDerivaBinding("https", "example.org")

                def sync_work():
                    # Complex sync operations here
                    return some_result

                result = await binding.run_sync(sync_work)
        """
        # Use class-level executor for thread reuse
        if AsyncDerivaBinding._sync_executor is None:
            AsyncDerivaBinding._sync_executor = ThreadPoolExecutor(
                max_workers=10,
                thread_name_prefix="deriva-sync",
            )

        loop = asyncio.get_running_loop()
        func = functools.partial(fn, *args, **kwargs)
        return await loop.run_in_executor(AsyncDerivaBinding._sync_executor, func)

    async def close(self) -> None:
        """Close the async HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "AsyncDerivaBinding":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()
