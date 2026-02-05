"""Async HTTP binding for Deriva servers.

This module provides async HTTP methods using httpx.AsyncClient,
following the same patterns as the synchronous DerivaBinding class.
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

    async def get_async(
        self,
        path: str,
        headers: dict | None = None,
        raise_not_modified: bool = False,
    ) -> httpx.Response:
        """Perform async GET request.

        Args:
            path: Request path
            headers: Optional headers dict
            raise_not_modified: Raise error on 304 response

        Returns:
            httpx.Response object
        """
        client = await self._get_client()
        request_headers = self._build_headers(headers)

        # Check cache for etag
        cache_key = f"{self._base_url}{path}"
        if self._caching and cache_key in self._cache:
            prev = self._cache[cache_key]
            if "etag" in prev.headers and "if-none-match" not in request_headers:
                request_headers["if-none-match"] = prev.headers["etag"]

        response = await client.get(path, headers=request_headers)

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
        """Perform async POST request.

        Args:
            path: Request path
            data: Raw data to send
            json_data: JSON-serializable data
            headers: Optional headers dict

        Returns:
            httpx.Response object
        """
        client = await self._get_client()
        request_headers = self._build_headers(headers)

        if json_data is not None:
            response = await client.post(path, json=json_data, headers=request_headers)
        else:
            response = await client.post(path, content=data, headers=request_headers)

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
        """Perform async PUT request.

        Args:
            path: Request path
            data: Raw data to send
            json_data: JSON-serializable data
            headers: Optional headers dict
            guard_response: Previous response for conditional update

        Returns:
            httpx.Response object
        """
        client = await self._get_client()
        request_headers = self._build_headers(headers)

        # Add If-Match header for conditional updates
        if guard_response is not None and "etag" in guard_response.headers:
            request_headers["if-match"] = guard_response.headers["etag"]

        if json_data is not None:
            response = await client.put(path, json=json_data, headers=request_headers)
        else:
            response = await client.put(path, content=data, headers=request_headers)

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
        """Perform async DELETE request.

        Args:
            path: Request path
            headers: Optional headers dict
            guard_response: Previous response for conditional delete

        Returns:
            httpx.Response object
        """
        client = await self._get_client()
        request_headers = self._build_headers(headers)

        # Add If-Match header for conditional deletes
        if guard_response is not None and "etag" in guard_response.headers:
            request_headers["if-match"] = guard_response.headers["etag"]

        response = await client.delete(path, headers=request_headers)

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
