"""Async datapath operations for Deriva.

This module extends the datapath API with async fetch capabilities,
following SQLAlchemy's pattern of providing both sync and async interfaces.

The key addition is `fetch_async()` on result sets, which allows
non-blocking data retrieval:

    # Sync (blocking)
    results = path.entities().fetch()

    # Async (non-blocking)
    results = await path.entities().fetch_async()

For clone operations, this enables concurrent data fetching across
multiple tables.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Iterator, TypeVar

from deriva.core import DEFAULT_HEADERS
from deriva.core.asyncio.async_catalog import AsyncErmrestCatalog
from deriva.core.datapath import (
    DataPath,
    DataPathException,
    _CatalogWrapper,
    _ResultSet,
    _http_error_message,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class AsyncResultSet:
    """Async-capable result set for datapath queries.

    This wraps a standard _ResultSet and adds async fetch capability.
    The sync interface remains available for backward compatibility.

    Usage:
        # Create from datapath
        path = catalog.path.Schema.Table
        result_set = AsyncResultSet.from_datapath(path, async_catalog)

        # Async fetch
        results = await result_set.fetch_async()

        # Or iterate asynchronously
        async for batch in result_set.fetch_paged_async(page_size=10000):
            process(batch)
    """

    def __init__(
        self,
        uri: str,
        async_catalog: AsyncErmrestCatalog,
        sync_result_set: _ResultSet | None = None,
    ):
        """Initialize async result set.

        Args:
            uri: The ERMrest entity URI (can be full URL or relative path)
            async_catalog: Async catalog for fetching
            sync_result_set: Optional sync result set for compatibility
        """
        # Extract relative path from full URL if needed
        # The uri from datapath looks like: https://host/ermrest/catalog/ID/entity/...
        # We need just the /entity/... part for use with AsyncErmrestCatalog
        self.uri = self._extract_relative_path(uri, async_catalog)
        self._async_catalog = async_catalog
        self._sync_result_set = sync_result_set
        self._results_doc: list[dict] | None = None
        self._sort_keys: list[Any] | None = None
        self._limit: int | None = None

    @staticmethod
    def _extract_relative_path(uri: str, async_catalog: AsyncErmrestCatalog) -> str:
        """Extract the relative path from a full URL.

        The datapath library generates full URLs like:
            https://host/ermrest/catalog/ID/entity/Schema:Table

        We need to extract just the /entity/Schema:Table part for use
        with AsyncErmrestCatalog which prepends the catalog path.
        """
        # Check if it's a full URL
        if uri.startswith("http://") or uri.startswith("https://"):
            # Find the catalog path portion and remove it
            catalog_path = f"/ermrest/catalog/{async_catalog.catalog_id}/"
            idx = uri.find(catalog_path)
            if idx >= 0:
                # Return the path after the catalog ID
                return "/" + uri[idx + len(catalog_path):]
        # Already a relative path or doesn't match expected pattern
        return uri

    @classmethod
    def from_datapath(
        cls,
        path: DataPath,
        async_catalog: AsyncErmrestCatalog,
    ) -> "AsyncResultSet":
        """Create async result set from a datapath.

        Args:
            path: A DataPath object
            async_catalog: Async catalog for fetching

        Returns:
            AsyncResultSet instance
        """
        # Get the sync result set for the URI
        sync_rs = path.entities()
        return cls(sync_rs.uri, async_catalog, sync_rs)

    def sort(self, *keys) -> "AsyncResultSet":
        """Set sort keys for the result set.

        Args:
            *keys: Column objects to sort by

        Returns:
            self for chaining
        """
        self._sort_keys = list(keys)
        return self

    def limit(self, n: int) -> "AsyncResultSet":
        """Set limit on results.

        Args:
            n: Maximum number of results

        Returns:
            self for chaining
        """
        self._limit = n
        return self

    async def fetch_async(
        self,
        limit: int | None = None,
        headers: dict | None = None,
    ) -> list[dict]:
        """Fetch results asynchronously.

        Args:
            limit: Maximum results to fetch (overrides .limit())
            headers: Optional HTTP headers

        Returns:
            List of entity dictionaries
        """
        headers = headers or DEFAULT_HEADERS
        effective_limit = limit or self._limit

        # Build path with sort and limit
        path = self.uri
        if self._sort_keys:
            sort_str = ",".join(col._uname for col in self._sort_keys)
            path = f"{path}@sort({sort_str})"
        if effective_limit:
            path = f"{path}?limit={effective_limit}"

        logger.debug(f"Async fetching {path}")

        try:
            response = await self._async_catalog.get_async(path, headers=headers)
            self._results_doc = response.json()
            return self._results_doc
        except Exception as e:
            logger.debug(f"Fetch error: {e}")
            raise DataPathException(f"Error fetching {path}: {e}", e)

    async def fetch_paged_async(
        self,
        page_size: int = 10000,
        sort_column: str = "RID",
    ):
        """Async generator that yields pages of results.

        This is the key method for efficient large data transfers.
        Each page is fetched asynchronously, allowing other operations
        to proceed while waiting for network I/O.

        Args:
            page_size: Number of entities per page
            sort_column: Column to sort by for pagination

        Yields:
            List of entity dictionaries for each page

        Example:
            async for page in result_set.fetch_paged_async(10000):
                await upload_page(page)
        """
        after_rid: str | None = None

        while True:
            # Build path with pagination
            path = self.uri
            path = f"{path}@sort({sort_column})"

            if after_rid:
                from deriva.core import urlquote
                path = f"{path}@after({urlquote(after_rid)})"

            path = f"{path}?limit={page_size}"

            logger.debug(f"Async fetching page: {path}")

            try:
                response = await self._async_catalog.get_async(path)
                page = response.json()
            except Exception as e:
                logger.error(f"Paged fetch error: {e}")
                raise DataPathException(f"Error fetching page: {e}", e)

            if not page:
                break

            yield page

            # Set cursor for next page
            after_rid = page[-1].get(sort_column)
            if after_rid is None:
                break

    # Sync compatibility methods

    def fetch(self, limit: int | None = None, headers: dict | None = None) -> list[dict]:
        """Synchronous fetch (blocks the event loop if called from async).

        Prefer fetch_async() in async contexts.
        """
        if self._sync_result_set:
            return self._sync_result_set.fetch(limit, headers)
        # Fall back to running async in sync context
        return asyncio.get_event_loop().run_until_complete(
            self.fetch_async(limit, headers)
        )

    @property
    def _results(self) -> list[dict]:
        """Lazy-load results (sync)."""
        if self._results_doc is None:
            self.fetch()
        return self._results_doc

    def __iter__(self) -> Iterator[dict]:
        """Iterate over results (sync)."""
        return iter(self._results)

    def __len__(self) -> int:
        """Number of results (triggers fetch if needed)."""
        return len(self._results)


class AsyncCatalogWrapper:
    """Async-capable wrapper for catalog datapath operations.

    This extends the standard datapath catalog wrapper with async
    fetch capabilities.

    Usage:
        async_wrapper = AsyncCatalogWrapper(sync_catalog, async_catalog)

        # Access datapath as usual
        path = async_wrapper.path.Schema.Table

        # Get async result set
        results = await async_wrapper.entities_async(path)
    """

    def __init__(
        self,
        sync_catalog,
        async_catalog: AsyncErmrestCatalog,
    ):
        """Initialize wrapper.

        Args:
            sync_catalog: Synchronous ErmrestCatalog
            async_catalog: Async catalog for fetching
        """
        self._sync_catalog = sync_catalog
        self._async_catalog = async_catalog
        self._sync_wrapper = _CatalogWrapper(sync_catalog)

    @property
    def path(self):
        """Access the datapath root (schemas)."""
        return self._sync_wrapper

    def async_result_set(self, path: DataPath) -> AsyncResultSet:
        """Create an async result set from a datapath.

        Args:
            path: DataPath object

        Returns:
            AsyncResultSet for async fetching
        """
        return AsyncResultSet.from_datapath(path, self._async_catalog)

    async def fetch_entities_async(
        self,
        path: DataPath,
        limit: int | None = None,
    ) -> list[dict]:
        """Fetch entities from a path asynchronously.

        Args:
            path: DataPath object
            limit: Maximum results

        Returns:
            List of entity dictionaries
        """
        rs = self.async_result_set(path)
        return await rs.fetch_async(limit)


# Utility functions for clone operations


async def copy_table_async(
    src_wrapper: AsyncCatalogWrapper,
    dst_async_catalog: AsyncErmrestCatalog,
    table_spec: str,
    page_size: int = 10000,
    pipeline_depth: int = 3,
) -> int:
    """Copy a table's data asynchronously using datapath.

    Uses a producer-consumer pipeline:
    - Producer: async generator yielding pages from source
    - Consumer: posts pages to destination

    Args:
        src_wrapper: Source catalog wrapper with datapath access
        dst_async_catalog: Destination async catalog
        table_spec: Table specification (schema:table)
        page_size: Rows per page
        pipeline_depth: Buffer size for pipeline

    Returns:
        Number of rows copied
    """
    from urllib.parse import quote as urlquote

    # Parse table spec
    schema_name, table_name = table_spec.split(":", 1)

    # Get datapath for source table
    src_path = getattr(getattr(src_wrapper.path, schema_name), table_name)
    src_result = src_wrapper.async_result_set(src_path)

    # Pipeline queue
    queue: asyncio.Queue[list[dict] | None] = asyncio.Queue(maxsize=pipeline_depth)
    rows_copied = 0

    async def producer():
        """Fetch pages and put in queue."""
        try:
            async for page in src_result.fetch_paged_async(page_size):
                await queue.put(page)
        finally:
            await queue.put(None)  # Signal end

    async def consumer():
        """Upload pages from queue."""
        nonlocal rows_copied
        while True:
            page = await queue.get()
            if page is None:
                break

            # Post to destination
            dst_path = f"/entity/{urlquote(table_spec, safe=':@')}?nondefaults=RID,RCT,RCB"
            await dst_async_catalog.post_async(dst_path, json_data=page)
            rows_copied += len(page)

    # Run producer and consumer concurrently
    await asyncio.gather(producer(), consumer())

    logger.info(f"Copied {rows_copied} rows from {table_spec}")
    return rows_copied


async def copy_tables_concurrent_async(
    src_wrapper: AsyncCatalogWrapper,
    dst_async_catalog: AsyncErmrestCatalog,
    table_specs: list[str],
    table_concurrency: int = 5,
    page_size: int = 10000,
    progress_callback: Callable[[str, int], None] | None = None,
) -> dict[str, int]:
    """Copy multiple tables concurrently using datapath.

    Args:
        src_wrapper: Source catalog wrapper
        dst_async_catalog: Destination async catalog
        table_specs: List of table specifications
        table_concurrency: Max concurrent table copies
        page_size: Rows per page
        progress_callback: Optional callback(table_spec, rows_copied)

    Returns:
        Dict mapping table_spec to rows_copied
    """
    semaphore = asyncio.Semaphore(table_concurrency)
    results: dict[str, int] = {}

    async def copy_one(table_spec: str) -> tuple[str, int]:
        async with semaphore:
            logger.info(f"Starting copy of {table_spec}")
            rows = await copy_table_async(
                src_wrapper,
                dst_async_catalog,
                table_spec,
                page_size,
            )
            if progress_callback:
                progress_callback(table_spec, rows)
            return table_spec, rows

    # Copy all tables concurrently (limited by semaphore)
    tasks = [copy_one(ts) for ts in table_specs]
    for coro in asyncio.as_completed(tasks):
        table_spec, rows = await coro
        results[table_spec] = rows

    return results
