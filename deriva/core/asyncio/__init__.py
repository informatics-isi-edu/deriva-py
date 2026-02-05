"""Async support for Deriva using asyncio.

This module provides async versions of Deriva classes that work with asyncio,
following patterns similar to SQLAlchemy's asyncio extension.

The implementation uses httpx.AsyncClient for async HTTP requests, providing
concurrent execution capabilities for operations like catalog cloning.

Usage:
    from deriva.core.asyncio import AsyncErmrestCatalog, AsyncDerivaServer

    async def main():
        server = AsyncDerivaServer("https", "example.org", credentials)
        catalog = await server.connect_ermrest_async("1")

        # Perform async operations
        result = await catalog.get_async("/entity/schema:table")

        # Or use the sync-in-async bridge
        def sync_operation(catalog):
            return catalog.getCatalogModel()

        model = await catalog.run_sync(sync_operation)

The module provides two approaches for async operations:

1. Native async methods (get_async, post_async, etc.) - Full async implementation
2. run_sync() bridge - Run existing sync code in a thread pool (like SQLAlchemy)

For datapath operations, use AsyncCatalogWrapper:

    from deriva.core.asyncio import AsyncCatalogWrapper, AsyncResultSet

    # Combine sync catalog (for datapath) with async catalog (for fetching)
    wrapper = AsyncCatalogWrapper(sync_catalog, async_catalog)

    # Use datapath normally
    path = wrapper.path.Schema.Table
    result_set = wrapper.async_result_set(path)

    # Fetch asynchronously
    results = await result_set.fetch_async()

    # Or iterate through pages
    async for page in result_set.fetch_paged_async(page_size=10000):
        process(page)

For table-level async copying, use AsyncTableCopier:

    from deriva.core.asyncio.clone import AsyncTableCopier

    copier = AsyncTableCopier(src_wrapper, dst_catalog, "Schema", "Table")
    rows_copied = await copier.copy_async()

For full catalog clone orchestration (FK handling, orphan strategies, etc.),
see ``deriva_ml.catalog.clone``.
"""

from deriva.core.asyncio.async_binding import AsyncDerivaBinding
from deriva.core.asyncio.async_catalog import AsyncErmrestCatalog
from deriva.core.asyncio.async_server import AsyncDerivaServer
from deriva.core.asyncio.async_datapath import AsyncCatalogWrapper, AsyncResultSet
from deriva.core.asyncio.clone import AsyncTableCopier, AsyncCloneResult

__all__ = [
    "AsyncDerivaBinding",
    "AsyncErmrestCatalog",
    "AsyncDerivaServer",
    "AsyncCatalogWrapper",
    "AsyncResultSet",
    "AsyncTableCopier",
    "AsyncCloneResult",
]
