"""Async table copying utilities using datapath operations.

This module provides AsyncTableCopier for concurrent copying of table data
between catalogs using the datapath API. It uses a producer-consumer pattern
with asyncio queues to pipeline fetching and uploading pages.

Key optimization: fetch page N while uploading page N-1 (pipeline parallelism).

For catalog clone orchestration (full or partial clones with FK handling,
orphan strategies, etc.), see `deriva_ml.catalog.clone`.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from deriva.core.asyncio.async_catalog import AsyncErmrestCatalog
from deriva.core.asyncio.async_datapath import AsyncCatalogWrapper

logger = logging.getLogger(__name__)


@dataclass
class AsyncCloneResult:
    """Result of an async catalog clone operation."""

    catalog_id: str
    hostname: str
    source_hostname: str
    source_catalog_id: str
    tables_copied: int = 0
    rows_copied: int = 0
    errors: list[str] = field(default_factory=list)


class AsyncTableCopier:
    """Handles concurrent copying of a single table's data using datapath.

    Uses a producer-consumer pattern with asyncio queues to pipeline
    fetching and uploading pages. All source queries use the datapath API
    for type-safe schema navigation.
    """

    def __init__(
        self,
        src_wrapper: AsyncCatalogWrapper,
        dst_catalog: AsyncErmrestCatalog,
        schema_name: str,
        table_name: str,
        page_size: int = 10000,
        pipeline_depth: int = 3,
    ):
        """Initialize table copier.

        Args:
            src_wrapper: Source catalog wrapper with datapath access
            dst_catalog: Destination async catalog
            schema_name: Schema name
            table_name: Table name
            page_size: Number of rows per page
            pipeline_depth: Number of pages to buffer (concurrent fetches)
        """
        self.src_wrapper = src_wrapper
        self.dst_catalog = dst_catalog
        self.schema_name = schema_name
        self.table_name = table_name
        self.table_spec = f"{schema_name}:{table_name}"
        self.page_size = page_size
        self.pipeline_depth = pipeline_depth
        self.rows_copied = 0
        self.pages_copied = 0

    async def copy_async(self) -> int:
        """Copy all data from source to destination table using datapath.

        Returns:
            Number of rows copied
        """
        # Use asyncio.Queue for producer-consumer pipeline
        queue: asyncio.Queue[list[dict] | None] = asyncio.Queue(maxsize=self.pipeline_depth)

        # Start producer and consumer tasks
        producer = asyncio.create_task(self._fetch_pages_datapath(queue))
        consumer = asyncio.create_task(self._upload_pages(queue))

        # Wait for both to complete
        await asyncio.gather(producer, consumer)

        logger.info(f"Copied {self.rows_copied} rows from {self.table_spec}")
        return self.rows_copied

    async def _fetch_pages_datapath(self, queue: asyncio.Queue) -> None:
        """Fetch pages from source using datapath API."""
        try:
            # Get the datapath for this table
            schema_path = getattr(self.src_wrapper.path, self.schema_name)
            table_path = getattr(schema_path, self.table_name)

            # Create async result set from the datapath
            result_set = self.src_wrapper.async_result_set(table_path)

            # Fetch pages asynchronously using datapath
            async for page in result_set.fetch_paged_async(
                page_size=self.page_size,
                sort_column="RID",
            ):
                await queue.put(page)
        finally:
            # Signal end of data
            await queue.put(None)

    async def _upload_pages(self, queue: asyncio.Queue) -> None:
        """Upload pages from queue to destination."""
        while True:
            page = await queue.get()
            if page is None:
                break

            try:
                await self.dst_catalog.post_entities_async(
                    self.table_spec,
                    page,
                    nondefaults=["RID", "RCT", "RCB"],
                )
                self.rows_copied += len(page)
                self.pages_copied += 1
            except Exception as e:
                logger.error(f"Error uploading page to {self.table_spec}: {e}")
                raise
