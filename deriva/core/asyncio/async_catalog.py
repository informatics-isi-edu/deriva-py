"""Async ErmrestCatalog implementation.

This module provides an async version of ErmrestCatalog for use with asyncio.
It wraps the synchronous catalog and provides async-native methods.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, TypeVar

from deriva.core import ErmrestCatalog
from deriva.core.asyncio.async_binding import AsyncDerivaBinding

logger = logging.getLogger(__name__)

T = TypeVar("T")


class AsyncErmrestCatalog(AsyncDerivaBinding):
    """Async wrapper for ErmrestCatalog operations.

    Provides both native async methods and a bridge to the synchronous
    ErmrestCatalog for operations that haven't been converted to async.

    The sync_catalog property provides access to the underlying sync
    catalog for operations that require the full sync API.

    Example:
        async with AsyncErmrestCatalog("https", "example.org", "1", creds) as catalog:
            # Native async operations
            response = await catalog.get_async("/entity/schema:table")
            data = response.json()

            # Bridge to sync operations
            model = await catalog.run_sync(lambda c: c.getCatalogModel(), catalog.sync_catalog)
    """

    def __init__(
        self,
        scheme: str,
        server: str,
        catalog_id: str | int,
        credentials: dict | None = None,
        caching: bool = True,
        session_config: dict | None = None,
    ):
        """Initialize async catalog.

        Args:
            scheme: HTTP scheme ("http" or "https")
            server: Server hostname
            catalog_id: Catalog identifier
            credentials: Authentication credentials
            caching: Enable response caching
            session_config: Session configuration overrides
        """
        super().__init__(scheme, server, credentials, caching, session_config)
        self.catalog_id = str(catalog_id)
        self._catalog_path = f"/ermrest/catalog/{self.catalog_id}"

        # Lazy-initialized sync catalog for bridge operations
        self._sync_catalog: ErmrestCatalog | None = None

    @property
    def sync_catalog(self) -> ErmrestCatalog:
        """Get the underlying synchronous catalog.

        Creates the sync catalog on first access. This is useful for
        operations that haven't been converted to async yet.
        """
        if self._sync_catalog is None:
            self._sync_catalog = ErmrestCatalog(
                self.scheme,
                self.server,
                self.catalog_id,
                self.credentials,
                self._caching,
                self._session_config,
            )
        return self._sync_catalog

    def _catalog_uri(self, path: str) -> str:
        """Build full catalog URI from relative path."""
        if path.startswith("/"):
            return f"{self._catalog_path}{path}"
        return f"{self._catalog_path}/{path}"

    async def get_async(
        self,
        path: str,
        headers: dict | None = None,
        raise_not_modified: bool = False,
    ):
        """Async GET on catalog path."""
        return await super().get_async(self._catalog_uri(path), headers, raise_not_modified)

    async def post_async(
        self,
        path: str,
        data: bytes | str | None = None,
        json_data: Any | None = None,
        headers: dict | None = None,
    ):
        """Async POST to catalog path."""
        return await super().post_async(self._catalog_uri(path), data, json_data, headers)

    async def put_async(
        self,
        path: str,
        data: bytes | str | None = None,
        json_data: Any | None = None,
        headers: dict | None = None,
        guard_response=None,
    ):
        """Async PUT to catalog path."""
        return await super().put_async(self._catalog_uri(path), data, json_data, headers, guard_response)

    async def delete_async(
        self,
        path: str,
        headers: dict | None = None,
        guard_response=None,
    ):
        """Async DELETE on catalog path."""
        return await super().delete_async(self._catalog_uri(path), headers, guard_response)

    async def get_catalog_model_async(self) -> Any:
        """Async version of getCatalogModel().

        Fetches the catalog schema and returns an ERMrest Model object.
        """
        # Use sync bridge for now - the Model parsing is complex
        return await self.run_sync(lambda: self.sync_catalog.getCatalogModel())

    async def get_entities_async(
        self,
        table_spec: str,
        limit: int | None = None,
        sort: str | None = None,
        after: str | None = None,
    ) -> list[dict]:
        """Fetch entities from a table asynchronously.

        Args:
            table_spec: Table specification (schema:table or schema:table@snapshot)
            limit: Maximum number of entities to return
            sort: Sort specification (e.g., "RID")
            after: Pagination cursor (last RID from previous page)

        Returns:
            List of entity dictionaries
        """
        from urllib.parse import quote as urlquote

        path = f"/entity/{urlquote(table_spec, safe=':@')}"

        # Build query parameters
        params = []
        if sort:
            params.append(f"@sort({sort})")
        if after:
            params.append(f"@after({urlquote(after)})")
        if limit:
            params.append(f"limit={limit}")

        if params:
            path = path + "?" + "&".join(params)

        response = await self.get_async(path)
        return response.json()

    async def get_entities_paged_async(
        self,
        table_spec: str,
        page_size: int = 10000,
        sort: str = "RID",
    ):
        """Async generator that yields pages of entities.

        This is useful for iterating over large tables without
        loading everything into memory.

        Args:
            table_spec: Table specification
            page_size: Number of entities per page
            sort: Sort column for pagination

        Yields:
            List of entity dictionaries for each page
        """
        after = None
        while True:
            page = await self.get_entities_async(
                table_spec,
                limit=page_size,
                sort=sort,
                after=after,
            )
            if not page:
                break
            yield page
            after = page[-1].get("RID")

    async def post_entities_async(
        self,
        table_spec: str,
        entities: list[dict],
        defaults: list[str] | None = None,
        nondefaults: list[str] | None = None,
    ) -> list[dict]:
        """Insert entities into a table asynchronously.

        Args:
            table_spec: Table specification
            entities: List of entity dictionaries to insert
            defaults: Columns to use default values for
            nondefaults: Columns to preserve (not use defaults)

        Returns:
            List of inserted entity dictionaries (with generated values)
        """
        from urllib.parse import quote as urlquote

        path = f"/entity/{urlquote(table_spec, safe=':@')}"

        # Build query parameters
        params = []
        if defaults:
            params.append(f"defaults={','.join(defaults)}")
        if nondefaults:
            params.append(f"nondefaults={','.join(nondefaults)}")

        if params:
            path = path + "?" + "&".join(params)

        response = await self.post_async(path, json_data=entities)
        return response.json()

    async def close(self) -> None:
        """Close async resources."""
        await super().close()
        # Note: sync_catalog cleanup handled by garbage collection


class AsyncErmrestSnapshot(AsyncErmrestCatalog):
    """Async wrapper for catalog snapshots.

    Provides access to a specific point-in-time snapshot of a catalog.
    """

    def __init__(
        self,
        scheme: str,
        server: str,
        catalog_id: str | int,
        snaptime: str,
        credentials: dict | None = None,
        caching: bool = True,
        session_config: dict | None = None,
    ):
        """Initialize async snapshot.

        Args:
            scheme: HTTP scheme
            server: Server hostname
            catalog_id: Catalog identifier
            snaptime: Snapshot timestamp
            credentials: Authentication credentials
            caching: Enable response caching
            session_config: Session configuration overrides
        """
        super().__init__(scheme, server, catalog_id, credentials, caching, session_config)
        self.snaptime = snaptime
        self._catalog_path = f"/ermrest/catalog/{self.catalog_id}@{snaptime}"
