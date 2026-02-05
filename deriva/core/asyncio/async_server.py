"""Async DerivaServer implementation."""

from __future__ import annotations

import logging

from deriva.core.asyncio.async_catalog import AsyncErmrestCatalog

logger = logging.getLogger(__name__)


class AsyncDerivaServer:
    """Async wrapper for DerivaServer operations.

    Provides async methods for creating and connecting to catalogs.
    """

    def __init__(
        self,
        scheme: str,
        server: str,
        credentials: dict | None = None,
        session_config: dict | None = None,
    ):
        """Initialize async server connection.

        Args:
            scheme: HTTP scheme ("http" or "https")
            server: Server hostname
            credentials: Authentication credentials
            session_config: Session configuration overrides
        """
        self.scheme = scheme
        self.server = server
        self.credentials = credentials
        self._session_config = session_config

    def connect_ermrest(self, catalog_id: str | int) -> AsyncErmrestCatalog:
        """Connect to an existing catalog asynchronously.

        Args:
            catalog_id: Catalog identifier

        Returns:
            AsyncErmrestCatalog instance
        """
        return AsyncErmrestCatalog(
            self.scheme,
            self.server,
            catalog_id,
            self.credentials,
            session_config=self._session_config,
        )
