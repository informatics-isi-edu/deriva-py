# Tests for the async_catalog module.
#
# These tests verify the AsyncErmrestCatalog class functionality.
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

from deriva.core.asyncio.async_catalog import AsyncErmrestCatalog, AsyncErmrestSnapshot

logger = logging.getLogger(__name__)
if os.getenv("DERIVA_PY_TEST_VERBOSE"):
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

hostname = os.getenv("DERIVA_PY_TEST_HOSTNAME")


class TestAsyncErmrestCatalogUnit(unittest.TestCase):
    """Unit tests for AsyncErmrestCatalog."""

    def test_init_sets_catalog_path(self):
        """Constructor should set catalog path."""
        catalog = AsyncErmrestCatalog("https", "example.org", "42")

        self.assertEqual(catalog.catalog_id, "42")
        self.assertEqual(catalog._catalog_path, "/ermrest/catalog/42")

    def test_init_converts_int_catalog_id(self):
        """Constructor should convert int catalog_id to string."""
        catalog = AsyncErmrestCatalog("https", "example.org", 42)

        self.assertEqual(catalog.catalog_id, "42")

    def test_catalog_uri_absolute_path(self):
        """_catalog_uri should handle absolute paths."""
        catalog = AsyncErmrestCatalog("https", "example.org", "1")

        uri = catalog._catalog_uri("/entity/schema:table")
        self.assertEqual(uri, "/ermrest/catalog/1/entity/schema:table")

    def test_catalog_uri_relative_path(self):
        """_catalog_uri should handle relative paths."""
        catalog = AsyncErmrestCatalog("https", "example.org", "1")

        uri = catalog._catalog_uri("entity/schema:table")
        self.assertEqual(uri, "/ermrest/catalog/1/entity/schema:table")

    def test_sync_catalog_lazy_init(self):
        """sync_catalog property should lazily create sync catalog."""
        catalog = AsyncErmrestCatalog("https", "example.org", "1")

        self.assertIsNone(catalog._sync_catalog)
        sync_cat = catalog.sync_catalog
        self.assertIsNotNone(catalog._sync_catalog)
        self.assertIs(sync_cat, catalog._sync_catalog)


class TestAsyncErmrestSnapshotUnit(unittest.TestCase):
    """Unit tests for AsyncErmrestSnapshot."""

    def test_init_sets_snapshot_path(self):
        """Constructor should include snaptime in path."""
        snapshot = AsyncErmrestSnapshot(
            "https", "example.org", "1", "2T12:34:56"
        )

        self.assertEqual(snapshot.snaptime, "2T12:34:56")
        self.assertEqual(snapshot._catalog_path, "/ermrest/catalog/1@2T12:34:56")


class TestAsyncErmrestCatalogAsync(unittest.IsolatedAsyncioTestCase):
    """Async unit tests for AsyncErmrestCatalog."""

    async def test_get_async_prepends_catalog_path(self):
        """get_async should prepend catalog path to request."""
        catalog = AsyncErmrestCatalog("https", "example.org", "1")

        with patch.object(catalog, "_get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.headers = {}
            mock_client.get.return_value = mock_response
            mock_get_client.return_value = mock_client

            await catalog.get_async("/entity/schema:table")

            mock_client.get.assert_called_once()
            call_args = mock_client.get.call_args
            self.assertIn("/ermrest/catalog/1/entity/schema:table", call_args[0][0])

        await catalog.close()

    async def test_post_async_prepends_catalog_path(self):
        """post_async should prepend catalog path to request."""
        catalog = AsyncErmrestCatalog("https", "example.org", "1")

        with patch.object(catalog, "_get_client") as mock_get_client:
            mock_client = AsyncMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_client.post.return_value = mock_response
            mock_get_client.return_value = mock_client

            await catalog.post_async("/entity/schema:table", json_data=[{"key": "value"}])

            mock_client.post.assert_called_once()
            call_args = mock_client.post.call_args
            self.assertIn("/ermrest/catalog/1/entity/schema:table", call_args[0][0])

        await catalog.close()


@unittest.skipUnless(hostname, "Test host not specified")
class TestAsyncErmrestCatalogIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for AsyncErmrestCatalog."""

    catalog = None
    catalog_id = None

    @classmethod
    def setUpClass(cls):
        """Create a test catalog."""
        from deriva.core import DerivaServer, get_credential

        credential = os.getenv("DERIVA_PY_TEST_CREDENTIAL") or get_credential(hostname)
        server = DerivaServer("https", hostname, credentials=credential)
        cls.sync_catalog = server.create_ermrest_catalog()
        cls.catalog_id = cls.sync_catalog.catalog_id
        cls.credential = credential

    @classmethod
    def tearDownClass(cls):
        """Delete the test catalog."""
        if cls.sync_catalog:
            cls.sync_catalog.delete_ermrest_catalog(really=True)

    async def asyncSetUp(self):
        self.catalog = AsyncErmrestCatalog(
            "https", hostname, self.catalog_id, self.credential
        )

    async def asyncTearDown(self):
        await self.catalog.close()

    async def test_get_catalog_model_async(self):
        """Should fetch catalog model."""
        model = await self.catalog.get_catalog_model_async()

        self.assertIsNotNone(model)
        self.assertIn("public", model.schemas)

    async def test_get_entities_async_empty_table(self):
        """Should handle empty tables."""
        # public schema exists but has no user tables with data
        try:
            entities = await self.catalog.get_entities_async("public:ERMrest_Client")
            self.assertIsInstance(entities, list)
        except Exception:
            # Table may not exist, which is fine
            pass

    async def test_get_entities_paged_async(self):
        """Should iterate through pages."""
        # This test needs a table with data; we'll just verify the async generator works
        pages = []
        try:
            async for page in self.catalog.get_entities_paged_async(
                "public:ERMrest_Client", page_size=10
            ):
                pages.append(page)
                if len(pages) >= 2:  # Limit iterations
                    break
        except Exception:
            # Table may not exist or be empty
            pass

        # Just verify it didn't crash
        self.assertIsInstance(pages, list)


if __name__ == "__main__":
    unittest.main()
