# Tests for the async clone module.
#
# These tests verify the AsyncTableCopier functionality.
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

from deriva.core.asyncio.clone import (
    AsyncTableCopier,
    AsyncCloneResult,
)

logger = logging.getLogger(__name__)
if os.getenv("DERIVA_PY_TEST_VERBOSE"):
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

hostname = os.getenv("DERIVA_PY_TEST_HOSTNAME")


class TestAsyncCloneResultDataclass(unittest.TestCase):
    """Unit tests for AsyncCloneResult dataclass."""

    def test_required_fields(self):
        """Should require catalog info."""
        result = AsyncCloneResult(
            catalog_id="42",
            hostname="dest.example.org",
            source_hostname="src.example.org",
            source_catalog_id="1",
        )

        self.assertEqual(result.catalog_id, "42")
        self.assertEqual(result.hostname, "dest.example.org")

    def test_default_counters(self):
        """Should have zero counters by default."""
        result = AsyncCloneResult(
            catalog_id="42",
            hostname="dest.example.org",
            source_hostname="src.example.org",
            source_catalog_id="1",
        )

        self.assertEqual(result.tables_copied, 0)
        self.assertEqual(result.rows_copied, 0)
        self.assertEqual(result.errors, [])


class TestAsyncTableCopierUnit(unittest.TestCase):
    """Unit tests for AsyncTableCopier."""

    def test_init_stores_config(self):
        """Constructor should store configuration."""
        mock_wrapper = MagicMock()
        mock_dst = MagicMock()

        copier = AsyncTableCopier(
            mock_wrapper,
            mock_dst,
            "ISA",
            "Experiment",
            page_size=5000,
            pipeline_depth=5,
        )

        self.assertEqual(copier.schema_name, "ISA")
        self.assertEqual(copier.table_name, "Experiment")
        self.assertEqual(copier.table_spec, "ISA:Experiment")
        self.assertEqual(copier.page_size, 5000)
        self.assertEqual(copier.pipeline_depth, 5)
        self.assertEqual(copier.rows_copied, 0)


class TestAsyncTableCopierAsync(unittest.IsolatedAsyncioTestCase):
    """Async unit tests for AsyncTableCopier."""

    async def test_copy_async_empty_table(self):
        """Should handle empty source table."""
        # Setup mocks
        mock_wrapper = MagicMock()
        mock_dst = AsyncMock()

        # Mock datapath access
        mock_table_path = MagicMock()
        mock_schema = MagicMock()
        mock_schema.Experiment = mock_table_path
        mock_wrapper.path = MagicMock()
        mock_wrapper.path.ISA = mock_schema

        # Mock async result set that yields empty
        class EmptyResultSet:
            async def fetch_paged_async(self, page_size=10000, sort_column="RID"):
                return
                yield  # Make this an async generator

        mock_wrapper.async_result_set.return_value = EmptyResultSet()

        copier = AsyncTableCopier(mock_wrapper, mock_dst, "ISA", "Experiment")
        rows = await copier.copy_async()

        self.assertEqual(rows, 0)
        self.assertEqual(copier.rows_copied, 0)

    async def test_copy_async_with_data(self):
        """Should copy data from source to destination."""
        # Setup mocks
        mock_wrapper = MagicMock()
        mock_dst = AsyncMock()

        # Mock datapath access
        mock_table_path = MagicMock()
        mock_schema = MagicMock()
        mock_schema.Experiment = mock_table_path
        mock_wrapper.path = MagicMock()
        mock_wrapper.path.ISA = mock_schema

        # Mock async result set that yields one page
        class DataResultSet:
            async def fetch_paged_async(self, page_size=10000, sort_column="RID"):
                yield [{"RID": "1", "Name": "exp1"}, {"RID": "2", "Name": "exp2"}]

        mock_wrapper.async_result_set.return_value = DataResultSet()

        # Mock destination post
        mock_dst.post_entities_async.return_value = None

        copier = AsyncTableCopier(mock_wrapper, mock_dst, "ISA", "Experiment")
        rows = await copier.copy_async()

        self.assertEqual(rows, 2)
        self.assertEqual(copier.rows_copied, 2)
        mock_dst.post_entities_async.assert_called_once()


@unittest.skipUnless(hostname, "Test host not specified")
class TestAsyncCloneIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for async clone operations.

    These tests create actual catalogs and clone data between them.
    """

    source_catalog = None
    dest_catalog_ids = []

    @classmethod
    def setUpClass(cls):
        """Create source catalog with test data."""
        from deriva.core import DerivaServer, get_credential, ermrest_model as _em

        credential = os.getenv("DERIVA_PY_TEST_CREDENTIAL") or get_credential(hostname)
        cls.credential = credential

        server = DerivaServer("https", hostname, credentials=credential)
        cls.source_catalog = server.create_ermrest_catalog()
        cls.source_catalog_id = cls.source_catalog.catalog_id

        try:
            # Create simple test schema
            model = cls.source_catalog.getCatalogModel()

            # Create test schema
            test_schema = model.create_schema(_em.Schema.define("TestSchema"))
            test_schema.create_table(_em.Table.define(
                "TestTable",
                column_defs=[
                    _em.Column.define("Name", _em.builtin_types.text),
                    _em.Column.define("Value", _em.builtin_types.int4),
                ],
                key_defs=[_em.Key.define(["Name"])]
            ))

            # Populate data
            paths = cls.source_catalog.getPathBuilder()
            paths.TestSchema.TestTable.insert([
                {"Name": f"item-{i}", "Value": i}
                for i in range(50)
            ])

        except Exception:
            cls.source_catalog.delete_ermrest_catalog(really=True)
            raise

    @classmethod
    def tearDownClass(cls):
        """Clean up all created catalogs."""
        from deriva.core import DerivaServer, get_credential

        credential = os.getenv("DERIVA_PY_TEST_CREDENTIAL") or get_credential(hostname)
        server = DerivaServer("https", hostname, credentials=credential)

        # Delete source catalog
        if cls.source_catalog:
            cls.source_catalog.delete_ermrest_catalog(really=True)

        # Delete any destination catalogs created during tests
        for cat_id in cls.dest_catalog_ids:
            try:
                cat = server.connect_ermrest(cat_id)
                cat.delete_ermrest_catalog(really=True)
            except Exception:
                pass

    async def test_table_copier_integration(self):
        """AsyncTableCopier should copy real data."""
        from deriva.core import DerivaServer, ErmrestCatalog
        from deriva.core.asyncio import AsyncErmrestCatalog, AsyncCatalogWrapper

        # Create destination catalog
        server = DerivaServer("https", hostname, credentials=self.credential)
        dest_sync = server.create_ermrest_catalog()
        self.dest_catalog_ids.append(dest_sync.catalog_id)

        try:
            # Create matching schema in destination
            from deriva.core import ermrest_model as _em
            dest_model = dest_sync.getCatalogModel()
            test_schema = dest_model.create_schema(_em.Schema.define("TestSchema"))
            test_schema.create_table(_em.Table.define(
                "TestTable",
                column_defs=[
                    _em.Column.define("Name", _em.builtin_types.text),
                    _em.Column.define("Value", _em.builtin_types.int4),
                ],
                key_defs=[_em.Key.define(["Name"])]
            ))

            # Setup async catalogs
            src_async = AsyncErmrestCatalog(
                "https", hostname, self.source_catalog_id, self.credential
            )
            src_wrapper = AsyncCatalogWrapper(self.source_catalog, src_async)

            dst_async = AsyncErmrestCatalog(
                "https", hostname, dest_sync.catalog_id, self.credential
            )

            try:
                # Copy table
                copier = AsyncTableCopier(
                    src_wrapper, dst_async, "TestSchema", "TestTable"
                )
                rows_copied = await copier.copy_async()

                self.assertEqual(rows_copied, 50)

                # Verify data in destination
                dest_paths = dest_sync.getPathBuilder()
                dest_data = list(dest_paths.TestSchema.TestTable.entities())
                self.assertEqual(len(dest_data), 50)

            finally:
                await src_async.close()
                await dst_async.close()

        finally:
            dest_sync.delete_ermrest_catalog(really=True)
            self.dest_catalog_ids.remove(dest_sync.catalog_id)


if __name__ == "__main__":
    unittest.main()
