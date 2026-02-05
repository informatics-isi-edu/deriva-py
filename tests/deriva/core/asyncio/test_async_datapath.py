# Tests for the async_datapath module.
#
# These tests verify the AsyncResultSet and AsyncCatalogWrapper classes.
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

from deriva.core.asyncio.async_datapath import (
    AsyncResultSet,
    AsyncCatalogWrapper,
)

logger = logging.getLogger(__name__)
if os.getenv("DERIVA_PY_TEST_VERBOSE"):
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

hostname = os.getenv("DERIVA_PY_TEST_HOSTNAME")

# Test constants matching test_datapath.py
SNAME_ISA = "ISA"
SNAME_VOCAB = "Vocab"
TNAME_PROJECT = "Project"
TNAME_EXPERIMENT = "Experiment"
TNAME_EXPERIMENT_TYPE = "Experiment_Type"
TEST_EXP_MAX = 100
TEST_EXPTYPE_MAX = 10


class TestAsyncResultSetUnit(unittest.TestCase):
    """Unit tests for AsyncResultSet."""

    def test_init_stores_uri(self):
        """Constructor should store URI."""
        mock_async_catalog = MagicMock()
        rs = AsyncResultSet("/entity/schema:table", mock_async_catalog)

        self.assertEqual(rs.uri, "/entity/schema:table")
        self.assertIs(rs._async_catalog, mock_async_catalog)

    def test_sort_returns_self(self):
        """sort() should return self for chaining."""
        mock_async_catalog = MagicMock()
        rs = AsyncResultSet("/entity/schema:table", mock_async_catalog)

        mock_col = MagicMock()
        mock_col._uname = "Name"

        result = rs.sort(mock_col)
        self.assertIs(result, rs)
        self.assertEqual(rs._sort_keys, [mock_col])

    def test_limit_returns_self(self):
        """limit() should return self for chaining."""
        mock_async_catalog = MagicMock()
        rs = AsyncResultSet("/entity/schema:table", mock_async_catalog)

        result = rs.limit(100)
        self.assertIs(result, rs)
        self.assertEqual(rs._limit, 100)

    def test_chaining(self):
        """sort() and limit() should chain."""
        mock_async_catalog = MagicMock()
        rs = AsyncResultSet("/entity/schema:table", mock_async_catalog)

        mock_col = MagicMock()
        mock_col._uname = "Name"

        result = rs.sort(mock_col).limit(50)
        self.assertIs(result, rs)
        self.assertEqual(rs._sort_keys, [mock_col])
        self.assertEqual(rs._limit, 50)


class TestAsyncResultSetAsync(unittest.IsolatedAsyncioTestCase):
    """Async unit tests for AsyncResultSet."""

    async def test_fetch_async_makes_request(self):
        """fetch_async should make GET request to async catalog."""
        mock_async_catalog = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = [{"RID": "1"}, {"RID": "2"}]
        mock_async_catalog.get_async.return_value = mock_response

        rs = AsyncResultSet("/entity/schema:table", mock_async_catalog)
        results = await rs.fetch_async()

        self.assertEqual(results, [{"RID": "1"}, {"RID": "2"}])
        mock_async_catalog.get_async.assert_called_once()

    async def test_fetch_async_with_limit(self):
        """fetch_async should include limit in path."""
        mock_async_catalog = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = [{"RID": "1"}]
        mock_async_catalog.get_async.return_value = mock_response

        rs = AsyncResultSet("/entity/schema:table", mock_async_catalog)
        await rs.fetch_async(limit=10)

        call_args = mock_async_catalog.get_async.call_args
        self.assertIn("limit=10", call_args[0][0])

    async def test_fetch_async_with_sort(self):
        """fetch_async should include sort in path."""
        mock_async_catalog = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_async_catalog.get_async.return_value = mock_response

        mock_col = MagicMock()
        mock_col._uname = "Name"

        rs = AsyncResultSet("/entity/schema:table", mock_async_catalog)
        rs.sort(mock_col)
        await rs.fetch_async()

        call_args = mock_async_catalog.get_async.call_args
        self.assertIn("@sort(Name)", call_args[0][0])

    async def test_fetch_paged_async_yields_pages(self):
        """fetch_paged_async should yield pages of results."""
        mock_async_catalog = AsyncMock()

        # First page has results, second page is empty
        responses = [
            MagicMock(json=lambda: [{"RID": "1"}, {"RID": "2"}]),
            MagicMock(json=lambda: []),
        ]
        mock_async_catalog.get_async.side_effect = responses

        rs = AsyncResultSet("/entity/schema:table", mock_async_catalog)
        pages = []
        async for page in rs.fetch_paged_async(page_size=10):
            pages.append(page)

        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0], [{"RID": "1"}, {"RID": "2"}])


class TestAsyncCatalogWrapperUnit(unittest.TestCase):
    """Unit tests for AsyncCatalogWrapper."""

    def test_init_stores_catalogs(self):
        """Constructor should store both catalogs."""
        mock_sync = MagicMock()
        mock_async = MagicMock()

        wrapper = AsyncCatalogWrapper(mock_sync, mock_async)

        self.assertIs(wrapper._sync_catalog, mock_sync)
        self.assertIs(wrapper._async_catalog, mock_async)

    def test_path_returns_datapath(self):
        """path property should return datapath root."""
        mock_sync = MagicMock()
        mock_async = MagicMock()

        wrapper = AsyncCatalogWrapper(mock_sync, mock_async)
        path = wrapper.path

        self.assertIsNotNone(path)


class TestAsyncCatalogWrapperAsync(unittest.IsolatedAsyncioTestCase):
    """Async unit tests for AsyncCatalogWrapper."""

    async def test_fetch_entities_async(self):
        """fetch_entities_async should return entities."""
        mock_sync = MagicMock()
        mock_async = AsyncMock()

        # Setup mock datapath
        mock_path = MagicMock()
        mock_path.entities.return_value = MagicMock(uri="/entity/schema:table")

        mock_response = MagicMock()
        mock_response.json.return_value = [{"RID": "1"}]
        mock_async.get_async.return_value = mock_response

        wrapper = AsyncCatalogWrapper(mock_sync, mock_async)
        result_set = wrapper.async_result_set(mock_path)
        results = await result_set.fetch_async()

        self.assertEqual(results, [{"RID": "1"}])


@unittest.skipUnless(hostname, "Test host not specified")
class TestAsyncDatapathIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for async datapath operations.

    These tests mirror the sync datapath tests but use async operations.
    """

    catalog = None
    async_catalog = None
    wrapper = None

    @classmethod
    def setUpClass(cls):
        """Create and populate a test catalog."""
        from deriva.core import DerivaServer, get_credential, ermrest_model as _em

        credential = os.getenv("DERIVA_PY_TEST_CREDENTIAL") or get_credential(hostname)
        server = DerivaServer("https", hostname, credentials=credential)
        cls.sync_catalog = server.create_ermrest_catalog()
        cls.catalog_id = cls.sync_catalog.catalog_id
        cls.credential = credential

        try:
            # Create test schema (simplified from test_datapath.py)
            model = cls.sync_catalog.getCatalogModel()

            # Vocab schema with experiment types
            vocab = model.create_schema(_em.Schema.define(SNAME_VOCAB))
            vocab.create_table(_em.Table.define_vocabulary(TNAME_EXPERIMENT_TYPE, "TEST:{RID}"))

            # ISA schema with experiments
            isa = model.create_schema(_em.Schema.define(SNAME_ISA))

            # Project table
            isa.create_table(_em.Table.define(
                TNAME_PROJECT,
                column_defs=[
                    _em.Column.define("Investigator", _em.builtin_types.text),
                    _em.Column.define("Num", _em.builtin_types.int4),
                ],
                key_defs=[_em.Key.define(["Investigator", "Num"])]
            ))

            # Experiment table
            isa.create_table(_em.Table.define(
                TNAME_EXPERIMENT,
                column_defs=[
                    _em.Column.define("Name", _em.builtin_types.text),
                    _em.Column.define("Amount", _em.builtin_types.int4),
                    _em.Column.define("Type", _em.builtin_types.text),
                    _em.Column.define("Project_Investigator", _em.builtin_types.text),
                    _em.Column.define("Project_Num", _em.builtin_types.int4),
                ],
                key_defs=[_em.Key.define(["Name"])],
                fkey_defs=[
                    _em.ForeignKey.define(["Type"], SNAME_VOCAB, TNAME_EXPERIMENT_TYPE, ["ID"]),
                    _em.ForeignKey.define(
                        ["Project_Investigator", "Project_Num"],
                        SNAME_ISA, TNAME_PROJECT,
                        ["Investigator", "Num"]
                    )
                ]
            ))

            # Populate test data
            paths = cls.sync_catalog.getPathBuilder()

            # Insert project
            paths.schemas[SNAME_ISA].tables[TNAME_PROJECT].insert([
                {"Investigator": "Smith", "Num": 1}
            ])

            # Insert experiment types
            type_table = paths.schemas[SNAME_VOCAB].tables[TNAME_EXPERIMENT_TYPE]
            types = type_table.insert([
                {"Name": str(i), "Description": "NA"} for i in range(TEST_EXPTYPE_MAX)
            ], defaults=["ID", "URI"])

            # Insert experiments
            exp = paths.schemas[SNAME_ISA].tables[TNAME_EXPERIMENT]
            exp.insert([
                {
                    "Name": f"experiment-{i}",
                    "Amount": i,
                    "Type": types[i % TEST_EXPTYPE_MAX]["ID"],
                    "Project_Investigator": "Smith",
                    "Project_Num": 1,
                }
                for i in range(TEST_EXP_MAX)
            ])

        except Exception:
            cls.sync_catalog.delete_ermrest_catalog(really=True)
            raise

    @classmethod
    def tearDownClass(cls):
        """Delete the test catalog."""
        if cls.sync_catalog:
            cls.sync_catalog.delete_ermrest_catalog(really=True)

    async def asyncSetUp(self):
        from deriva.core.asyncio import AsyncErmrestCatalog, AsyncCatalogWrapper

        self.async_catalog = AsyncErmrestCatalog(
            "https", hostname, self.catalog_id, self.credential
        )
        self.wrapper = AsyncCatalogWrapper(self.sync_catalog, self.async_catalog)

    async def asyncTearDown(self):
        await self.async_catalog.close()

    async def test_fetch_all_entities(self):
        """Should fetch all entities from a table."""
        path = self.wrapper.path.ISA.Experiment
        result_set = self.wrapper.async_result_set(path)
        results = await result_set.fetch_async()

        self.assertEqual(len(results), TEST_EXP_MAX)

    async def test_fetch_with_limit(self):
        """Should respect limit parameter."""
        path = self.wrapper.path.ISA.Experiment
        result_set = self.wrapper.async_result_set(path)
        results = await result_set.fetch_async(limit=10)

        self.assertEqual(len(results), 10)

    async def test_fetch_paged_all(self):
        """Should page through all results."""
        path = self.wrapper.path.ISA.Experiment
        result_set = self.wrapper.async_result_set(path)

        all_rows = []
        async for page in result_set.fetch_paged_async(page_size=25):
            all_rows.extend(page)

        self.assertEqual(len(all_rows), TEST_EXP_MAX)

    async def test_fetch_paged_preserves_order(self):
        """Paged results should maintain RID order."""
        path = self.wrapper.path.ISA.Experiment
        result_set = self.wrapper.async_result_set(path)

        all_rids = []
        async for page in result_set.fetch_paged_async(page_size=25):
            all_rids.extend(row["RID"] for row in page)

        # Verify RIDs are unique
        self.assertEqual(len(all_rids), len(set(all_rids)))

        # Verify sorted order
        self.assertEqual(all_rids, sorted(all_rids))

    async def test_wrapper_convenience_method(self):
        """fetch_entities_async should work."""
        path = self.wrapper.path.ISA.Experiment
        results = await self.wrapper.fetch_entities_async(path, limit=5)

        self.assertEqual(len(results), 5)

    async def test_filtered_path(self):
        """Should work with filtered paths."""
        exp = self.wrapper.path.ISA.Experiment
        filtered = exp.filter(exp.Amount < 10)

        result_set = self.wrapper.async_result_set(filtered)
        results = await result_set.fetch_async()

        self.assertEqual(len(results), 10)
        for row in results:
            self.assertLess(row["Amount"], 10)


if __name__ == "__main__":
    unittest.main()
