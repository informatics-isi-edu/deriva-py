"""Unit tests for MMO+DDL Drop operations.
"""
import os
import logging
from deriva.core import mmo
from deriva.core.ermrest_model import UpdateMappings
from tests.deriva.core.mmo.base import BaseMMOTestCase

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('DERIVA_PY_TEST_LOGLEVEL', default=logging.WARNING))


class TestMMOxDDLDrop (BaseMMOTestCase):

    @classmethod
    def setUpClass(cls):
        """Don't bother setting up catalog for the suite.
        """
        pass

    def setUp(self):
        """Setup catalog for each unit test.
        """
        TestMMOxDDLDrop.setUpCatalog()
        super().setUp()

    def test_drop_key(self):
        kname = ["dept_schema", "dept_dept_no_key"]

        def test():
            matches = mmo.find(self.model, kname)
            return len(matches)

        self.assertTrue(test())
        t = self.model.schemas[kname[0]].tables["dept"]
        key = t.keys[(t.schema, kname[1])]
        key.drop(cascade=True, update_mappings=UpdateMappings.immediate)
        self.assertFalse(test())

    def test_drop_key_deferred(self):
        kname = ["dept_schema", "dept_dept_no_key"]

        def test(model):
            matches = mmo.find(model, kname)
            return len(matches)

        self.assertTrue(test(self.model))
        t = self.model.schemas[kname[0]].tables["dept"]
        key = t.keys[(t.schema, kname[1])]
        key.drop(cascade=True, update_mappings=UpdateMappings.deferred)
        self.assertTrue(test(self.model.catalog.getCatalogModel()))
        self.model.apply()
        self.assertFalse(test(self.model.catalog.getCatalogModel()))

    def test_drop_key_cascade(self):
        kname = ["dept_schema", "dept_dept_no_key"]
        fkname = ["person_schema", "person_dept_fkey"]

        def test(name):
            matches = mmo.find(self.model, name)
            return len(matches)

        self.assertTrue(test(kname))
        self.assertTrue(test(fkname))
        t = self.model.schemas[kname[0]].tables["dept"]
        key = t.keys[(t.schema, kname[1])]
        key.drop(cascade=True, update_mappings=UpdateMappings.immediate)
        self.assertFalse(test(kname))
        self.assertFalse(test(fkname))

    def test_drop_fkey(self):
        fkname = ["person_schema", "person_dept_fkey"]

        def test():
            matches = mmo.find(self.model, fkname)
            return len(matches)

        self.assertTrue(test())
        self.model.fkey(fkname).drop(update_mappings=UpdateMappings.immediate)
        self.assertFalse(test())

    def test_drop_fkey_deferred(self):
        fkname = ["person_schema", "person_dept_fkey"]

        def test(model):
            matches = mmo.find(model, fkname)
            return len(matches)

        self.assertTrue(test(self.model))
        self.model.fkey(fkname).drop(update_mappings=UpdateMappings.deferred)
        self.assertTrue(test(self.model.catalog.getCatalogModel()))
        self.model.apply()
        self.assertFalse(test(self.model.catalog.getCatalogModel()))

    def test_drop_col(self):
        cname = ["person_schema", "person", "last_name"]

        def test():
            matches = mmo.find(self.model, cname)
            return len(matches)

        self.assertTrue(test())
        t = self.model.schemas[cname[0]].tables[cname[1]]
        col = t.columns[cname[2]]
        col.drop(update_mappings=UpdateMappings.immediate)
        self.assertFalse(test())

    def test_drop_col_deferred(self):
        cname = ["person_schema", "person", "last_name"]

        def test(model):
            matches = mmo.find(model, cname)
            return len(matches)

        self.assertTrue(test(self.model))
        t = self.model.schemas[cname[0]].tables[cname[1]]
        col = t.columns[cname[2]]
        col.drop(update_mappings=UpdateMappings.deferred)
        self.assertTrue(test(self.model.catalog.getCatalogModel()))
        self.model.apply()
        self.assertFalse(test(self.model.catalog.getCatalogModel()))

    def test_drop_col_cascade_key(self):
        cname = ["dept_schema", "dept", "dept_no"]
        kname = ["dept_schema", "dept_dept_no_key"]
        fkname = ["person_schema", "person_dept_fkey"]

        def test(name):
            matches = mmo.find(self.model, name)
            return len(matches)

        for name in [cname, kname, fkname]:
            with self.subTest(f'subTest {name}'):
                self.assertTrue(test(name))

        t = self.model.schemas[cname[0]].tables[cname[1]]
        col = t.columns[cname[2]]
        col.drop(cascade=True, update_mappings=UpdateMappings.immediate)
        
        for name in [cname, kname, fkname]:
            with self.subTest(f'subTest {name}'):
                self.assertFalse(test(name))

    def test_drop_col_cascade_fkey(self):
        cname = ["person_schema", "person", "dept"]
        fkname = ["person_schema", "person_dept_fkey"]

        def test(name):
            matches = mmo.find(self.model, name)
            return len(matches)

        self.assertTrue(test(cname))
        self.assertTrue(test(fkname))
        t = self.model.schemas[cname[0]].tables[cname[1]]
        col = t.columns[cname[2]]
        col.drop(cascade=True, update_mappings=UpdateMappings.immediate)
        self.assertFalse(test(cname))
        self.assertFalse(test(fkname))

    def test_drop_table_cascade(self):
        cname = ["dept_schema", "dept", "dept_no"]
        kname = ["dept_schema", "dept_dept_no_key"]
        fkname = ["person_schema", "person_dept_fkey"]

        def test(name):
            matches = mmo.find(self.model, name)
            return len(matches)

        for name in [cname, kname, fkname]:
            with self.subTest(f'subTest {name}'):
                self.assertTrue(test(name))

        t = self.model.schemas[cname[0]].tables[cname[1]]
        t.drop(cascade=True, update_mappings=UpdateMappings.immediate)

        for name in [cname, kname, fkname]:
            with self.subTest(f'subTest {name}'):
                self.assertFalse(test(name))

    def test_drop_table_cascade_deferred(self):
        cname = ["person_schema", "person", "name"]
        kname = ["person_schema", "person_RID_key"]
        fkname = ["person_schema", "person_dept_fkey"]

        def test(model, name):
            matches = mmo.find(model, name)
            return len(matches)

        for name in [cname, kname, fkname]:
            with self.subTest(f'subtest {name} precondition'):
                self.assertTrue(test(self.model, name))

        t = self.model.schemas[cname[0]].tables[cname[1]]
        t.drop(cascade=True, update_mappings=UpdateMappings.deferred)

        # fkname should linger on in the dept table's annotations until the update is applied to the model
        self.assertTrue(test(self.model.catalog.getCatalogModel(), fkname))
        self.model.apply()
        self.assertFalse(test(self.model.catalog.getCatalogModel(), fkname))

    def test_drop_table(self):
        fkname = ["person_schema", "person_dept_fkey"]

        def test(name):
            matches = mmo.find(self.model, name)
            return len(matches)

        self.assertTrue(test(fkname))
        t = self.model.schemas["person_schema"].tables["person"]
        t.drop(update_mappings=UpdateMappings.immediate)
        self.assertFalse(test(fkname))

    def test_drop_schema_cascade(self):
        cname = ["dept_schema", "dept", "dept_no"]
        kname = ["dept_schema", "dept_dept_no_key"]
        fkname = ["person_schema", "person_dept_fkey"]

        def test(name):
            matches = mmo.find(self.model, name)
            return len(matches)

        for name in [cname, kname, fkname]:
            with self.subTest(f'subtest {name} precondition'):
                self.assertTrue(test(name))

        s = self.model.schemas[cname[0]]
        s.drop(cascade=True, update_mappings=UpdateMappings.immediate)

        for name in [cname, kname, fkname]:
            with self.subTest(f'subtest {name} postcondition'):
                self.assertFalse(test(name))

    def test_drop_schema_cascade_deferred(self):
        cname = ["person_schema", "person", "name"]
        kname = ["person_schema", "person_RID_key"]
        fkname = ["person_schema", "person_dept_fkey"]

        def test(model, name):
            matches = mmo.find(model, name)
            return len(matches)

        for name in [cname, kname, fkname]:
            with self.subTest(f'subtest {name} precondition'):
                self.assertTrue(test(self.model, name))

        s = self.model.schemas[cname[0]]
        s.drop(cascade=True, update_mappings=UpdateMappings.deferred)

        # fkname should linger on in the dept table's annotations until the update is applied to the model
        self.assertTrue(test(self.model.catalog.getCatalogModel(), fkname))
        self.model.apply()
        self.assertFalse(test(self.model.catalog.getCatalogModel(), fkname))
