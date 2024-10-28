"""Unit tests for MMO+DDL Alter Rename operations.
"""
import logging
import os

from deriva.core import mmo
from deriva.core.ermrest_model import UpdateMappings, Schema
from tests.deriva.core.mmo.base import BaseMMOTestCase

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('DERIVA_PY_TEST_LOGLEVEL', default=logging.WARNING))


class TestMMOxDDLRename (BaseMMOTestCase):

    @classmethod
    def setUpClass(cls):
        """Don't bother setting up catalog for the suite.
        """
        pass

    def setUp(self):
        """Setup catalog for each unit test.
        """
        TestMMOxDDLRename.setUpCatalog()
        super().setUp()

    def _pre(self, fn):
        """Pre-condition evaluation."""
        fn(self.assertTrue, self.assertFalse)

    def _post(self, fn):
        """Post-condition evaluation"""
        fn(self.assertFalse, self.assertTrue)

    def test_rename_col(self):
        oldname = "postal_code"
        newname = "ZIP"

        def cond(before, after):
            before(len(mmo.find(self.model, ["dept_schema", "dept", oldname])))
            after(len(mmo.find(self.model, ["dept_schema", "dept", newname])))

        self._pre(cond)
        self.model.schemas["dept_schema"].tables["dept"].columns[oldname].alter(name=newname, update_mappings=UpdateMappings.immediate)
        self._post(cond)

    def test_rename_col_deferred(self):
        oldname = "postal_code"
        newname = "ZIP"

        def condf(model):
            def cond(before, after):
                before(len(mmo.find(model, ["dept_schema", "dept", oldname])))
                after(len(mmo.find(model, ["dept_schema", "dept", newname])))
            return cond

        self._pre(condf(self.model))
        self.model.schemas["dept_schema"].tables["dept"].columns[oldname].alter(name=newname, update_mappings=UpdateMappings.deferred)
        self._pre(condf(self.model.catalog.getCatalogModel()))
        self.model.apply()
        self._post(condf(self.model.catalog.getCatalogModel()))

    def test_rename_key(self):
        oldname = "dept_dept_no_key"
        newname = "dept_DEPT_NUM_key"

        def cond(before, after):
            before(len(mmo.find(self.model, ["dept_schema", oldname])))
            after(len(mmo.find(self.model, ["dept_schema", newname])))

        self._pre(cond)
        t = self.model.schemas["dept_schema"].tables["dept"]
        k = t.keys[(t.schema, oldname)]
        k.alter(constraint_name=newname, update_mappings=UpdateMappings.immediate)
        self._post(cond)

    def test_rename_key_deferred(self):
        oldname = "dept_dept_no_key"
        newname = "dept_DEPT_NUM_key"

        def condf(model):
            def cond(before, after):
                before(len(mmo.find(model, ["dept_schema", oldname])))
                after(len(mmo.find(model, ["dept_schema", newname])))
            return cond

        self._pre(condf(self.model))
        t = self.model.schemas["dept_schema"].tables["dept"]
        k = t.keys[(t.schema, oldname)]
        k.alter(constraint_name=newname, update_mappings=UpdateMappings.deferred)
        self._pre(condf(self.model.catalog.getCatalogModel()))
        self.model.apply()
        self._post(condf(self.model.catalog.getCatalogModel()))

    def test_rename_fkey(self):
        oldname = "person_dept_fkey"
        newname = "person_department_FKey"

        def cond(before, after):
            before(len(mmo.find(self.model, ["person_schema", oldname])))
            after(len(mmo.find(self.model, ["person_schema", newname])))

        self._pre(cond)
        t = self.model.schemas["person_schema"].tables["person"]
        fk = t.foreign_keys[(t.schema, oldname)]
        fk.alter(constraint_name=newname, update_mappings=UpdateMappings.immediate)
        self._post(cond)

    def test_rename_fkey_deferred(self):
        oldname = "person_dept_fkey"
        newname = "person_department_FKey"

        def condf(model):
            def cond(before, after):
                before(len(mmo.find(model, ["person_schema", oldname])))
                after(len(mmo.find(model, ["person_schema", newname])))
            return cond

        self._pre(condf(self.model))
        t = self.model.schemas["person_schema"].tables["person"]
        fk = t.foreign_keys[(t.schema, oldname)]
        fk.alter(constraint_name=newname, update_mappings=UpdateMappings.deferred)
        self._pre(condf(self.model.catalog.getCatalogModel()))
        self.model.apply()
        self._post(condf(self.model.catalog.getCatalogModel()))

    def _do_test_rename_key_on_table_move(self, deferred=False):
        oldname = ["dept_schema", "dept_dept_no_key"]
        newname = ["new_schema", "dept_dept_no_key"]

        def condf(model):
            def cond(before, after):
                before(len(mmo.find(model, oldname)))
                after(len(mmo.find(model, newname)))
            return cond

        self.model.create_schema(
            Schema.define(newname[0])
        )

        self._pre(condf(self.model))
        t = self.model.schemas[oldname[0]].tables["dept"]
        t.alter(schema_name=newname[0],
                update_mappings=UpdateMappings.deferred if deferred else UpdateMappings.immediate)
        if deferred:
            self._pre(condf(self.model.catalog.getCatalogModel()))
            self.model.apply()
            self._post(condf(self.model.catalog.getCatalogModel()))
        else:
            self._post(condf(self.model))

    def test_rename_key_on_table_move(self):
        self._do_test_rename_key_on_table_move()

    def test_rename_key_on_table_move_deferred(self):
        self._do_test_rename_key_on_table_move(deferred=True)

    def _do_test_rename_fkey_on_table_move(self, deferred=False):
        oldname = ["person_schema", "person_dept_fkey"]
        newname = ["new_schema", "person_dept_fkey"]

        def condf(model):
            def cond(before, after):
                before(len(mmo.find(model, oldname)))
                after(len(mmo.find(model, newname)))
            return cond

        self.model.create_schema(
            Schema.define(newname[0])
        )

        self._pre(condf(self.model))
        t = self.model.schemas[oldname[0]].tables["person"]
        t.alter(schema_name=newname[0],
                update_mappings=UpdateMappings.deferred if deferred else UpdateMappings.immediate)
        if deferred:
            self._pre(condf(self.model.catalog.getCatalogModel()))
            self.model.apply()
            self._post(condf(self.model.catalog.getCatalogModel()))
        else:
            self._post(condf(self.model))

    def test_rename_fkey_on_table_move(self):
        self._do_test_rename_key_on_table_move()

    def test_rename_fkey_on_table_move_deferred(self):
        self._do_test_rename_key_on_table_move(deferred=True)
