"""Unit tests for MMO+DDL Alter Rename operations.
"""
import logging
import os

from deriva.core import mmo
from .base import BaseMMOTestCase

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
        self.model.schemas["dept_schema"].tables["dept"].columns[oldname].alter(name=newname, update_mappings=True)
        self._post(cond)

    def test_rename_key(self):
        oldname = "dept_dept_no_key"
        newname = "dept_DEPT_NUM_key"

        def cond(before, after):
            before(len(mmo.find(self.model, ["dept_schema", oldname])))
            after(len(mmo.find(self.model, ["dept_schema", newname])))

        self._pre(cond)
        t = self.model.schemas["dept_schema"].tables["dept"]
        k = t.keys[(t.schema, oldname)]
        k.alter(constraint_name=newname, update_mappings=True)
        self._post(cond)

    def test_rename_fkey(self):
        oldname = "person_dept_fkey"
        newname = "person_department_FKey"

        def cond(before, after):
            before(len(mmo.find(self.model, ["person_schema", oldname])))
            after(len(mmo.find(self.model, ["person_schema", newname])))

        self._pre(cond)
        t = self.model.schemas["person_schema"].tables["person"]
        fk = t.foreign_keys[(t.schema, oldname)]
        fk.alter(constraint_name=newname, update_mappings=True)
        self._post(cond)
