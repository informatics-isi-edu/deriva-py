"""Unit tests for MMO+DDL Drop operations.
"""
import os
import logging
from deriva.core import mmo
from deriva.core.ermrest_model import tag

from .base import BaseMMOTestCase

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

    def _pre(self, fn):
        """Pre-condition evaluation."""
        fn(self.assertTrue)

    def _post(self, fn):
        """Post-condition evaluation"""
        fn(self.assertFalse)

    def test_drop_key(self):
        kname = ["test", "dept_dept_no_key"]

        def cond(assertion):
            matches = mmo.find(self.model, kname)
            assertion(len(matches))

        self._pre(cond)
        t = self.model.schemas[kname[0]].tables["dept"]
        key = t.keys[(t.schema, kname[1])]
        key.drop(cascade=True, update_mappings=True)
        self._post(cond)

    def test_drop_key_cascade(self):
        kname = ["test", "dept_dept_no_key"]
        fkname = ["test", "person_dept_fkey"]

        def condf(name):
            def cond(assertion):
                matches = mmo.find(self.model, name)
                assertion(len(matches))
            return cond

        self._pre(condf(kname))
        self._pre(condf(fkname))
        t = self.model.schemas[kname[0]].tables["dept"]
        key = t.keys[(t.schema, kname[1])]
        key.drop(cascade=True, update_mappings=True)
        self._post(condf(kname))
        self._post(condf(fkname))

    def test_drop_fkey(self):
        fkname = ["test", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(len(matches))

        self._pre(cond)
        self.model.fkey(fkname).drop(update_mappings=True)
        self._post(cond)

    def test_drop_col(self):
        cname = ["test", "person", "last_name"]

        def cond(assertion):
            matches = mmo.find(self.model, cname)
            assertion(len(matches))

        self._pre(cond)
        t = self.model.schemas[cname[0]].tables[cname[1]]
        col = t.columns[cname[2]]
        col.drop(update_mappings=True)
        self._post(cond)

    def test_drop_col_cascade_key(self):
        cname = ["test", "dept", "dept_no"]
        kname = ["test", "dept_dept_no_key"]
        fkname = ["test", "person_dept_fkey"]

        def condf(name):
            def cond(assertion):
                matches = mmo.find(self.model, name)
                assertion(len(matches))
            return cond

        self._pre(condf(cname))
        self._pre(condf(kname))
        self._pre(condf(fkname))
        t = self.model.schemas[cname[0]].tables[cname[1]]
        col = t.columns[cname[2]]
        col.drop(cascade=True, update_mappings=True)
        self._post(condf(cname))
        self._post(condf(kname))
        self._post(condf(fkname))

    def test_drop_col_cascade_fkey(self):
        cname = ["test", "person", "dept"]
        fkname = ["test", "person_dept_fkey"]

        def condf(name):
            def cond(assertion):
                matches = mmo.find(self.model, name)
                assertion(len(matches))
            return cond

        self._pre(condf(cname))
        self._pre(condf(fkname))
        t = self.model.schemas[cname[0]].tables[cname[1]]
        col = t.columns[cname[2]]
        col.drop(cascade=True, update_mappings=True)
        self._post(condf(cname))
        self._post(condf(fkname))
