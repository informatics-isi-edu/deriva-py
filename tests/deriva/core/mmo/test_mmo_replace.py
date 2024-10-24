"""Unit tests for MMO replace operation.
"""
import logging
import os

from deriva.core import mmo
from deriva.core.ermrest_model import tag
from tests.deriva.core.mmo.base import BaseMMOTestCase

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('DERIVA_PY_TEST_LOGLEVEL', default=logging.WARNING))


class TestMMOReplace (BaseMMOTestCase):

    def _pre(self, fn):
        """Pre-condition evaluation."""
        fn(self.assertTrue, self.assertFalse)

    def _post(self, fn):
        """Post-condition evaluation"""
        fn(self.assertFalse, self.assertTrue)

    def test_replace_col_in_vizcols(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["dept_schema", "dept", "postal_code"])) == 1)
            after(len(mmo.find(self.model, ["dept_schema", "dept", "zip"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["dept_schema", "dept", "postal_code"], ["dept_schema", "dept", "zip"])
        self._post(cond)

    def test_replace_col_in_vizcols_pseudocol_simple(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["dept_schema", "dept", "street_address"])) == 1)
            after(len(mmo.find(self.model, ["dept_schema", "dept", "number_and_street_name"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["dept_schema", "dept", "street_address"], ["dept_schema", "dept", "number_and_street_name"])
        self._post(cond)

    def test_replace_col_in_sourcedefs_columns(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["dept_schema", "dept", "country"])) == 1)
            after(len(mmo.find(self.model, ["dept_schema", "dept", "country_code"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["dept_schema", "dept", "country"], ["dept_schema", "dept", "country_code"])
        self._post(cond)

    def test_replace_col_in_vizcols_pseudocol(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["dept_schema", "dept", "state"])) == 1)
            after(len(mmo.find(self.model, ["dept_schema", "dept", "state_or_province"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["dept_schema", "dept", "state"], ["dept_schema", "dept", "state_or_province"])
        self._post(cond)

    def test_replace_col_in_sourcedefs_sources(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["dept_schema", "dept", "city"])) == 1)
            after(len(mmo.find(self.model, ["dept_schema", "dept", "township"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["dept_schema", "dept", "city"], ["dept_schema", "dept", "township"])
        self._post(cond)

    def test_replace_key_in_vizcols(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["dept_schema", "dept_RID_key"])) == 1)
            after(len(mmo.find(self.model, ["dept_schema", "dept_RID_key1"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["dept_schema", "dept_RID_key"], ["dept_schema", "dept_RID_key1"])
        self._post(cond)

    def _do_test_replace_fkey_in_vizsrc(self, tagname):
        oldfk = ["person_schema", "person_dept_fkey"]
        newfk = ["person_schema", "person_dept_fkey1"]

        def cond(before, after):
            before(any([m.tag == tagname and m.mapping == oldfk for m in mmo.find(self.model, oldfk)]))
            after(any([m.tag == tagname and m.mapping == newfk for m in mmo.find(self.model, newfk)]))

        self._pre(cond)
        mmo.replace(self.model, oldfk, newfk)
        self._post(cond)

    def test_replace_fkey_in_vizfkeys(self):
        self._do_test_replace_fkey_in_vizsrc(tag.visible_foreign_keys)

    def test_replace_fkey_in_vizcols(self):
        self._do_test_replace_fkey_in_vizsrc(tag.visible_columns)

    def test_replace_fkey_in_sourcedefs_fkeys(self):
        self._do_test_replace_fkey_in_vizsrc(tag.source_definitions)

    def test_replace_fkey_in_pseudocolumn(self):
        oldfk = ["person_schema", "person_dept_fkey"]
        newfk = ["person_schema", "person_dept_fkey1"]

        def cond(before, after):
            before(any([m.tag == tag.visible_columns and isinstance(m.mapping, dict) for m in mmo.find(self.model, oldfk)]))
            after(any([m.tag == tag.visible_columns and isinstance(m.mapping, dict) for m in mmo.find(self.model, newfk)]))

        self._pre(cond)
        mmo.replace(self.model, oldfk, newfk)
        self._post(cond)

    def test_replace_fkey_in_sourcedefs_sources(self):
        oldfk = ["person_schema", "person_dept_fkey"]
        newfk = ["person_schema", "person_dept_fkey1"]

        def cond(before, after):
            before(any([m.tag == tag.source_definitions and m.mapping == 'personnel' for m in mmo.find(self.model, oldfk)]))
            after(any([m.tag == tag.source_definitions and m.mapping == 'personnel' for m in mmo.find(self.model, newfk)]))

        self._pre(cond)
        mmo.replace(self.model, oldfk, newfk)
        self._post(cond)

    def test_replace_col_in_search_box(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["person_schema", "person", "last_name"])) == 1)
            after(len(mmo.find(self.model, ["person_schema", "person", "surname"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["person_schema", "person", "last_name"], ["person_schema", "person", "surname"])
        self._post(cond)
