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

    def _do_test_replace_key_in_vizcols(self, oldfk, newfk):
        replacement = newfk
        newfk = [replacement[0], replacement[1] if replacement[1] else oldfk[1]]

        def cond(before, after):
            before(len(mmo.find(self.model, oldfk)) == 1)
            after(len(mmo.find(self.model, newfk)) == 1)

        self._pre(cond)
        mmo.replace(self.model, oldfk, replacement)
        self._post(cond)

    def test_replace_key_in_vizcols(self):
        self._do_test_replace_key_in_vizcols(["dept_schema", "dept_RID_key"], ["dept_schema", "dept_RID_key1"])

    def test_replace_key_in_vizcols_by_schema_only(self):
        self._do_test_replace_key_in_vizcols(["dept_schema", "dept_RID_key"], ["new_schema", None])

    def _do_test_replace_fkey_in_vizsrc(self, tagname, oldfk, newfk):
        replacement = newfk
        newfk = [replacement[0], replacement[1] if replacement[1] else oldfk[1]]

        def cond(before, after):
            before(any([m.tag == tagname and m.mapping == oldfk for m in mmo.find(self.model, oldfk)]))
            after(any([m.tag == tagname and m.mapping == newfk for m in mmo.find(self.model, newfk)]))

        self._pre(cond)
        mmo.replace(self.model, oldfk, replacement)
        self._post(cond)

    def test_replace_fkey_in_vizfkeys(self):
        self._do_test_replace_fkey_in_vizsrc(tag.visible_foreign_keys, ["person_schema", "person_dept_fkey"], ["person_schema", "person_dept_fkey1"])

    def test_replace_fkey_in_vizcols(self):
        self._do_test_replace_fkey_in_vizsrc(tag.visible_columns, ["person_schema", "person_dept_fkey"], ["person_schema", "person_dept_fkey1"])

    def test_replace_fkey_in_sourcedefs_fkeys(self):
        self._do_test_replace_fkey_in_vizsrc(tag.source_definitions, ["person_schema", "person_dept_fkey"], ["person_schema", "person_dept_fkey1"])

    def test_replace_fkey_in_vizfkeys_by_schema_only(self):
        self._do_test_replace_fkey_in_vizsrc(tag.visible_foreign_keys, ["person_schema", "person_dept_fkey"], ["new_schema", None])

    def test_replace_fkey_in_vizcols_by_schema_only(self):
        self._do_test_replace_fkey_in_vizsrc(tag.visible_columns, ["person_schema", "person_dept_fkey"], ["new_schema", None])

    def test_replace_fkey_in_sourcedefs_fkeys_by_schema_only(self):
        self._do_test_replace_fkey_in_vizsrc(tag.source_definitions, ["person_schema", "person_dept_fkey"], ["new_schema", None])

    def _do_test_replace_fkey_in_pseudocolumn(self, oldfk, newfk):
        replacement = newfk
        newfk = [replacement[0], replacement[1] if replacement[1] else oldfk[1]]

        def cond(before, after):
            before(any([m.tag == tag.visible_columns and isinstance(m.mapping, dict) for m in mmo.find(self.model, oldfk)]))
            after(any([m.tag == tag.visible_columns and isinstance(m.mapping, dict) for m in mmo.find(self.model, newfk)]))

        self._pre(cond)
        mmo.replace(self.model, oldfk, replacement)
        self._post(cond)

    def test_replace_fkey_in_pseudocolumn(self):
        self._do_test_replace_fkey_in_pseudocolumn(["person_schema", "person_dept_fkey"], ["person_schema", "person_dept_fkey1"])

    def test_replace_fkey_in_pseudocolumn_by_schema_only(self):
        self._do_test_replace_fkey_in_pseudocolumn(["person_schema", "person_dept_fkey"], ["new_schema", None])

    def _do_test_replace_fkey_in_sourcedefs_sources(self, oldfk, newfk):
        replacement = newfk
        newfk = [replacement[0], replacement[1] if replacement[1] else oldfk[1]]

        def cond(before, after):
            before(any([m.tag == tag.source_definitions and m.mapping == 'personnel' for m in mmo.find(self.model, oldfk)]))
            after(any([m.tag == tag.source_definitions and m.mapping == 'personnel' for m in mmo.find(self.model, newfk)]))

        self._pre(cond)
        mmo.replace(self.model, oldfk, replacement)
        self._post(cond)

    def test_replace_fkey_in_sourcedefs_sources(self):
        self._do_test_replace_fkey_in_sourcedefs_sources(["person_schema", "person_dept_fkey"], ["person_schema", "person_dept_fkey1"])

    def test_replace_fkey_in_sourcedefs_sources_by_schema_only(self):
        self._do_test_replace_fkey_in_sourcedefs_sources(["person_schema", "person_dept_fkey"], ["new_schema", None])

    def test_replace_col_in_search_box(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["person_schema", "person", "last_name"])) == 1)
            after(len(mmo.find(self.model, ["person_schema", "person", "surname"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["person_schema", "person", "last_name"], ["person_schema", "person", "surname"])
        self._post(cond)

    def test_replace_all_constraints_by_schema_only(self):
        oldfk = ["person_schema", "person_dept_fkey"]
        newfk = ["new_schema", "person_dept_fkey"]
        oldk = ["person_schema", "person_RID_key"]
        newk = ["new_schema", "person_RID_key"]

        def cond(before, after):
            for tag_name, mapping_before, mapping_after, old_constraint, new_constraint in [
                (tag.visible_columns, oldk, newk, oldk, newk),
                (tag.visible_foreign_keys, oldfk, newfk, oldfk, newfk),
                (tag.source_definitions, 'personnel', 'personnel', oldfk, newfk)
            ]:
                before(any([m.tag == tag_name and m.mapping == mapping_before for m in mmo.find(self.model, old_constraint)]))
                after(any([m.tag == tag_name and m.mapping == mapping_after for m in mmo.find(self.model, new_constraint)]))

        self._pre(cond)
        mmo.replace(self.model, ["person_schema", None], ["new_schema", None])
        self._post(cond)

