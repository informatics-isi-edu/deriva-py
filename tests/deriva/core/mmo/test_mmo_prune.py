"""Unit tests for MMO prune operation.
"""
import logging
import os

from deriva.core import mmo
from deriva.core.ermrest_model import tag
from tests.deriva.core.mmo.base import BaseMMOTestCase

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('DERIVA_PY_TEST_LOGLEVEL', default=logging.WARNING))


class TestMMOPrune (BaseMMOTestCase):

    def _pre(self, fn):
        """Pre-condition evaluation."""
        fn(self.assertTrue)

    def _post(self, fn):
        """Post-condition evaluation"""
        fn(self.assertFalse)

    def test_prune_key_in_vizcols(self):
        fkname = ["dept_schema", "dept_RID_key"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(len(matches) == 1)

        self._pre(cond)
        mmo.prune(self.model, fkname)
        self._post(cond)

    def test_prune_key_in_vizcols_by_schema_only(self):
        fkname = ["dept_schema", "dept_RID_key"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(len(matches) == 1)

        self._pre(cond)
        mmo.prune(self.model, [fkname[0], None])
        self._post(cond)

    def test_prune_col_in_vizcols(self):
        def cond(assertion):
            matches = mmo.find(self.model, ["dept_schema", "dept", "RCT"])
            assertion(len(matches) == 1)

        self._pre(cond)
        mmo.prune(self.model, ["dept_schema", "dept", "RCT"])
        self._post(cond)

    def test_prune_col_in_vizcols_pseudocol_simple(self):
        def cond(assertion):
            matches = mmo.find(self.model, ["dept_schema", "dept", "RMT"])
            assertion(len(matches) == 1)

        self._pre(cond)
        mmo.prune(self.model, ["dept_schema", "dept", "RMT"])
        self._post(cond)

    def test_prune_col_in_vizcols_pseudocol(self):
        def cond(assertion):
            matches = mmo.find(self.model, ["dept_schema", "dept", "name"])
            assertion(any([m.anchor.name == 'person' and m.tag == tag.visible_columns and isinstance(m.mapping, dict) for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, ["dept_schema", "dept", "name"])
        self._post(cond)

    def test_prune_col_in_sourcedefs_columns(self):
        def cond(assertion):
            matches = mmo.find(self.model, ["person_schema", "person", "dept"])
            assertion(any([m.anchor.name == 'person' and m.tag == tag.source_definitions and m.mapping == 'dept' for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, ["person_schema", "person", "dept"])
        self._post(cond)

    def test_prune_col_in_sourcedefs_sources(self):
        def cond(assertion):
            matches = mmo.find(self.model, ["person_schema", "person", "RID"])
            assertion(any([m.tag == tag.source_definitions and m.mapping == 'dept_size' for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, ["person_schema", "person", "RID"])
        self._post(cond)

    def test_prune_fkey_in_vizfkeys(self):
        fkname = ["person_schema", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.visible_foreign_keys and m.mapping == fkname for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, fkname)
        self._post(cond)

    def test_prune_fkey_in_vizfkeys_by_schema_only(self):
        fkname = ["person_schema", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.visible_foreign_keys and m.mapping == fkname for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, [fkname[0], None])
        self._post(cond)

    def test_prune_fkey_in_vizcols(self):
        fkname = ["person_schema", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.visible_columns and m.mapping == fkname for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, fkname)
        self._post(cond)

    def test_prune_fkey_in_vizcols_by_schema_only(self):
        fkname = ["person_schema", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.visible_columns and m.mapping == fkname for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, [fkname[0], None])
        self._post(cond)

    def test_prune_fkey_in_sourcedefs_sources(self):
        fkname = ["person_schema", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.source_definitions and m.mapping == 'personnel' for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, fkname)
        self._post(cond)

    def test_prune_fkey_in_sourcedefs_sources_by_schema_only(self):
        fkname = ["person_schema", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.source_definitions and m.mapping == 'personnel' for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, [fkname[0], None])
        self._post(cond)

    def test_prune_fkey_in_sourcedefs_fkeys(self):
        fkname = ["person_schema", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.source_definitions and m.mapping == fkname for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, fkname)
        self._post(cond)

    def test_prune_fkey_in_sourcedefs_fkeys_by_schema_only(self):
        fkname = ["person_schema", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.source_definitions and m.mapping == fkname for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, [fkname[0], None])
        self._post(cond)

    def test_prune_fkey_in_sourcedefs_recurse(self):
        def cond(assertion):
            assertion(any([
                isinstance(vizcol, dict) and vizcol.get("sourcekey") == "dept_size"
                for vizcol in self.model.schemas['person_schema'].tables['person'].annotations[tag.visible_columns]['detailed']
            ]))

        self._pre(cond)
        mmo.prune(self.model, ["person_schema", "person_dept_fkey"])
        self._post(cond)

    def test_prune_col_in_search_box(self):
        def cond(assertion):
            assertion(len(mmo.find(self.model, ["person_schema", "person", "last_name"])) == 1)

        self._pre(cond)
        mmo.prune(self.model, ["person_schema", "person", "last_name"])
        self._post(cond)
