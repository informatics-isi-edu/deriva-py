"""Unit tests for MMO find operation.
"""
import logging
import os

from deriva.core import mmo
from deriva.core.ermrest_model import tag
from tests.deriva.core.mmo.base import BaseMMOTestCase

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('DERIVA_PY_TEST_LOGLEVEL', default=logging.WARNING))


class TestMMOFind (BaseMMOTestCase):

    def test_find_key_in_vizcols(self):
        matches = mmo.find(self.model, ["dept_schema", "dept_RID_key"])
        self.assertEqual(len(matches), 1)

    def test_find_col_in_vizcols(self):
        matches = mmo.find(self.model, ["dept_schema", "dept", "RCT"])
        self.assertEqual(len(matches), 1)

    def test_find_col_in_vizcols_pseudocol_simple(self):
        matches = mmo.find(self.model, ["dept_schema", "dept", "RMT"])
        self.assertEqual(len(matches), 1)

    def test_find_col_in_vizcols_pseudocol(self):
        matches = mmo.find(self.model, ["dept_schema", "dept", "name"])
        self.assertTrue(any([m.anchor.name == 'person' and m.tag == tag.visible_columns and isinstance(m.mapping, dict) for m in matches]))

    def test_find_col_in_sourcedefs_columns(self):
        matches = mmo.find(self.model, ["person_schema", "person", "dept"])
        self.assertTrue(any([m.anchor.name == 'person' and m.tag == tag.source_definitions and m.mapping == 'dept' for m in matches]))

    def test_find_col_in_sourcedefs_sources(self):
        matches = mmo.find(self.model, ["person_schema", "person", "RID"])
        self.assertTrue(any([m.tag == tag.source_definitions and m.mapping == 'dept_size' for m in matches]))

    def test_find_fkey_in_vizfkeys(self):
        fkname = ["person_schema", "person_dept_fkey"]
        matches = mmo.find(self.model, fkname)
        self.assertTrue(any([m.tag == tag.visible_foreign_keys and m.mapping == fkname for m in matches]))

    def test_find_fkey_in_vizcols(self):
        fkname = ["person_schema", "person_dept_fkey"]
        matches = mmo.find(self.model, fkname)
        self.assertTrue(any([m.tag == tag.visible_columns and m.mapping == fkname for m in matches]))

    def test_find_fkey_in_sourcedefs_sources(self):
        matches = mmo.find(self.model, ["person_schema", "person_dept_fkey"])
        self.assertTrue(any([m.tag == tag.source_definitions and m.mapping == 'personnel' for m in matches]))

    def test_find_fkey_in_sourcedefs_fkeys(self):
        fkname = ["person_schema", "person_dept_fkey"]
        matches = mmo.find(self.model, fkname)
        self.assertTrue(any([m.tag == tag.source_definitions and m.mapping == fkname for m in matches]))

    def test_find_col_in_search_box(self):
        matches = mmo.find(self.model, ["person_schema", "person", "last_name"])
        self.assertTrue(len(matches) == 1)
