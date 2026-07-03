"""Regression test for issue #283: mmo.find() must tolerate a list-shaped
'filter' context in visible-columns / visible-foreign-keys annotations.

Chaise accepts a bare list for the 'filter' context (instead of the canonical
{"and": [...]} dict), so such catalogs exist in the wild. Before the fix,
mmo.find() called .get('and', ...) unconditionally and raised
`AttributeError: 'list' object has no attribute 'get'`, which aborted the whole
model walk -- breaking immediate-mode renames of ANY column/constraint on ANY
table in the catalog.

These tests build a minimal in-memory model stub so they run without a live
ERMrest server (unlike the other mmo tests, which require DERIVA_PY_TEST_*).
"""
import unittest

from deriva.core import mmo
from deriva.core import tag as tags


class _StubTable:
    def __init__(self, schema, name, annotations):
        self.schema = schema
        self.name = name
        self.annotations = annotations


class _StubSchema:
    def __init__(self, name, tables):
        self.name = name
        # find() iterates schema.tables.values()
        self.tables = {t.name: t for t in tables}
        for t in tables:
            t.schema = self


class _StubModel:
    def __init__(self, schemas):
        # find() iterates model.schemas.values()
        self.schemas = {s.name: s for s in schemas}


def _model_with_filter(filter_value, tag=tags.visible_foreign_keys):
    """Build a one-table model whose annotation's 'filter' context is
    `filter_value` (either a bare list or the canonical {"and": [...]} dict)."""
    table = _StubTable(
        schema=None,
        name="dept",
        annotations={
            tag: {
                "filter": filter_value,
            }
        },
    )
    schema = _StubSchema("dept_schema", [table])
    return _StubModel([schema])


class TestMMOFindListFilter(unittest.TestCase):

    def test_list_shaped_filter_does_not_crash(self):
        """REGRESSION (#283): a bare-list 'filter' context must not raise."""
        fkname = ["person_schema", "person_dept_fkey"]
        model = _model_with_filter([fkname])  # bare list, not {"and": [...]}
        # Before the fix this raised AttributeError: 'list' object has no
        # attribute 'get'.
        matches = mmo.find(model, fkname)
        # The constraint inside the list-shaped filter is found.
        self.assertTrue(
            any(m.tag == tags.visible_foreign_keys and m.mapping == fkname
                for m in matches),
            "list-shaped 'filter' constraint was not found: %r" % (matches,),
        )

    def test_list_and_dict_filter_yield_same_matches(self):
        """A bare list and the canonical {"and": [...]} form of the same
        entries produce equivalent find() results."""
        fkname = ["person_schema", "person_dept_fkey"]
        list_matches = mmo.find(_model_with_filter([fkname]), fkname)
        dict_matches = mmo.find(_model_with_filter({"and": [fkname]}), fkname)
        self.assertEqual(
            [m.mapping for m in list_matches],
            [m.mapping for m in dict_matches],
        )

    def test_empty_list_filter_is_safe(self):
        """An empty bare-list 'filter' yields no matches and does not crash."""
        model = _model_with_filter([])
        self.assertEqual(mmo.find(model, ["person_schema", "person_dept_fkey"]), [])

    def test_dict_filter_still_works(self):
        """The canonical {"and": [...]} form is unaffected by the fix."""
        fkname = ["person_schema", "person_dept_fkey"]
        model = _model_with_filter({"and": [fkname]})
        matches = mmo.find(model, fkname)
        self.assertTrue(
            any(m.mapping == fkname for m in matches),
            "dict-shaped 'filter' constraint was not found: %r" % (matches,),
        )


if __name__ == "__main__":
    unittest.main()
