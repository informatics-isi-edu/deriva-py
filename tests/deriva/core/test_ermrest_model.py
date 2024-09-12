# Tests for the datapath module.
#
# Environment variables:
#  DERIVA_PY_TEST_HOSTNAME: hostname of the test server
#  DERIVA_PY_CATALOG: catalog identifier of the reusable test catalog (optional)
#  DERIVA_PY_TEST_CREDENTIAL: user credential, if none, it will attempt to get credential for given hostname (optional)
#  DERIVA_PY_TEST_VERBOSE: set for verbose logging output to stdout (optional)

import logging
import os
import unittest
from deriva.core import DerivaServer, get_credential, ermrest_model, tag

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
if os.getenv("DERIVA_PY_TEST_VERBOSE"):
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

hostname = os.getenv("DERIVA_PY_TEST_HOSTNAME")


@unittest.skipUnless(hostname, "Test host not specified")
class ErmrestModelTests (unittest.TestCase):

    catalog = None

    @classmethod
    def _purgeCatalog(cls):
        model = cls.catalog.getCatalogModel()

        # exclude the 'public' schema
        schemas = [s for s in model.schemas.values() if s.name != 'public']

        # drop all fkeys
        for s in schemas:
            for t in s.tables.values():
                for fk in list(t.foreign_keys):
                    fk.drop()

        # drop all tables and schemas
        for s in list(schemas):
            for t in list(s.tables.values()):
                t.drop()
            s.drop()

    @classmethod
    def setUpClass(cls):
        credential = os.getenv("DERIVA_PY_TEST_CREDENTIAL") or get_credential(hostname)
        server = DerivaServer('https', hostname, credentials=credential)
        catalog_id = os.getenv("DERIVA_PY_TEST_CATALOG")
        if catalog_id is not None:
            logger.info(f"Reusing catalog {catalog_id} on host {hostname}")
            cls.catalog = server.connect_ermrest(catalog_id)
            cls._purgeCatalog()
        else:
            cls.catalog = server.create_ermrest_catalog()
            logger.info(f"Created catalog {cls.catalog.catalog_id} on host {hostname}")

    @classmethod
    def tearDownClass(cls):
        if cls.catalog and os.getenv("DERIVA_PY_TEST_CATALOG") is None:
            logger.info(f"Deleting catalog {cls.catalog.catalog_id} on host {hostname}")
            cls.catalog.delete_ermrest_catalog(really=True)

    def setUp(self):
        self.model = self.catalog.getCatalogModel()

    def tearDown(self):
        self._purgeCatalog()

    def _create_schema_with_fkeys(self):
        """Creates a simple schema of two tables with a fkey relationship from child to parent."""

        # build a single, low-level catalog /schema POST operation
        # should be (slightly) faster and avoids using the client APIs under test in this module
        schema_def = ermrest_model.Schema.define('schema_with_fkeys')
        schema_def["tables"] = {
            "parent": ermrest_model.Table.define(
                'parent',
                column_defs=[
                    ermrest_model.Column.define('id', ermrest_model.builtin_types.text),
                    ermrest_model.Column.define('id_extra', ermrest_model.builtin_types.text),
                ],
                key_defs=[
                    ermrest_model.Key.define(['id'], constraint_name='parent_id_key'),
                    ermrest_model.Key.define(['id', 'id_extra'], constraint_name='parent_compound_key'),
                ]
            ),
            "child": ermrest_model.Table.define(
                'child',
                column_defs=[
                    ermrest_model.Column.define('parent_id', ermrest_model.builtin_types.text),
                    ermrest_model.Column.define('parent_id_extra', ermrest_model.builtin_types.text),
                ],
                fkey_defs=[
                    ermrest_model.ForeignKey.define(
                        ['parent_id'], 'schema_with_fkeys', 'parent', ['id']
                    ),
                    ermrest_model.ForeignKey.define(
                        ['parent_id_extra', 'parent_id'], 'schema_with_fkeys', 'parent', ['id_extra', 'id']
                    )
                ]
            ),
        }
        self.catalog.post('/schema', json=[schema_def])
        # refresh the local state of the model
        self.model = self.catalog.getCatalogModel()

    def test_0a_schema_define_defaults(self):
        sname = "test_schema"
        sdef = ermrest_model.Schema.define(sname)
        self.assertEqual(sdef.get("schema_name"), sname)
        self.assertEqual(sdef.get("comment"), None)
        self.assertEqual(sdef.get("acls"), dict())
        self.assertEqual(sdef.get("annotations"), dict())

    _test_comment = "my comment"
    _test_acls = {
        "insert": ["a"],
        "update": ["b"],
    }
    _test_acl_bindings = {
    }
    _test_annotations = {"tag1": "value1"}

    def test_0b_schema_define_custom(self):
        sname = "test_schema"
        sdef = ermrest_model.Schema.define(sname, self._test_comment, self._test_acls, self._test_annotations)
        self.assertEqual(sdef.get("schema_name"), sname)
        self.assertEqual(sdef.get("comment"), self._test_comment)
        self.assertEqual(sdef.get("acls"), self._test_acls)
        self.assertEqual(sdef.get("annotations"), self._test_annotations)

    def test_1a_column_define_defaults(self):
        cname = "test_column"
        cdef = ermrest_model.Column.define(cname, ermrest_model.builtin_types.text)
        self.assertEqual(cdef.get("name"), cname)
        ctype = cdef.get("type")
        self.assertIsInstance(ctype, dict)
        self.assertEqual(ctype.get("typename"), "text")
        self.assertEqual(cdef.get("nullok"), True)
        self.assertEqual(cdef.get("default"), None)
        self.assertEqual(cdef.get("comment"), None)
        self.assertEqual(cdef.get("acls"), dict())
        self.assertEqual(cdef.get("acl_bindings"), dict())
        self.assertEqual(cdef.get("annotations"), dict())

    def test_1b_column_define_custom(self):
        cname = "test_column"
        nullok = False
        default = "my default"
        cdef = ermrest_model.Column.define(
            cname, ermrest_model.builtin_types.text, nullok, default,
            self._test_comment, self._test_acls, self._test_acl_bindings, self._test_annotations,
        )
        self.assertEqual(cdef.get("name"), cname)
        ctype = cdef.get("type")
        self.assertIsInstance(ctype, dict)
        self.assertEqual(ctype.get("typename"), "text")
        self.assertEqual(cdef.get("nullok"), nullok)
        self.assertEqual(cdef.get("default"), default)
        self.assertEqual(cdef.get("comment"), self._test_comment)
        self.assertEqual(cdef.get("acls"), self._test_acls)
        self.assertEqual(cdef.get("acl_bindings"), self._test_acl_bindings)
        self.assertEqual(cdef.get("annotations"), self._test_annotations)

    def test_1c_key_define_defaults(self):
        cnames = ["id"]
        kdef = ermrest_model.Key.define(cnames)
        self.assertEqual(kdef.get("unique_columns"), cnames)
        self.assertEqual(not kdef.get("names"), True)
        self.assertEqual(kdef.get("comment"), None)
        self.assertEqual(kdef.get("annotations"), dict())

    def test_1d_key_define_custom(self):
        cnames = ["id"]
        constraint_name = "my_constraint"
        kdef = ermrest_model.Key.define(cnames, None, self._test_comment, self._test_annotations, constraint_name)
        self.assertEqual(kdef.get("unique_columns"), cnames)
        self.assertIsInstance(kdef.get("names"), list)
        self.assertIsInstance(kdef.get("names")[0], list)
        self.assertEqual(kdef.get("names")[0][1], constraint_name)
        self.assertEqual(kdef.get("comment"), self._test_comment)
        self.assertEqual(kdef.get("annotations"), self._test_annotations)

    def test_1e_fkey_define_defaults(self):
        fk_colnames = ["fk1"]
        pk_sname = "pk_schema"
        pk_tname = "pk_table"
        pk_colnames = ["RID"]
        fkdef = ermrest_model.ForeignKey.define(
            fk_colnames,
            pk_sname,
            pk_tname,
            pk_colnames,
        )
        self.assertEqual([ c.get("column_name") for c in fkdef.get("foreign_key_columns") ], fk_colnames)
        self.assertEqual([ c.get("column_name") for c in fkdef.get("referenced_columns") ], pk_colnames)
        self.assertEqual(all([ c.get("schema_name") == pk_sname for c in fkdef.get("referenced_columns") ]), True)
        self.assertEqual(all([ c.get("table_name") == pk_tname for c in fkdef.get("referenced_columns") ]), True)
        self.assertEqual(fkdef.get("on_update"), "NO ACTION")
        self.assertEqual(fkdef.get("on_delete"), "NO ACTION")
        self.assertEqual(not fkdef.get("names"), True)
        self.assertEqual(fkdef.get("comment"), None)
        self.assertEqual(fkdef.get("acls"), dict())
        self.assertEqual(fkdef.get("acl_bindings"), dict())
        self.assertEqual(fkdef.get("annotations"), dict())

    def test_1f_fkey_define_custom(self):
        fk_colnames = ["fk1"]
        pk_sname = "pk_schema"
        pk_tname = "pk_table"
        pk_colnames = ["RID"]
        constraint_name = "my_constraint"
        action1 = "CASCADE"
        action2 = "RESTRICT"
        fkdef = ermrest_model.ForeignKey.define(
            fk_colnames,
            pk_sname,
            pk_tname,
            pk_colnames,
            action1,
            action2,
            None,
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
            constraint_name,
        )
        self.assertEqual([ c.get("column_name") for c in fkdef.get("foreign_key_columns") ], fk_colnames)
        self.assertEqual([ c.get("column_name") for c in fkdef.get("referenced_columns") ], pk_colnames)
        self.assertEqual(all([ c.get("schema_name") == pk_sname for c in fkdef.get("referenced_columns") ]), True)
        self.assertEqual(all([ c.get("table_name") == pk_tname for c in fkdef.get("referenced_columns") ]), True)
        self.assertEqual(fkdef.get("on_update"), action1)
        self.assertEqual(fkdef.get("on_delete"), action2)
        self.assertIsInstance(fkdef.get("names"), list)
        self.assertIsInstance(fkdef.get("names")[0], list)
        self.assertEqual(fkdef.get("names")[0][1], constraint_name)
        self.assertEqual(fkdef.get("comment"), self._test_comment)
        self.assertEqual(fkdef.get("acls"), self._test_acls)
        self.assertEqual(fkdef.get("acl_bindings"), self._test_acl_bindings)
        self.assertEqual(fkdef.get("annotations"), self._test_annotations)

    def test_2a_table_define_defaults(self):
        tname = "test_table"
        tdef = ermrest_model.Table.define(
            tname,
            [
                ermrest_model.Column.define("id", ermrest_model.builtin_types.text),
            ],
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 1)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 2)
        self.assertEqual(tdef.get("comment"), None)
        self.assertEqual(tdef.get("acls"), dict())
        self.assertEqual(tdef.get("acl_bindings"), dict())
        self.assertEqual(tdef.get("annotations"), dict())
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_2b_table_define_custom(self):
        tname = "test_table"
        tdef = ermrest_model.Table.define(
            tname,
            [
                ermrest_model.Column.define("id", ermrest_model.builtin_types.text),
                ermrest_model.Column.define("fk1", ermrest_model.builtin_types.text),
            ],
            [
                ermrest_model.Key.define(["id"]),
            ],
            [
                ermrest_model.ForeignKey.define(["fk1"], "public", "ERMrest_Client", ["RID"]),
            ],
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
            provide_system_fkeys=False,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 2)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 1)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 1)
        self.assertEqual(tdef.get("comment"), self._test_comment)
        self.assertEqual(tdef.get("acls"), self._test_acls)
        self.assertEqual(tdef.get("acl_bindings"), self._test_acl_bindings)
        self.assertEqual(tdef.get("annotations"), self._test_annotations)
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_2c_table_define_with_reference(self):
        tname = "test_table"
        tdef = ermrest_model.Table.define(
            tname,
            [
                ermrest_model.Column.define("id", ermrest_model.builtin_types.text),
                self.model.schemas["public"].tables["ERMrest_Client"],
            ],
            [],
            [],
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 2)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 2 + 1)
        self.assertEqual(
            {
                (
                    fkdef["referenced_columns"][0]["schema_name"],
                    fkdef["referenced_columns"][0]["table_name"],
                    frozenset(zip(
                        ( c["column_name"] for c in fkdef["foreign_key_columns"] ),
                        ( c["column_name"] for c in fkdef["referenced_columns"] ),
                    ))
                )
                for fkdef in fkdefs
             },
            {
                ("public", "ERMrest_Client", frozenset([ ("ERMrest_Client", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RCB", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RMB", "ID",) ])),
            }
        )
        self.assertEqual(tdef.get("comment"), self._test_comment)
        self.assertEqual(tdef.get("acls"), self._test_acls)
        self.assertEqual(tdef.get("acl_bindings"), self._test_acl_bindings)
        self.assertEqual(tdef.get("annotations"), self._test_annotations)
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_3a_vocab_define_defaults(self):
        tname = "test_table"
        curie_template = "test:{RID}"
        tdef = ermrest_model.Table.define_vocabulary(
            tname,
            curie_template,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 5)
        self.assertEqual({ c["name"] for c in cdefs }, {"RID","RCT","RMT","RCB","RMB","ID","Name","Description","Synonyms","URI"})
        cdefs = { c["name"]: c for c in cdefs }
        self.assertEqual(cdefs["ID"]["default"], curie_template)
        self.assertEqual(cdefs["URI"]["default"], '/id/{RID}')
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 3)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 2)
        self.assertEqual(tdef.get("comment"), None)
        self.assertEqual(tdef.get("acls"), dict())
        self.assertEqual(tdef.get("acl_bindings"), dict())
        self.assertEqual(tdef.get("annotations"), dict())
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_3b_vocab_define_custom(self):
        tname = "test_table"
        curie_template = "test:{RID}"
        uri_template = "/id/1/{RID}"
        tdef = ermrest_model.Table.define_vocabulary(
            tname,
            curie_template,
            uri_template,
            [
                ermrest_model.Column.define("fk1", ermrest_model.builtin_types.text),
            ],
            [],
            [
                ermrest_model.ForeignKey.define(["fk1"], "public", "ERMrest_Client", ["RID"]),
            ],
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
            provide_name_key=False,
            provide_system_fkeys=False,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 5 + 1)
        self.assertEqual({ c["name"] for c in cdefs }, {"RID","RCT","RMT","RCB","RMB","ID","Name","Description","Synonyms","URI","fk1"})
        cdefs = { c["name"]: c for c in cdefs }
        self.assertEqual(cdefs["ID"]["default"], curie_template)
        self.assertEqual(cdefs["URI"]["default"], uri_template)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 2)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 1)
        self.assertEqual(tdef.get("comment"), self._test_comment)
        self.assertEqual(tdef.get("acls"), self._test_acls)
        self.assertEqual(tdef.get("acl_bindings"), self._test_acl_bindings)
        self.assertEqual(tdef.get("annotations"), self._test_annotations)
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_3c_vocab_define_with_reference(self):
        tname = "test_table"
        curie_template = "test:{RID}"
        uri_template = "/id/1/{RID}"
        target = self.model.schemas["public"].tables["ERMrest_Client"]
        self.assertIsInstance(target, ermrest_model.Table)
        tdef = ermrest_model.Table.define_vocabulary(
            tname,
            curie_template,
            uri_template,
            [ target, ],
            [],
            [],
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 5 + 1)
        self.assertEqual({ c["name"] for c in cdefs }, {"RID","RCT","RMT","RCB","RMB","ID","Name","Description","Synonyms","URI","ERMrest_Client"})
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 3)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 2 + 1)
        self.assertEqual(
            {
                (
                    fkdef["referenced_columns"][0]["schema_name"],
                    fkdef["referenced_columns"][0]["table_name"],
                    frozenset(zip(
                        ( c["column_name"] for c in fkdef["foreign_key_columns"] ),
                        ( c["column_name"] for c in fkdef["referenced_columns"] ),
                    ))
                )
                for fkdef in fkdefs
             },
            {
                ("public", "ERMrest_Client", frozenset([ ("ERMrest_Client", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RCB", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RMB", "ID",) ])),
            }
        )
        self.assertEqual(tdef.get("comment"), self._test_comment)
        self.assertEqual(tdef.get("acls"), self._test_acls)
        self.assertEqual(tdef.get("acl_bindings"), self._test_acl_bindings)
        self.assertEqual(tdef.get("annotations"), self._test_annotations)
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)  

    def test_4a_asset_define_defaults(self):
        tname = "test_table"
        tdef = ermrest_model.Table.define_asset(
            "model_define_schema",
            tname,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 5)
        self.assertEqual({ c["name"] for c in cdefs }, {"RID","RCT","RMT","RCB","RMB","Filename","Description","Length","MD5","URL"})
        cdefs = { c["name"]: c for c in cdefs }
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 1)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 2)
        self.assertEqual(tdef.get("acls"), dict())
        self.assertEqual(tdef.get("acl_bindings"), dict())
        self.assertEqual(set(tdef.get("annotations").keys()), {tag.table_display})
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_4b_asset_define_custom(self):
        tname = "test_table"
        hatrac_template = "/hatrac/foo/{{{MD5}}}.{{{Filename}}}"
        tdef = ermrest_model.Table.define_asset(
            "model_define_schema",
            tname,
            hatrac_template,
            [
                ermrest_model.Column.define("fk1", ermrest_model.builtin_types.text),
            ],
            [],
            [
                ermrest_model.ForeignKey.define(["fk1"], "public", "ERMrest_Client", ["RID"]),
            ],
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
            provide_system_fkeys=False,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 5 + 1)
        self.assertEqual({ c["name"] for c in cdefs }, {"RID","RCT","RMT","RCB","RMB","Filename","Description","Length","MD5","URL","fk1"})
        cdefs = { c["name"]: c for c in cdefs }
        self.assertEqual(cdefs["URL"]["annotations"][tag.asset]["url_pattern"], hatrac_template)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 1)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 1)
        self.assertEqual(tdef.get("comment"), self._test_comment)
        self.assertEqual(tdef.get("acls"), self._test_acls)
        self.assertEqual(tdef.get("acl_bindings"), self._test_acl_bindings)
        annotations = tdef.get("annotations")
        for k, v in self._test_annotations.items():
            self.assertEqual(annotations[k], v)
        self.assertIn(tag.table_display, annotations)
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_4c_asset_define_with_reference(self):
        tname = "test_table"
        hatrac_template = "/hatrac/foo/{{{MD5}}}.{{{Filename}}}"
        target = self.model.schemas["public"].tables["ERMrest_Client"]
        self.assertIsInstance(target, ermrest_model.Table)
        tdef = ermrest_model.Table.define_asset(
            "model_define_schema",
            tname,
            hatrac_template,
            [ target, ],
            [],
            [],
            self._test_comment,
            self._test_acls,
            self._test_acl_bindings,
            self._test_annotations,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 5 + 1)
        self.assertEqual({ c["name"] for c in cdefs }, {"RID","RCT","RMT","RCB","RMB","Filename","Description","Length","MD5","URL","ERMrest_Client"})
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(len(kdefs), 1 + 1)
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(len(fkdefs), 2 + 1)
        self.assertEqual(
            {
                (
                    fkdef["referenced_columns"][0]["schema_name"],
                    fkdef["referenced_columns"][0]["table_name"],
                    frozenset(zip(
                        ( c["column_name"] for c in fkdef["foreign_key_columns"] ),
                        ( c["column_name"] for c in fkdef["referenced_columns"] ),
                    ))
                )
                for fkdef in fkdefs
             },
            {
                ("public", "ERMrest_Client", frozenset([ ("ERMrest_Client", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RCB", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RMB", "ID",) ])),
            }
        )
        self.assertEqual(tdef.get("comment"), self._test_comment)
        self.assertEqual(tdef.get("acls"), self._test_acls)
        self.assertEqual(tdef.get("acl_bindings"), self._test_acl_bindings)
        annotations = tdef.get("annotations")
        for k, v in self._test_annotations.items():
            self.assertEqual(annotations[k], v)
        self.assertIn(tag.table_display, annotations)
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)  

    def test_5a_assoctable_define_defaults(self):
        tdef = ermrest_model.Table.define_association(
            [
                self.model.schemas["public"].tables["ERMrest_Client"],
                self.model.schemas["public"].tables["ERMrest_Group"],
            ],
        )
        self.assertEqual(tdef.get("table_name"), "ERMrest_Client_ERMrest_Group")
        self.assertEqual(tdef.get("comment"), None)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 2)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(
            { tuple(key["unique_columns"]) for key in kdefs },
            { ('RID',), ('ERMrest_Client', 'ERMrest_Group') },
        )
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(
            {
                (
                    fkdef["referenced_columns"][0]["schema_name"],
                    fkdef["referenced_columns"][0]["table_name"],
                    frozenset(zip(
                        tuple( c["column_name"] for c in fkdef["foreign_key_columns"] ),
                        tuple( c["column_name"] for c in fkdef["referenced_columns"] ),
                    ))
                )
                for fkdef in fkdefs
             },
            {
                ("public", "ERMrest_Client", frozenset([ ("ERMrest_Client", "ID",) ])),
                ("public", "ERMrest_Group", frozenset([ ("ERMrest_Group", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RCB", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RMB", "ID",) ])),
            }
        )
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_5b_assoctable_define_custom(self):
        tname = "client_group"
        tdef = ermrest_model.Table.define_association(
            [
                ('c_id', self.model.schemas["public"].tables["ERMrest_Client"]),
                ('g_id', self.model.schemas["public"].tables["ERMrest_Group"]),
            ],
            [],
            tname,
            self._test_comment,
            provide_system_fkeys=False,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        self.assertEqual(tdef.get("comment"), self._test_comment)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 2)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(
            { tuple(key["unique_columns"]) for key in kdefs },
            { ('RID',), ('c_id', 'g_id') },
        )
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(
            {
                (
                    fkdef["referenced_columns"][0]["schema_name"],
                    fkdef["referenced_columns"][0]["table_name"],
                    frozenset(zip(
                        tuple( c["column_name"] for c in fkdef["foreign_key_columns"] ),
                        tuple( c["column_name"] for c in fkdef["referenced_columns"] ),
                    ))
                )
                for fkdef in fkdefs
             },
            {
                ("public", "ERMrest_Client", frozenset([ ("c_id", "ID",) ])),
                ("public", "ERMrest_Group", frozenset([ ("g_id", "ID",) ])),
            }
        )
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_5c_assoctable_define_with_metadata(self):
        tname = "client_group"
        tdef = ermrest_model.Table.define_association(
            [
                ('c_id', self.model.schemas["public"].tables["ERMrest_Client"]),
                ('g_id', self.model.schemas["public"].tables["ERMrest_Group"]),
            ],
            [
                ermrest_model.Column.define("md1", ermrest_model.builtin_types.text),
            ],
            tname,
            self._test_comment,
        )
        self.assertEqual(tdef.get("table_name"), tname)
        self.assertEqual(tdef.get("comment"), self._test_comment)
        cdefs = tdef.get("column_definitions")
        self.assertIsInstance(cdefs, list)
        self.assertEqual(len(cdefs), 5 + 2 + 1)
        kdefs = tdef.get("keys")
        self.assertIsInstance(kdefs, list)
        self.assertEqual(
            { tuple(key["unique_columns"]) for key in kdefs },
            { ('RID',), ('c_id', 'g_id') },
        )
        fkdefs = tdef.get("foreign_keys")
        self.assertIsInstance(fkdefs, list)
        self.assertEqual(
            {
                (
                    fkdef["referenced_columns"][0]["schema_name"],
                    fkdef["referenced_columns"][0]["table_name"],
                    frozenset(zip(
                        tuple( c["column_name"] for c in fkdef["foreign_key_columns"] ),
                        tuple( c["column_name"] for c in fkdef["referenced_columns"] ),
                    ))
                )
                for fkdef in fkdefs
             },
            {
                ("public", "ERMrest_Client", frozenset([ ("c_id", "ID",) ])),
                ("public", "ERMrest_Group", frozenset([ ("g_id", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RCB", "ID",) ])),
                ("public", "ERMrest_Client", frozenset([ ("RMB", "ID",) ])),
            }
        )
        schema = self.model.create_schema(ermrest_model.Schema.define("model_define_schema"))
        table = schema.create_table(tdef)
        self.assertIsInstance(table, ermrest_model.Table)

    def test_key_drop_cascading(self):
        self._create_schema_with_fkeys()
        schema = self.model.schemas['schema_with_fkeys']
        self.model.schemas['schema_with_fkeys'].tables['parent'].keys[(schema, 'parent_id_key')].drop(cascade=True)

    def test_key_reordered_drop_cascading(self):
        self._create_schema_with_fkeys()
        schema = self.model.schemas['schema_with_fkeys']
        self.model.schemas['schema_with_fkeys'].tables['parent'].keys[(schema, 'parent_compound_key')].drop(cascade=True)

    def test_key_column_drop_cascading(self):
        self._create_schema_with_fkeys()
        self.model.schemas['schema_with_fkeys'].tables['parent'].columns['id'].drop(cascade=True)

    def test_fkey_column_drop_cascading(self):
        self._create_schema_with_fkeys()
        self.model.schemas['schema_with_fkeys'].tables['child'].columns['parent_id_extra'].drop(cascade=True)

    def test_table_drop_cascading(self):
        self._create_schema_with_fkeys()
        self.model.schemas['schema_with_fkeys'].tables['parent'].drop(cascade=True)

    def test_schema_drop_cascading(self):
        self._create_schema_with_fkeys()
        self.model.schemas['schema_with_fkeys'].drop(cascade=True)


if __name__ == '__main__':
    unittest.main()
