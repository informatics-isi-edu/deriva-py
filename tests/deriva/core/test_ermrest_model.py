# Tests for the datapath module.
#
# Environment variables:
#  DERIVA_PY_TEST_HOSTNAME: hostname of the test server
#  DERIVA_PY_CATALOG: catalog identifier of the reusable test catalog (optional)
#  DERIVA_PY_TEST_CREDENTIAL: user credential, if none, it will attempt to get credential for given hostname (optional)
#  DERIVA_PY_TEST_VERBOSE: set for verbose logging output to stdout (optional)

import logging
import os
import datetime
import itertools
import unittest
import requests
from deriva.core import DerivaServer, get_credential, ermrest_model, tag
from deriva.core import \
    crockford_b32encode, crockford_b32decode, \
    int_to_uintX, uintX_to_int, \
    datetime_to_epoch_microseconds, epoch_microseconds_to_datetime
from deriva.core.ermrest_model import \
    timestamptz_to_datetime, datetime_to_timestamptz, \
    snaptime_to_epoch_microseconds, epoch_microseconds_to_snaptime, \
    datetime_to_snaptime, snaptime_to_datetime, \
    timestamptz_to_snaptime, snaptime_to_timestamptz

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
if os.getenv("DERIVA_PY_TEST_VERBOSE"):
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

hostname = os.getenv("DERIVA_PY_TEST_HOSTNAME")

class ErmrestTimestampCodingTests (unittest.TestCase):
    _basic_equivalents = [
        # datetime, isostr, usecs, snap
        (
            datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc),
            "1970-01-01 00:00:00+00:00",
            0,
            '0',
        ),
        (
            datetime.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc),
            "1969-12-31 23:59:59.999999+00:00",
            -1,
            'Z-ZZZZ-ZZZZ-ZZZY',
        ),
        (
            datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc),
            '1900-01-01 00:00:00+00:00',
            -2208988800000000,
            'Z-ZW2D-VXQ8-DG00',
        ),
        (
            datetime.datetime(2030, 12, 31, 12, 34, 56, 789012, tzinfo=datetime.timezone.utc),
            '2030-12-31 12:34:56.789012+00:00',
            1924950896789012,
            '3DD-EWED-7318',
        ),
    ]

    def test_fromiso_denorm(self):
        for i in range(8):
            us = 789012 // 10**i * 10**i
            dt = datetime.datetime(2030, 12, 31, 12, 34, 56, us, tzinfo=datetime.timezone.utc)
            ts = '2030-12-31 12:34:56%s+00:00' % (
                ('.%d' % us).rstrip('0') if us else ''
            )
            self.assertEqual(dt, timestamptz_to_datetime(ts), f"{dt=} {ts=}")

    def test_timestamptz_to_datetime(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(dt, timestamptz_to_datetime(ts), f"{dt=} {ts=}")

    def test_datetime_to_timestamptz(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(ts, datetime_to_timestamptz(dt), f"{ts=} {dt=}")

    def test_timestamptz_to_usecs(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(usecs, datetime_to_epoch_microseconds(timestamptz_to_datetime(ts)), f"{usecs=} {ts=}")

    def test_usecs_to_timestamptz(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(ts, datetime_to_timestamptz(epoch_microseconds_to_datetime(usecs)), f"{ts=} {usecs=}")

    def test_usecs_to_snaptime(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(snap, epoch_microseconds_to_snaptime(usecs), f"{snap=} {usecs=}")

    def test_snap_to_usecs(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(usecs, snaptime_to_epoch_microseconds(snap), f"{usecs=} {snap=}")

    def test_datetime_to_snaptime(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(snap, datetime_to_snaptime(dt), f"{snap=} {dt=}")

    def test_snaptime_to_datetime(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(dt, snaptime_to_datetime(snap), f"{dt=} {snap=}")

    def test_timestamptz_to_snaptime(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(snap, timestamptz_to_snaptime(ts), f"{snap=} {ts=}")

    def test_snaptime_to_timestamptz(self):
        for dt, ts, usecs, snap in self._basic_equivalents:
            self.assertEqual(ts, snaptime_to_timestamptz(snap), f"{ts=} {snap=}")

class ErmrestModelTypeTests (unittest.TestCase):

    _scalar_typenames = [
        'boolean',
        'date',
        'float4', 'float8',
        'json', 'jsonb',
        'int2', 'int4', 'int8',
        'text',
        'timestamptz', 'timestamp',
    ]

    _serial_typenames = [
        'serial2', 'serial4', 'serial8',
    ]

    _array_typenames = {
        f'{typename}[]': typename
        for typename in _scalar_typenames
    }

    _domain_typenames = {
        'color_rgb_hex': 'text',
        'ermrest_curie': 'text',
        'ermrest_rid': 'text',
        'ermrest_rcb': 'text',
        'ermrest_rmb': 'text',
        'ermrest_rct': 'timestamptz',
        'ermrest_rmt': 'timestamptz',
        'ermrest_uri': 'text',
        'markdown': 'text',
        'longtext': 'text',
    }

    def test_builtin_types(self):
        for typename in itertools.chain(
                self._scalar_typenames,
                self._serial_typenames,
                self._array_typenames,
                self._domain_typenames,
        ):
            with self.subTest(typename=typename):
                self.assertIn(typename, ermrest_model.builtin_types)
                self.assertIsInstance(
                    ermrest_model.builtin_types[typename],
                    ermrest_model.Type,
                )

    def test_type_hierarchy(self):
        for typename in itertools.chain(
                self._scalar_typenames,
                self._serial_typenames,
        ):
            with self.subTest(typename=typename):
                typ = ermrest_model.builtin_types[typename]
                self.assertNotIsInstance(typ, ermrest_model.ArrayType)
                self.assertNotIsInstance(typ, ermrest_model.DomainType)
                self.assertFalse(typ.is_array)
                self.assertFalse(typ.is_domain)

        for typename in self._array_typenames:
            with self.subTest(typename=typename):
                typ = ermrest_model.builtin_types[typename]
                self.assertIsInstance(typ, ermrest_model.ArrayType)
                self.assertNotIsInstance(typ, ermrest_model.DomainType)
                self.assertTrue(typ.is_array)
                self.assertFalse(typ.is_domain)

        for typename in self._domain_typenames:
            with self.subTest(typename=typename):
                typ = ermrest_model.builtin_types[typename]
                self.assertNotIsInstance(typ, ermrest_model.ArrayType)
                self.assertIsInstance(typ, ermrest_model.DomainType)
                self.assertFalse(typ.is_array)
                self.assertTrue(typ.is_domain)

    def test_base_types(self):
        for typename in itertools.chain(
                self._scalar_typenames,
                self._serial_typenames,
        ):
            with self.subTest(typename=typename):
                typ = ermrest_model.builtin_types[typename]
                self.assertNotHasAttr(typ, 'base_type')

        for typename, btypename in itertools.chain(
                self._array_typenames.items(),
                self._domain_typenames.items(),
        ):
            with self.subTest(typename=typename):
                typ = ermrest_model.builtin_types[typename]
                self.assertHasAttr(typ, 'base_type')
                btyp = typ.base_type
                self.assertEqual(btypename, btyp.typename)

    def test_prejson(self):
        for typename in itertools.chain(
                self._scalar_typenames,
                self._serial_typenames,
                self._array_typenames,
                self._domain_typenames,
        ):
            with self.subTest(typename=typename, scenario='typename'):
                typ = ermrest_model.builtin_types[typename]
                pj = typ.prejson()
                self.assertIsInstance(pj, dict)
                self.assertIn('typename', pj)
                self.assertEqual(typename, pj['typename'])

        for typename, btypename in itertools.chain(
                self._array_typenames.items(),
                self._domain_typenames.items(),
        ):
            with self.subTest(typename=typename, scenario='base_type'):
                typ = ermrest_model.builtin_types[typename]
                btyp = ermrest_model.builtin_types[typename]
                pj = typ.prejson()
                bpj = btyp.prejson()
                self.assertIn('base_type', pj)
                self.assertDictEqual(bpj, pj)

    def test_sqlite3_ddl(self):
        for typename in itertools.chain(
                self._scalar_typenames,
                self._serial_typenames,
        ):
            with self.subTest(typename=typename):
                typ = ermrest_model.builtin_types[typename]
                expect = {
                    'boolean': 'boolean',
                    'date': 'date',
                    'float4': 'real',
                    'float8': 'real',
                    'int2': 'integer',
                    'int4': 'integer',
                    'int8': 'integer',
                    'json': 'json',
                    'jsonb': 'json',
                    'serial2': 'integer',
                    'serial4': 'integer',
                    'serial8': 'integer',
                    'timestamptz': 'datetime',
                    'timestamp': 'datetime',
                }.get(typename, 'text')
                self.assertEqual(expect, typ.sqlite3_ddl())

        for typename, btypename in self._array_typenames.items():
            with self.subTest(typename=typename):
                typ = ermrest_model.builtin_types[typename]
                self.assertEqual('json', typ.sqlite3_ddl())

        for typename, btypename in self._domain_typenames.items():
            with self.subTest(typename=typename):
                typ = ermrest_model.builtin_types[typename]
                btyp = typ.base_type
                self.assertEqual(
                    btyp.sqlite3_ddl(),
                    typ.sqlite3_ddl(),
                )

    def test_indexable(self):
        for typename in itertools.chain(
                self._scalar_typenames,
                self._serial_typenames,
                self._array_typenames,
                self._domain_typenames,
        ):
            with self.subTest(typename=typename, indextype=ermrest_model.IndexType.trgm):
                typ = ermrest_model.builtin_types[typename]
                self.assertTrue(typ.indexable(ermrest_model.IndexType.trgm))

        for typename in itertools.chain(
                self._scalar_typenames,
                self._serial_typenames,
                self._domain_typenames,
        ):
            with self.subTest(typename=typename, indextype=ermrest_model.IndexType.gin_array):
                typ = ermrest_model.builtin_types[typename]
                self.assertFalse(typ.indexable(ermrest_model.IndexType.gin_array))

        for typename in self._array_typenames:
            with self.subTest(typename=typename, indextype=ermrest_model.IndexType.gin_array):
                typ = ermrest_model.builtin_types[typename]
                if typename == 'json[]':
                    self.assertFalse(typ.indexable(ermrest_model.IndexType.gin_array))
                else:
                    self.assertTrue(typ.indexable(ermrest_model.IndexType.gin_array))

        for typename in itertools.chain(
                self._scalar_typenames,
                self._serial_typenames,
                self._array_typenames,
                self._domain_typenames,
        ):
            with self.subTest(typename=typename, indextype=ermrest_model.IndexType.btree):
                typ = ermrest_model.builtin_types[typename]
                if typename in {'json', 'json[]'}:
                    self.assertFalse(typ.indexable(ermrest_model.IndexType.btree))
                else:
                    self.assertTrue(typ.indexable(ermrest_model.IndexType.btree))

    def test_history_indexable(self):
        for typename in itertools.chain(
                self._scalar_typenames,
                self._serial_typenames,
                self._array_typenames,
                self._domain_typenames,
        ):
            with self.subTest(typename=typename, indextype=ermrest_model.IndexType.trgm):
                typ = ermrest_model.builtin_types[typename]
                if typename in {'timestamp', 'timestamp[]'}:
                    self.assertFalse(typ.history_indexable(ermrest_model.IndexType.trgm))
                else:
                    self.assertTrue(typ.history_indexable(ermrest_model.IndexType.trgm))

        for typename in itertools.chain(
                self._scalar_typenames,
                self._serial_typenames,
                self._domain_typenames,
        ):
            with self.subTest(typename=typename, indextype=ermrest_model.IndexType.gin_array):
                typ = ermrest_model.builtin_types[typename]
                self.assertFalse(typ.history_indexable(ermrest_model.IndexType.gin_array))

        for typename in self._array_typenames:
            with self.subTest(typename=typename, indextype=ermrest_model.IndexType.gin_array):
                typ = ermrest_model.builtin_types[typename]
                if typename in {'timestamp[]', 'json[]'}:
                    self.assertFalse(typ.history_indexable(ermrest_model.IndexType.gin_array))
                else:
                    self.assertTrue(typ.history_indexable(ermrest_model.IndexType.gin_array))

        for typename in itertools.chain(
                self._scalar_typenames,
                self._serial_typenames,
                self._array_typenames,
                self._domain_typenames,
        ):
            with self.subTest(typename=typename, indextype=ermrest_model.IndexType.btree):
                typ = ermrest_model.builtin_types[typename]
                if typename in {'timestamp', 'timestamp[]', 'json', 'json[]'}:
                    self.assertFalse(typ.history_indexable(ermrest_model.IndexType.btree))
                else:
                    self.assertTrue(typ.history_indexable(ermrest_model.IndexType.btree))

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

    def _create_schema_for_indexing(self):
        """Creates a simple schema with indexing target columns."""

        # build a single, low-level catalog /schema POST operation
        # should be (slightly) faster and avoids using the client APIs under test in this module
        schema_def = ermrest_model.Schema.define('schema_for_indexing')
        schema_def["tables"] = {
            "target": ermrest_model.Table.define(
                'target',
                column_defs=[
                    ermrest_model.Column.define(f'c_{typ.typename}', typ)
                    for typ in ermrest_model.builtin_types.values()
                ],
            )
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

    def _skip_unless_index_mgmt_present(self):
        if not hasattr(self, 'ermrest_features'):
            self.ermrest_features = self.catalog.get("/").json()['features']
        if 'index_management' not in self.ermrest_features:
            self.skipTest('catalog does not advertise index_management!')

    def test_index_management(self):
        self._skip_unless_index_mgmt_present()
        self._create_schema_for_indexing()

        target = self.model.schemas['schema_for_indexing'].tables['target']

        live_idx = None
        with self.subTest(scenario='live index list'):
            res = target.get_indexes()
            self.assertIsInstance(res, list)
            self.assertGreater(len(res), 0)
            self.assertTrue(all([ isinstance(idx, ermrest_model.LiveIndex) for idx in res ]))
            live_idx = res[0]

        history_idx = None
        with self.subTest(scenario='history index list'):
            res = target.get_history_indexes()
            self.assertIsInstance(res, list)
            self.assertGreater(len(res), 0)
            self.assertTrue(all([ isinstance(idx, ermrest_model.HistoryIndex) for idx in res ]))
            history_idx = res[0]

        with self.subTest(scenario='history index list low-level'):
            res = self.model.get_history_indexes(target.rid)
            self.assertIsInstance(res, list)
            self.assertGreater(len(res), 0)
            self.assertTrue(all([ isinstance(idx, ermrest_model.HistoryIndex) for idx in res ]))

        if live_idx:
            with self.subTest(indexname=live_idx, scenario='get live index'):
                idx = target.get_index(live_idx.indexname)
                self.assertIsInstance(idx, ermrest_model.LiveIndex)
                self.assertEqual(live_idx.indexname, idx.indexname)

        if history_idx:
            with self.subTest(indexname=history_idx, scenario='get history index'):
                idx = target.get_history_index(history_idx.indexname)
                self.assertIsInstance(idx, ermrest_model.HistoryIndex)
                self.assertEqual(history_idx.indexname, idx.indexname)

        with self.subTest(scenario='get live notfound'):
            with self.assertRaises(requests.HTTPError) as cm:
                idx = target.get_index('nonexistant index')
            self.assertEqual(cm.exception.response.status_code, 404)

        with self.subTest(scenario='get history notfound'):
            with self.assertRaises(requests.HTTPError) as cm:
                idx = target.get_history_index('nonexistant index')
            self.assertEqual(cm.exception.response.status_code, 404)

        for indextype in [
                ermrest_model.IndexType.btree,
                ermrest_model.IndexType.trgm,
                ermrest_model.IndexType.gin_array,
        ]:
            with self.subTest(indextype=indextype, scenario='empty live target'):
                with self.assertRaises(ValueError):
                    idx = target.create_index('invalid_empty1_idx', indextype, [])

            with self.subTest(indextype=indextype, scenario='empty history target'):
                with self.assertRaises(ValueError):
                    idx = target.create_history_index('invalid_empty2_idx', indextype, [])

            l_parity = 0
            h_parity = 0
            for column in target.columns:
                cname = column.name
                if not cname.startswith('c_'):
                    continue

                indexname1 = f'target_{column.type.typename}_{indextype}_idx'
                indexname2 = f't{target.rid}_{column.type.typename}_{indextype}_idx'
                if not column.type.indexable(indextype):
                    with self.subTest(indexname=indexname1, scenario='create live nonindexable'):
                        with self.assertRaises(ValueError):
                            idx = target.create_index(indexname1, indextype, [column])
                else:
                    live_idx = None
                    with self.subTest(indexname=indexname1, scenario='create live indexable'):
                        idx = target.create_index(indexname1, indextype, [column])
                        self.assertIsInstance(idx, ermrest_model.LiveIndex)
                        self.assertEqual(indexname1, idx.indexname)
                        live_idx = idx

                    if live_idx:
                        with self.subTest(indexname=indexname1, scenario='table delete live index'):
                            if l_parity:
                                live_idx.delete_index(really=True)
                            else:
                                target.delete_index(indexname1, really=True)
                        l_parity = (l_parity + 1) % 2

                if not column.type.history_indexable(indextype):
                    with self.subTest(indexname=indexname2, scenario='create history nonindexable'):
                        with self.assertRaises(ValueError):
                            idx = target.create_history_index(indexname2, indextype, [column])
                else:
                    history_idx = None
                    with self.subTest(indexname=indexname2, scenario='table create history indexable'):
                        idx = target.create_history_index(indexname2, indextype, [column])
                        self.assertIsInstance(idx, ermrest_model.HistoryIndex)
                        self.assertEqual(indexname2, idx.indexname)
                        history_idx = idx

                    if history_idx:
                        with self.subTest(indexname=indexname2, scenario='table delete history index'):
                            if h_parity:
                                history_idx.delete_index(really=True)
                            else:
                                target.delete_history_index(indexname2, really=True)
                        h_parity = (h_parity + 1) % 2

if __name__ == '__main__':
    unittest.main()
