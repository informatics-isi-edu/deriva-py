# Tests for the datapath module.
#
# Environment variables:
#  DERIVA_PY_TEST_HOSTNAME: hostname of the test server
#  DERIVA_PY_TEST_CREDENTIAL: user credential, if none, it will attempt to get credentail for given hostname
#  DERIVA_PY_TEST_VERBOSE: set for verbose logging output to stdout

from copy import deepcopy
import logging
from operator import itemgetter
import os
import unittest
import sys
from deriva.core import DerivaServer, get_credential, ermrest_model as _em, __version__
from deriva.core.datapath import DataPathException, Min, Max, Sum, Avg, Cnt, CntD, Array, ArrayD, Bin, \
    simple_denormalization_with_whole_entities

try:
    from pandas import DataFrame
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

TEST_EXP_MAX = 100
TEST_EXPTYPE_MAX = 10
TEST_EXP_NAME_FORMAT = "experiment-{}"
TEST_PROJ_MAX = 1
TEST_PROJ_INVESTIGATOR = "Smith"
TEST_PROJ_NUM = 1

SPECIAL_CHARACTERS = '`~!@#$%^&*()_+-={}|[]\\;:"\',./<>?'
INVALID_IDENTIFIER, INVALID_IDENTIFIER_FIXED = '9 %$ ', '_9____'
RESERVED_IDENTIFIER = 'column_definitions'
CONFLICTING_IDENTIFIER, CONFLICTING_IDENTIFIER_FIXED = RESERVED_IDENTIFIER + '1', RESERVED_IDENTIFIER + '2'

SNAME_ISA = 'ISA'
SNAME_VOCAB = 'Vocab'
TNAME_PROJECT = 'Project'
TNAME_EXPERIMENT = 'Experiment'
TNAME_EXPERIMENT_TYPE = 'Experiment_Type'
TNAME_EXPERIMENT_COPY = 'Experiment_Copy'

hostname = os.getenv("DERIVA_PY_TEST_HOSTNAME")
logger = logging.getLogger(__name__)
if os.getenv("DERIVA_PY_TEST_VERBOSE"):
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())


def define_test_schema(catalog):
    """Defines the test schema.

    A 'vocab' schema with an 'experiment_type' term table.
    An 'isa' schema with an 'experiment' table, with 'type' that references the vocab table.
    """
    model = catalog.getCatalogModel()
    vocab = model.create_schema(_em.Schema.define(SNAME_VOCAB))
    vocab.create_table(_em.Table.define_vocabulary(TNAME_EXPERIMENT_TYPE, "TEST:{RID}"))
    isa = model.create_schema(_em.Schema.define(SNAME_ISA))

    # create TNAME_PROJECT table
    table_def = _em.Table.define(
        TNAME_PROJECT,
        column_defs=[
            _em.Column.define(cname, ctype) for (cname, ctype) in [
                ('Investigator', _em.builtin_types.text),
                ('Num', _em.builtin_types.int4),
                (INVALID_IDENTIFIER, _em.builtin_types.int4),
                (RESERVED_IDENTIFIER, _em.builtin_types.text),
                (RESERVED_IDENTIFIER + '1', _em.builtin_types.text)
            ]
        ],
        key_defs=[
            _em.Key.define(['Investigator', 'Num'])
        ]
    )
    isa.create_table(table_def)

    # experiment table definition helper
    def exp_table_def(exp_table_name):
        return _em.Table.define(
            exp_table_name,
            column_defs=[
                _em.Column.define(cname, ctype) for (cname, ctype) in [
                    ('Name', _em.builtin_types.text),
                    ('Amount', _em.builtin_types.int4),
                    ('Time', _em.builtin_types.timestamptz),
                    ('Type', _em.builtin_types.text),
                    ('Project Investigator', _em.builtin_types.text),
                    ('Project_Num', _em.builtin_types.int4),
                    ('Empty', _em.builtin_types.int4)
                ]
            ],
            key_defs=[
                _em.Key.define(['Name'])
            ],
            fkey_defs=[
                _em.ForeignKey.define(['Type'], SNAME_VOCAB, TNAME_EXPERIMENT_TYPE, ['ID']),
                _em.ForeignKey.define(['Project Investigator', 'Project_Num'], SNAME_ISA, TNAME_PROJECT, ['Investigator', 'Num'])
            ]
        )

    # create experiment tables
    isa.create_table(exp_table_def(TNAME_EXPERIMENT))
    isa.create_table(exp_table_def(TNAME_EXPERIMENT_COPY))


def _generate_experiment_entities(types, count):
    """Generates experiment entities (content only)

    :param types: type entities to be referenced from entities
    :param count: number of entities to return
    :return: a list of dict objects (experiment entities)
    """
    return [
        {
            "Name": TEST_EXP_NAME_FORMAT.format(i),
            "Amount": i,
            "Time": "2018-01-{}T01:00:00.0".format(1 + (i % 31)),
            "Type": types[i % TEST_EXPTYPE_MAX]['ID'],
            "Project Investigator": TEST_PROJ_INVESTIGATOR,
            "Project_Num": TEST_PROJ_NUM,
            "Empty": None
        }
        for i in range(count)
    ]


def populate_test_catalog(catalog):
    """Populate the test catalog."""
    paths = catalog.getPathBuilder()
    logger.debug("Inserting project...")
    proj_table = paths.schemas[SNAME_ISA].tables[TNAME_PROJECT]
    logger.debug("Inserting investigators...")
    proj_table.insert([
        {"Investigator": TEST_PROJ_INVESTIGATOR, "Num": TEST_PROJ_NUM}
    ])
    logger.debug("Inserting experiment types...")
    type_table = paths.schemas[SNAME_VOCAB].tables[TNAME_EXPERIMENT_TYPE]
    types = type_table.insert([
        {"Name": "{}".format(name), "Description": "NA"} for name in range(TEST_EXPTYPE_MAX)
    ], defaults=['ID', 'URI'])
    logger.debug("Inserting experiments...")
    exp = paths.schemas[SNAME_ISA].tables[TNAME_EXPERIMENT]
    exp.insert(_generate_experiment_entities(types, TEST_EXP_MAX))


@unittest.skipUnless(hostname, "Test host not specified")
class DatapathTests (unittest.TestCase):
    catalog = None

    @classmethod
    def setUpClass(cls):
        logger.debug("setupUpClass begin")
        credential = os.getenv("DERIVA_PY_TEST_CREDENTIAL") or get_credential(hostname)
        server = DerivaServer('https', hostname, credentials=credential)
        cls.catalog = server.create_ermrest_catalog()
        try:
            define_test_schema(cls.catalog)
            populate_test_catalog(cls.catalog)
        except Exception:
            # on failure, delete catalog and re-raise exception
            cls.catalog.delete_ermrest_catalog(really=True)
            raise
        logger.debug("setupUpClass done")

    @classmethod
    def tearDownClass(cls):
        logger.debug("tearDownClass begin")
        cls.catalog.delete_ermrest_catalog(really=True)
        logger.debug("tearDownClass done")

    def setUp(self):
        self.paths = self.catalog.getPathBuilder()
        self.project = self.paths.schemas[SNAME_ISA].tables[TNAME_PROJECT]
        self.experiment = self.paths.schemas[SNAME_ISA].tables[TNAME_EXPERIMENT]
        self.experiment_type = self.paths.schemas[SNAME_VOCAB].tables[TNAME_EXPERIMENT_TYPE]
        self.experiment_copy = self.paths.schemas[SNAME_ISA].tables[TNAME_EXPERIMENT_COPY]
        self.types = list(self.experiment_type.entities())
        self.model = self.catalog.getCatalogModel()

    def tearDown(self):
        try:
            self.experiment_copy.path.delete()
        except DataPathException:
            # suppresses 404 errors when the table is empty
            pass

    def test_catalog_dir_base(self):
        self.assertIn('schemas', dir(self.paths))

    def test_schema_dir_base(self):
        self.assertLess({'_name', 'tables', 'describe'}, set(dir(self.paths.schemas[SNAME_ISA])))

    def test_datapath_dir_base(self):
        self.assertLess({'aggregates', 'groupby', 'attributes', 'context', 'delete', 'entities', 'filter',
                         'link', 'table_instances', 'uri'}, set(dir(self.paths.schemas[SNAME_ISA].tables[TNAME_EXPERIMENT].path)))

    def test_table_dir_base(self):
        self.assertLess({'aggregates', 'alias', 'groupby', 'attributes', 'describe', 'entities', 'filter', 'insert',
                         'link', 'path', 'update', 'uri'}, set(dir(self.paths.schemas[SNAME_ISA].tables[TNAME_EXPERIMENT])))

    def test_catalog_dir_with_schemas(self):
        self.assertLess({SNAME_ISA, SNAME_VOCAB}, set(dir(self.paths)))

    def test_schema_dir_with_tables(self):
        self.assertIn(TNAME_EXPERIMENT, dir(self.paths.ISA))

    def test_table_dir_with_columns(self):
        self.assertLess({'Name', 'Amount', 'Time', 'Type'}, set(dir(self.paths.ISA.Experiment)))

    def test_dir_path(self):
        self.assertIn(TNAME_EXPERIMENT, dir(self.paths.ISA.Experiment.path))

    def test_dir_invalid_identifier(self):
        self.assertIn(INVALID_IDENTIFIER_FIXED, dir(self.project))
        self.assertIsNotNone(getattr(self.project, INVALID_IDENTIFIER_FIXED))

    def test_dir_conflicting_identifier(self):
        self.assertIn(CONFLICTING_IDENTIFIER_FIXED, dir(self.project))
        self.assertIsNotNone(getattr(self.project, CONFLICTING_IDENTIFIER))
        self.assertIsNotNone(getattr(self.project, CONFLICTING_IDENTIFIER_FIXED))

    def test_describe_schema(self):
        with self.assertWarns(DeprecationWarning):
            self.paths.schemas[SNAME_ISA].describe()

    def test_describe_table(self):
        with self.assertWarns(DeprecationWarning):
            self.paths.schemas[SNAME_ISA].tables[TNAME_EXPERIMENT].describe()

    def test_describe_column(self):
        with self.assertWarns(DeprecationWarning):
            self.paths.schemas[SNAME_ISA].tables[TNAME_EXPERIMENT].column_definitions['Name'].describe()

    def test_unfiltered_fetch(self):
        results = self.experiment.entities()
        self.assertEqual(len(results), TEST_EXP_MAX)

    def test_fetch_with_headers(self):
        headers = {'User-Agent': __name__ + '/' + __version__}
        results = self.experiment.entities().fetch(headers=headers)
        self.assertEqual(len(results), TEST_EXP_MAX)

    def test_fetch_with_limit(self):
        results = self.experiment.entities()
        limit = TEST_EXP_MAX / 5
        results.fetch(limit=limit)
        self.assertEqual(len(results), limit)

    def test_fetch_with_sort(self):
        results = self.experiment.entities()
        results.sort(self.experiment.column_definitions['Amount'])
        self.assertEqual(results[0]['Amount'], 0)

    def test_fetch_attributes_with_sort(self):
        results = self.experiment.attributes(self.experiment.RID, self.experiment.Amount)
        results.sort(self.experiment.Amount)
        self.assertEqual(results[0]['Amount'], 0)

    def test_fetch_all_attributes_with_sort(self):
        results = self.experiment.attributes(self.experiment)
        results.sort(self.experiment.Amount)
        self.assertEqual(results[0]['Amount'], 0)

    def test_fetch_all_attributes_with_sort_desc(self):
        results = self.experiment.attributes(self.experiment)
        results.sort(self.experiment.Amount.desc)
        self.assertEqual(results[0]['Amount'], TEST_EXP_MAX-1)

    def test_fetch_from_path_attributes_with_sort_on_talias(self):
        path = self.experiment.path
        results = path.Experiment.attributes(path.Experiment.RID, path.Experiment.Amount)
        results.sort(path.Experiment.Amount)
        self.assertEqual(results[0]['Amount'], 0)

    def test_fetch_from_path_attributes_with_sort_on_talias_desc(self):
        path = self.experiment.path
        results = path.Experiment.attributes(path.Experiment.RID, path.Experiment.Amount)
        results.sort(path.Experiment.Amount.desc)
        self.assertEqual(results[0]['Amount'], TEST_EXP_MAX-1)

    def test_fetch_from_path_all_attributes_with_sort_on_talias(self):
        path = self.experiment.path
        results = path.Experiment.attributes(*path.Experiment.column_definitions.values())
        results.sort(path.Experiment.Amount)
        self.assertEqual(results[0]['Amount'], 0)

    def test_fetch_from_path_all_attributes_with_sort_on_alias_desc(self):
        path = self.experiment.path
        results = path.Experiment.attributes(*path.Experiment.column_definitions.values())
        results.sort(path.Experiment.Amount.desc)
        self.assertEqual(results[0]['Amount'], TEST_EXP_MAX-1)

    def test_fetch_all_cols_with_talias(self):
        path = self.paths.schemas[SNAME_ISA].tables[TNAME_EXPERIMENT].alias('X').path
        results = path.attributes(path.X)
        result = results.fetch(limit=1)[0]
        self.assertIn('X:RID', result)
        self.assertIn('X:Name', result)

    def test_fetch_with_talias(self):
        path = self.paths.schemas[SNAME_ISA].tables[TNAME_EXPERIMENT].alias('X').path
        results = path.attributes(path.X.RID, path.X.Name.alias('typeName'))
        result = results.fetch(limit=1)[0]
        self.assertIn('RID', result)
        self.assertIn('typeName', result)

    def test_attribute_projection(self):
        results = self.experiment.attributes(
            self.experiment.column_definitions['Name'],
            self.experiment.column_definitions['Amount']
        )
        result = results.fetch(limit=1)[0]
        self.assertIn('Name', result)
        self.assertIn('Amount', result)

    def test_attribute_err_table_attr(self):
        table_attr = ['_name', '_schema']
        for attr in table_attr:
            with self.assertRaises(TypeError):
                self.experiment.attributes(getattr(self.experiment, attr))

    def test_update_err_no_targets(self):
        entities = [{'RID': 1234}]
        with self.assertRaises(ValueError):
            self.experiment.update(entities)

    def test_aggregate_w_invalid_attributes(self):
        with self.assertRaises(TypeError):
            self.experiment.aggregates(Min(self.experiment.column_definitions['Amount']))

    def test_aggregate_w_invalid_renames(self):
        with self.assertRaises(TypeError):
            self.experiment.aggregates(
                self.experiment.column_definitions['Name'],
                Min(self.experiment.column_definitions['Amount'])
            )

    def test_aggregate_fns(self):
        tests = [
            ('min_amount',      Min,    0),
            ('max_amount',      Max,    TEST_EXP_MAX-1),
            ('sum_amount',      Sum,    sum(range(TEST_EXP_MAX))),
            ('avg_amount',      Avg,    sum(range(TEST_EXP_MAX))/TEST_EXP_MAX),
            ('cnt_amount',      Cnt,    TEST_EXP_MAX),
            ('cnt_d_amount',    CntD,   TEST_EXP_MAX),
            ('array_amount',    Array,  list(range(TEST_EXP_MAX))),
            ('array_d_amount',  ArrayD, list(range(TEST_EXP_MAX)))
        ]
        for name, Fn, value in tests:
            with self.subTest(name=name):
                # results = self.experiment.aggregates(**{name: Fn(self.experiment.column_definitions['Amount'])})
                results = self.experiment.aggregates(Fn(self.experiment.column_definitions['Amount']).alias(name))
                result = results.fetch()[0]
                self.assertIn(name, result)
                self.assertEqual(result[name], value)

    def test_aggregate_w_2_fns(self):
        results = self.experiment.aggregates(
            Min(self.experiment.column_definitions['Amount']).alias('min_amount'),
            Max(self.experiment.column_definitions['Amount']).alias('max_amount')
        )
        result = results.fetch()[0]
        self.assertIn('min_amount', result)
        self.assertEqual(result['min_amount'], 0)
        self.assertIn('max_amount', result)
        self.assertEqual(result['max_amount'], TEST_EXP_MAX-1)

    def test_aggregate_fns_array_star(self):
        path = self.experiment.path
        tests = [
            ('array_table_star',  Array,  self.experiment, self.experiment),
            ('array_alias_star',  Array,  path,            path.Experiment),
            ('arrayd_table_star', ArrayD, self.experiment, self.experiment),
            ('arrayd_alias_star', ArrayD, path,            path.Experiment)
        ]
        for name, Fn, path, instance in tests:
            results = path.aggregates(Fn(instance).alias('arr'))
            with self.subTest(name=name):
                result = results.fetch()[0]
                self.assertIn('arr', result)
                self.assertEqual(len(result['arr']), TEST_EXP_MAX)
                self.assertIn('Time', result['arr'][0])

    def test_aggregate_fns_cnt_star(self):
        path = self.experiment.path
        tests = [
            ('cnt_table_star', Cnt, self.experiment, self.experiment),
            ('cnt_alias_star', Cnt, path,            path.Experiment)
        ]
        for name, Fn, path, instance in tests:
            results = path.aggregates(Fn(instance).alias('cnt'))
            with self.subTest(name=name):
                result = results.fetch()[0]
                self.assertIn('cnt', result)
                self.assertEqual(result['cnt'], TEST_EXP_MAX)

    def test_attributegroup_fns(self):
        tests = [
            ('one group key',     [self.experiment.column_definitions['Type']]),
            ('two group keys',    [self.experiment.column_definitions['Project_Num'], self.experiment.column_definitions['Type']]),
            ('aliased group key', [self.experiment.column_definitions['Type'].alias('The Type')])
        ]
        for test_name, group_key in tests:
            with self.subTest(name=test_name):
                self._do_attributegroup_fn_subtests(group_key)

    def _do_attributegroup_fn_subtests(self, group_key):
        """Helper method for running common attributegroup subtests for different group keys."""
        tests = [
            ('min_amount',      Min,    0),
            ('max_amount',      Max,    TEST_EXP_MAX-TEST_EXPTYPE_MAX),
            ('sum_amount',      Sum,    sum(range(0, TEST_EXP_MAX, TEST_EXPTYPE_MAX))),
            ('avg_amount',      Avg,    sum(range(0, TEST_EXP_MAX, TEST_EXPTYPE_MAX))/TEST_EXPTYPE_MAX),
            ('cnt_amount',      Cnt,    TEST_EXPTYPE_MAX),
            ('cnt_d_amount',    CntD,   TEST_EXPTYPE_MAX),
            ('array_amount',    Array,  list(range(0, TEST_EXP_MAX, TEST_EXPTYPE_MAX))),
            ('array_d_amount',  ArrayD, list(range(0, TEST_EXP_MAX, TEST_EXPTYPE_MAX)))
        ]
        for name, Fn, value in tests:
            with self.subTest(name=name):
                results = self.experiment.groupby(*group_key).attributes(
                    Fn(self.experiment.column_definitions['Amount']).alias(name)).sort(*group_key)

                result = results[0]
                self.assertEqual(len(results), TEST_EXPTYPE_MAX)
                self.assertTrue(all(key._name in result for key in group_key))
                self.assertIn(name, result)
                self.assertEqual(result[name], value)

    def test_attributegroup_w_bin(self):
        tests = [
            ('min/max given',     0,    TEST_EXP_MAX),
            ('min/max not given', None, None),
            ('min only given',    0,    None),
            ('max only given',    None, TEST_EXP_MAX)
        ]
        for testname, minval, maxval in tests:
            with self.subTest(name=testname):
                self._do_bin_subtests(minval, maxval)

    def _do_bin_subtests(self, minval, maxval):
        """Helper method for running common binning tests with & without min/max values."""
        new_name, bin_name = 'TheProj', 'ABin'
        nbins = int(TEST_EXP_MAX/20)
        group_key = [
            self.experiment.column_definitions['Project_Num'].alias(new_name),
            Bin(self.experiment.column_definitions['Amount'], nbins, minval=minval, maxval=maxval).alias(bin_name)
        ]
        tests = [
            ('min_amount',      Min,    lambda a, b: a >= b[1]),
            ('max_amount',      Max,    lambda a, b: a <= b[2]),
            ('sum_amount',      Sum,    lambda a, b: a >= b[1] + b[2]),
            ('avg_amount',      Avg,    lambda a, b: b[1] <= a <= b[2]),
            ('cnt_amount',      Cnt,    lambda a, b: a == TEST_EXP_MAX/nbins),
            ('cnt_d_amount',    CntD,   lambda a, b: a == TEST_EXP_MAX/nbins),
            ('array_amount',    Array,  lambda a, b: all(b[1] <= a_i <= b[2] for a_i in a)),
            ('array_d_amount',  ArrayD, lambda a, b: all(b[1] <= a_i <= b[2] for a_i in a))
        ]
        for name, Fn, compare in tests:
            with self.subTest(name=name):
                results = self.experiment.groupby(*group_key).attributes(
                    Fn(self.experiment.column_definitions['Amount']).alias(name)).fetch()

                self.assertTrue(all(key._name in results[0] for key in group_key))
                self.assertIn(name, results[0])
                for result in results:
                    bin = result[bin_name]
                    if not maxval and (bin[0] >= nbins):
                        # skip the last 2 bins when maxval was resolved; those bins are not aligned like the others
                        continue
                    self.assertTrue(compare(result[name], bin))

    def test_attributegroup_w_bin_sort(self):
        bin_name = 'bin'
        nbins = int(TEST_EXP_MAX/20)
        bin = Bin(self.experiment.column_definitions['Amount'], nbins, 0, TEST_EXP_MAX).alias(bin_name)
        bin_desc = bin.desc
        asc_fn = lambda n, a, b: a[n] <= b[n]
        desc_fn = lambda n, a, b: a[n] >= b[n]
        tests = [
            ('min_amount', Min, bin,      asc_fn),
            ('max_amount', Max, bin,      asc_fn),
            ('sum_amount', Sum, bin,      asc_fn),
            ('avg_amount', Avg, bin,      asc_fn),
            ('min_amount', Min, bin_desc, desc_fn),
            ('max_amount', Max, bin_desc, desc_fn),
            ('sum_amount', Sum, bin_desc, desc_fn),
            ('avg_amount', Avg, bin_desc, desc_fn)
        ]
        for name, Fn, sort_key, compfn in tests:
            with self.subTest(name=name):
                results = self.experiment.groupby(bin).attributes(
                    Fn(self.experiment.column_definitions['Amount']).alias(name)).sort(sort_key).fetch()

                self.assertIn(bin._name, results[0])
                self.assertIn(name, results[0])
                self.assertTrue(compfn(name, results[0], results[1]))

    def test_attributegroup_w_bin_resolution(self):
        binkey = self.experiment.column_definitions['Empty']
        binname = 'bin'
        tests = [
            ('min_max_valid', 0,    0,      True),
            ('max_invalid',   0,    None,   False),
            ('min_invalid',   None, 0,      False),
            ('both_invalid',  None, None,   False)
        ]
        for name, minval, maxval, valid in tests:
            def _do_query():
                bin = Bin(binkey, 10, minval, maxval).alias(binname)
                return self.experiment.groupby(bin).attributes(Avg(binkey).alias(name)).fetch()

            with self.subTest(name=name):
                if valid:
                    results = _do_query()
                    self.assertIn(binname, results[0])
                    self.assertIn(name, results[0])
                else:
                    with self.assertRaises(ValueError):
                        _do_query()

    def test_link_implicit(self):
        results = self.experiment.link(self.experiment_type).entities()
        self.assertEqual(TEST_EXPTYPE_MAX, len(results))

    def test_link_explicit_simple_key(self):
        results = self.experiment.link(
            self.experiment_type,
            on=(self.experiment.Type == self.experiment_type.ID)
        ).entities()
        self.assertEqual(TEST_EXPTYPE_MAX, len(results))

    def test_link_explicit_composite_key(self):
        path = self.experiment.link(
            self.project,
            on=(
                    (self.experiment.Project_Investigator == self.project.Investigator) &
                    (self.experiment.Project_Num == self.project.Num)
            )
        )
        results = path.entities()
        self.assertEqual(TEST_PROJ_MAX, len(results))

    def test_link_outbound_fkey(self):
        fkey_by_pk_table_name = {
            fkey.pk_table.name: fkey
            for fkey in self.model.schemas[SNAME_ISA].tables[TNAME_EXPERIMENT].foreign_keys
        }

        tests = [
            ('fkey-link-' + TNAME_PROJECT, fkey_by_pk_table_name[TNAME_PROJECT], self.project, TEST_PROJ_MAX),
            ('fkey-link-' + TNAME_EXPERIMENT_TYPE, fkey_by_pk_table_name[TNAME_EXPERIMENT_TYPE], self.project, TEST_EXPTYPE_MAX)
        ]

        for name, fkey, table, expected_results_len in tests:
            with self.subTest(name=name):
                results = self.experiment.link(table, on=fkey).entities()
                self.assertEqual(expected_results_len, len(results))

    def test_link_inbound_fkey(self):
        fkey_by_fk_table_name = {
            fkey.table.name: fkey
            for fkey in self.model.schemas[SNAME_VOCAB].tables[TNAME_EXPERIMENT_TYPE].referenced_by
        }

        tests = [
            ('fkey-link-' + TNAME_EXPERIMENT, fkey_by_fk_table_name[TNAME_EXPERIMENT], self.project, TEST_EXP_MAX)
        ]

        for name, fkey, table, expected_results_len in tests:
            with self.subTest(name=name):
                results = self.experiment_type.link(table, on=fkey).entities()
                self.assertEqual(expected_results_len, len(results))

    def test_filter_equality(self):
        results = self.experiment.filter(
            self.experiment.column_definitions['Name'] == TEST_EXP_NAME_FORMAT.format(1)
        ).entities()
        self.assertEqual(len(results), 1)

    def test_filter_inequality(self):
        results = self.experiment.filter(
            self.experiment.column_definitions['Amount'] < 10
        ).entities()
        self.assertEqual(len(results), 10)

    def test_filter_ciregexp(self):
        results = self.experiment.filter(
            self.experiment.column_definitions['Name'].ciregexp(TEST_EXP_NAME_FORMAT.format(0)[10:])
        ).entities()
        self.assertEqual(len(results), 1)

    def test_filter_negation(self):
        results = self.experiment.filter(
            ~ (self.experiment.column_definitions['Name'].ciregexp(TEST_EXP_NAME_FORMAT.format(0)[10:]))
        ).entities()
        self.assertEqual(len(results), TEST_EXP_MAX - 1)

    def test_filter_conjunction(self):
        results = self.experiment.filter(
            self.experiment.column_definitions['Name'].ciregexp(TEST_EXP_NAME_FORMAT.format(0)[10:])
            & (self.experiment.column_definitions['Amount'] == 0)
        ).entities()
        self.assertEqual(len(results), 1)

    def test_attribute_deprecated_rename(self):
        with self.assertRaises(TypeError):
            self.experiment.attributes(
                self.experiment.column_definitions['Name'],
                howmuch=self.experiment.column_definitions['Amount']
            )

    def test_attribute_rename(self):
        results = self.experiment.attributes(
            self.experiment.column_definitions['Name'],
            self.experiment.column_definitions['Amount'].alias('How many of them ?'),
            self.experiment.column_definitions['Project_Num'].alias('Project #')
        )
        result = results.fetch(limit=1)[0]
        self.assertIn('Name', result)
        self.assertIn('How many of them ?', result)
        self.assertIn('Project #', result)

    def test_attribute_rename_special_chars(self):
        # first test with only the `:` character present which would trigger a lexical error from ermrest
        special_character_out_alias = self.experiment._name + ':' + self.experiment.column_definitions['Name']._name
        results = self.experiment.attributes(self.experiment.column_definitions['Name'].alias(special_character_out_alias))
        result = results.fetch(limit=1)[0]
        self.assertIn(special_character_out_alias, result)

        # second test with url unsafe characters present which would trigger a bad request from the web server
        special_character_out_alias = SPECIAL_CHARACTERS
        results = self.experiment.attributes(self.experiment.column_definitions['Name'].alias(special_character_out_alias))
        result = results.fetch(limit=1)[0]
        self.assertIn(special_character_out_alias, result)

    def test_context(self):
        path = self.experiment.link(self.experiment_type)
        results = path.Experiment.entities()
        self.assertEqual(len(results), TEST_EXP_MAX)

    def test_path_table_instances(self):
        path = self.experiment.link(self.experiment_type)
        results = path.table_instances[TNAME_EXPERIMENT].entities()
        self.assertEqual(len(results), TEST_EXP_MAX)

    def test_path_project(self):
        path = self.experiment.link(self.experiment_type)
        results = path.Experiment.attributes(
            path.Experiment,
            path.Experiment_Type.column_definitions['URI'],
            path.Experiment_Type.column_definitions['Name'].alias('exptype')
        )
        result = results.fetch(limit=1)[0]
        self.assertIn('Experiment:Name', result)
        self.assertIn('Experiment:Time', result)
        self.assertIn('URI', result)
        self.assertIn('exptype', result)

    @unittest.skipUnless(HAS_PANDAS, "pandas library not available")
    def test_dataframe(self):
        results = self.experiment.entities()
        df = DataFrame(results)
        self.assertEqual(len(df), TEST_EXP_MAX)

    def test_insert_double_fetch(self):
        entities = _generate_experiment_entities(self.types, 2)
        results = self.experiment_copy.insert(entities)
        rows1 = results.fetch()
        rows2 = results.fetch()
        self.assertEqual(rows1, rows2)
        
    def test_insert_empty_entities(self):
        results = self.experiment_copy.insert(None)
        self.assertEqual(len(results), 0)
        results = self.experiment_copy.insert([])
        self.assertEqual(len(results), 0)

    def test_insert_entities_not_iterable(self):
        with self.assertRaises(TypeError):
            self.experiment_type.insert(1)

    def test_insert_entities0_not_dict(self):
        with self.assertRaises(TypeError):
            self.experiment_type.insert([1])
        with self.assertRaises(TypeError):
            self.experiment_type.insert('this is not a dict')

    def test_insert(self):
        results = self.experiment_copy.insert(_generate_experiment_entities(self.types, 10))
        self.assertEqual(len(results), 10)

    def test_insert_on_conflict_raise(self):
        entities = _generate_experiment_entities(self.types, 2)
        first = entities[0:1]
        results = self.experiment_copy.insert(first)
        self.assertEqual(len(results), 1)
        with self.assertRaises(DataPathException):
            self.experiment_copy.insert(entities)

    def test_insert_on_conflict_skip(self):
        entities = _generate_experiment_entities(self.types, 2)
        first = entities[0:1]
        results = self.experiment_copy.insert(first)
        self.assertEqual(len(results), 1)
        results = self.experiment_copy.insert(entities, on_conflict_skip=True)
        self.assertEqual(len(results), 1)

    def test_update(self):
        inserted = self.experiment_copy.insert(_generate_experiment_entities(self.types, 10))
        self.assertEqual(len(inserted), 10)
        # now change something in the first result
        updates = [dict(**inserted[0])]
        updates[0]['Name'] = '**CHANGED**'
        updated = self.experiment_copy.update(updates)
        self.assertEqual(len(updated), 1)
        self.assertEqual(inserted[0]['RID'], updated[0]['RID'])
        self.assertNotEqual(inserted[0]['Name'], updated[0]['Name'])

    def test_update_empty_entities(self):
        results = self.experiment_copy.update(None)
        self.assertEqual(len(results), 0)
        results = self.experiment_copy.update([])
        self.assertEqual(len(results), 0)

    def test_update_entities_not_iterable(self):
        with self.assertRaises(TypeError):
            self.experiment_type.update(1)

    def test_update_entities0_not_dict(self):
        with self.assertRaises(TypeError):
            self.experiment_type.update([1])
        with self.assertRaises(TypeError):
            self.experiment_type.update('this is not a dict')

    def test_delete_whole_path(self):
        self.experiment_copy.insert(_generate_experiment_entities(self.types, 10))
        self.assertEqual(len(self.experiment_copy.entities()), 10)
        self.experiment_copy.path.delete()
        self.assertEqual(len(self.experiment_copy.entities()), 0)

    def test_delete_filtered_path(self):
        self.experiment_copy.insert(_generate_experiment_entities(self.types, 10))
        expression = self.experiment_copy.column_definitions['Name'] == TEST_EXP_NAME_FORMAT.format(1)
        self.assertEqual(len(self.experiment_copy.filter(expression).entities()), 1)
        self.experiment_copy.filter(expression).delete()
        self.assertEqual(len(self.experiment_copy.filter(expression).entities()), 0)

    def test_delete_whole_table(self):
        self.experiment_copy.insert(_generate_experiment_entities(self.types, 10))
        self.assertEqual(len(self.experiment_copy.entities()), 10)
        self.experiment_copy.delete()
        self.assertEqual(len(self.experiment_copy.entities()), 0)

    def test_nondefaults(self):
        nondefaults = {'RID', 'RCB', 'RCT'}
        results = self.experiment.entities()
        self.assertEqual(len(results), TEST_EXP_MAX)
        entities_copy = self.experiment_copy.insert(results, nondefaults=nondefaults, add_system_defaults=False)
        self.assertEqual(len(results), len(entities_copy), 'entities not copied completely')
        ig = itemgetter(*nondefaults)
        for i in range(TEST_EXP_MAX):
            self.assertEqual(ig(results[i]), ig(entities_copy[i]), 'copied values do not match')

    def test_nondefaults_w_add_sys_defaults(self):
        nondefaults = {'RID', 'RCB', 'RCT'}
        results = self.experiment.entities()
        self.assertEqual(len(results), TEST_EXP_MAX)
        entities_copy = self.experiment_copy.insert(results, nondefaults=nondefaults)
        self.assertEqual(len(results), len(entities_copy), 'entities not copied completely')
        ig = itemgetter(*nondefaults)
        for i in range(TEST_EXP_MAX):
            self.assertEqual(ig(results[i]), ig(entities_copy[i]), 'copied values do not match')

    def test_deepcopy_of_paths(self):
        paths = [
            self.experiment.path,
            self.experiment.link(self.experiment_type),
            self.experiment.link(self.experiment_type, on=(self.experiment.Type == self.experiment_type.ID)),
            self.experiment.link(
                self.project,
                on=(
                        (self.experiment.Project_Investigator == self.project.Investigator) &
                        (self.experiment.Project_Num == self.project.Num)
                )
            ),
            self.project.filter(self.project.Num < 1000).link(self.experiment).link(self.experiment_type),
            self.experiment.alias('Exp').link(self.experiment_type.alias('ExpType')),
            self.experiment.filter(self.experiment.column_definitions['Name'] == TEST_EXP_NAME_FORMAT.format(1)),
            self.experiment.filter(self.experiment.column_definitions['Amount'] < 10),
            self.experiment.filter(
                self.experiment.column_definitions['Name'].ciregexp(TEST_EXP_NAME_FORMAT.format(0)[10:])
            ),
            self.experiment.filter(
                ~ (self.experiment.column_definitions['Name'].ciregexp(TEST_EXP_NAME_FORMAT.format(0)[10:]))
            ),
            self.experiment.filter(
                self.experiment.column_definitions['Name'].ciregexp(TEST_EXP_NAME_FORMAT.format(0)[10:])
                & (self.experiment.column_definitions['Amount'] == 0)
            )
        ]
        for path in paths:
            with self.subTest(name=path.uri):
                cp = deepcopy(path)
                self.assertNotEqual(path, cp)
                self.assertEqual(path.uri, cp.uri)

    def test_merge_paths(self):
        path1 = self.experiment.filter(self.experiment.Amount >= 0)
        path2 = self.experiment.link(self.experiment_type).filter(self.experiment_type.ID >= '0')
        path3 = self.experiment.link(self.project).filter(self.project.Num >= 0)
        original_uri = path1.uri

        # merge paths 1..3
        path1.merge(path2).merge(path3)
        self.assertNotEqual(path1.uri, original_uri, "Merged path's URI should have changed from its original URI")
        self.assertEqual(path1.context._name, path3.context._name, "Context of merged paths should equal far right-hand path's context")
        self.assertGreater(len(path1.Experiment.entities()), 0, "Should have returned results")

    def test_compose_paths(self):
        path1 = self.experiment.filter(self.experiment.Amount >= 0)
        path2 = self.experiment.link(self.experiment_type).filter(self.experiment_type.ID >= '0')
        path3 = self.experiment.link(self.project).filter(self.project.Num >= 0)
        original_uri = path1.uri

        # compose paths 1..3
        path = self.paths.compose(path1, path2, path3)
        self.assertNotEqual(path, path1, "Compose should have copied the first path rather than mutate it")
        self.assertNotEqual(path.uri, path1.uri, "Composed path URI should not match the first path URI")
        self.assertEqual(path1.uri, original_uri, "First path was changed")
        self.assertNotEqual(path.uri, original_uri, "Merged path's URI should have changed from its original URI")
        self.assertEqual(path.context._name, path3.context._name, "Context of composed paths should equal far right-hand path's context")
        self.assertGreater(len(path.Experiment.entities()), 0, "Should have returned results")

    def test_simple_denormalization(self):
        entities = self.experiment.entities()
        results = self.experiment.denormalize()
        self.assertEqual(len(entities), len(results))
        self.assertNotEqual(entities[0].keys(), results[0].keys())
        self.assertIn('Type', results[0])
        self.assertTrue(entities[0]['Type'].startswith('TEST:'))
        self.assertTrue(results[0]['Type'])
        self.assertFalse(results[0]['Type'].startswith('TEST:'))

    def test_simple_denormalization_w_entities(self):
        entities = self.experiment.entities()
        results = self.experiment.denormalize(heuristic=simple_denormalization_with_whole_entities)
        self.assertEqual(len(entities), len(results))
        self.assertLess(len(entities[0].keys()), len(results[0].keys()))
        self.assertIn('Experiment_Project Investigator_Project_Num_fkey', results[0])
        self.assertIsInstance(results[0]['Experiment_Project Investigator_Project_Num_fkey'], list)
        self.assertIsInstance(results[0]['Experiment_Project Investigator_Project_Num_fkey'][0], dict)
        self.assertIn('RID', results[0]['Experiment_Project Investigator_Project_Num_fkey'][0])


if __name__ == '__main__':
    unittest.main()
