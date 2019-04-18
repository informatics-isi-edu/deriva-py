# Tests for the datapath module.
#
# Environment variables:
#  DERIVA_PY_TEST_HOSTNAME: hostname of the test server
#  DERIVA_PY_TEST_CREDENTIAL: user credential, if none, it will attempt to get credentail for given hostname
#  DERIVA_PY_TEST_VERBOSE: set for verbose logging output to stdout

import logging
from operator import itemgetter
import os
import unittest
from deriva.core import DerivaServer, get_credential, ermrest_model as _em
from deriva.core.datapath import DataPathException, Min, Max, Avg, Cnt, CntD, Array, ArrayD

try:
    from pandas import DataFrame
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

TEST_EXP_MAX = 100
TEST_EXPTYPE_MAX = 10
TEST_EXP_NAME_FORMAT = "experiment-{}"

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
    vocab = model.create_schema(catalog, _em.Schema.define("Vocab"))
    vocab.create_table(catalog, _em.Table.define_vocabulary("Experiment_Type", "TEST:{RID}"))
    isa = model.create_schema(catalog, _em.Schema.define("ISA"))

    # create 'Experiment' table
    table_def = _em.Table.define(
        "Experiment",
        column_defs=[
            _em.Column.define(cname, ctype) for (cname, ctype) in [
                ('Name', _em.builtin_types.text),
                ('Amount', _em.builtin_types.int4),
                ('Time', _em.builtin_types.timestamptz),
                ('Type', _em.builtin_types.text)
            ]
        ],
        key_defs=[
            _em.Key.define(['Name'])
        ],
        fkey_defs=[
            _em.ForeignKey.define(['Type'], 'Vocab', 'Experiment_Type', ['ID'])
        ]
    )
    isa.create_table(catalog, table_def)

    # create copy of 'Experiment' table
    table_def['table_name'] = 'Experiment_Copy'
    isa.create_table(catalog, table_def)


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
            "Type": types[i % TEST_EXPTYPE_MAX]['ID']
        }
        for i in range(count)
    ]


def populate_test_catalog(catalog):
    """Populate the test catalog."""
    paths = catalog.getPathBuilder()
    logger.debug("Insert experiment types")
    type_table = paths.schemas['Vocab'].tables['Experiment_Type']
    types = type_table.insert([
        {"Name": "{}".format(name), "Description": "NA"} for name in range(TEST_EXPTYPE_MAX)
    ], defaults=['ID', 'URI'])
    logger.debug("Inserting experiments")
    exp = paths.schemas['ISA'].tables['Experiment']
    exp.insert(_generate_experiment_entities(types, TEST_EXP_MAX))


@unittest.skipUnless(hostname, "Test host not specified")
class DatapathTests (unittest.TestCase):
    catalog = None

    @classmethod
    def setUpClass(cls):
        logger.debug("setupUpClass begin")
        credential = os.getenv("DERIVA_PY_TEST_CREDENTIAL") or get_credential(hostname)
        server = DerivaServer('https', hostname, credential)
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
        self.experiment = self.paths.schemas['ISA'].tables['Experiment']
        self.experiment_type = self.paths.schemas['Vocab'].tables['Experiment_Type']
        self.experiment_copy = self.paths.schemas['ISA'].tables['Experiment_Copy']
        self.types = list(self.experiment_type.entities())

    def tearDown(self):
        try:
            self.experiment_copy.path.delete()
        except DataPathException:
            # suppresses 404 errors when the table is empty
            pass

    def test_dir_model(self):
        self.assertIn('ISA', dir(self.paths))

    def test_dir_schema(self):
        self.assertIn('Experiment', dir(self.paths.ISA))

    def test_dir_table(self):
        self.assertIn('Name', dir(self.paths.ISA.Experiment))

    def test_dir_path(self):
        self.assertIn('Experiment', dir(self.paths.ISA.Experiment.path))

    def test_describe_schema(self):
        self.assertTrue(self.paths.schemas['ISA'].describe())

    def test_describe_table(self):
        self.assertTrue(self.paths.schemas['ISA'].tables['Experiment'].describe())

    def test_describe_column(self):
        self.assertTrue(self.paths.schemas['ISA'].tables['Experiment'].column_definitions['Name'].describe())

    def test_unfiltered_fetch(self):
        entities = self.experiment.entities()
        self.assertEqual(len(entities), TEST_EXP_MAX)

    def test_fetch_with_limit(self):
        entities = self.experiment.entities()
        limit = TEST_EXP_MAX / 5
        entities.fetch(limit=limit)
        self.assertEqual(len(entities), limit)

    def test_attribute_projection(self):
        entities = self.experiment.entities(
            self.experiment.column_definitions['Name'],
            self.experiment.column_definitions['Amount']
        )
        entity = entities.fetch(limit=1)[0]
        self.assertIn('Name', entity)
        self.assertIn('Amount', entity)

    def test_aggregate_w_invalid_attributes(self):
        with self.assertRaises(ValueError):
            self.experiment.entities(Min(self.experiment.column_definitions['Amount']))

    def test_aggregate_w_invalid_renames(self):
        with self.assertRaises(ValueError):
            self.experiment.entities(
                self.experiment.column_definitions['Name'],
                Min(self.experiment.column_definitions['Amount'])
            )

    def test_aggregate_fns(self):
        tests = [
            ('min_amount',      Min,    0),
            ('max_amount',      Max,    TEST_EXP_MAX-1),
            ('avg_amount',      Avg,    sum(range(TEST_EXP_MAX))/TEST_EXP_MAX),
            ('cnt_amount',      Cnt,    TEST_EXP_MAX),
            ('cnt_d_amount',    CntD,   TEST_EXP_MAX),
            ('array_amount',    Array,  list(range(TEST_EXP_MAX))),
            ('array_d_amount',  ArrayD, list(range(TEST_EXP_MAX)))
        ]
        for name, Fn, value in tests:
            with self.subTest(name=name):
                entities = self.experiment.entities(**{name: Fn(self.experiment.column_definitions['Amount'])})
                entity = entities.fetch()[0]
                self.assertIn(name, entity)
                self.assertEqual(entity[name], value)

    def test_aggregate_w_2_fns(self):
        entities = self.experiment.entities(
            min_amount=Min(self.experiment.column_definitions['Amount']),
            max_amount=Max(self.experiment.column_definitions['Amount'])
        )
        entity = entities.fetch()[0]
        self.assertIn('min_amount', entity)
        self.assertEqual(entity['min_amount'], 0)
        self.assertIn('max_amount', entity)
        self.assertEqual(entity['max_amount'], TEST_EXP_MAX-1)

    def test_aggregate_fns_array_star(self):
        path = self.experiment.path
        tests = [
            ('array_table_star',  Array,  self.experiment, self.experiment),
            ('array_alias_star',  Array,  path,            path.Experiment),
            ('arrayd_table_star', ArrayD, self.experiment, self.experiment),
            ('arrayd_alias_star', ArrayD, path,            path.Experiment)
        ]
        for name, Fn, path, instance in tests:
            entities = path.entities(arr=Fn(instance))
            with self.subTest(name=name):
                entity = entities.fetch()[0]
                self.assertIn('arr', entity)
                self.assertEqual(len(entity['arr']), TEST_EXP_MAX)
                self.assertIn('Time', entity['arr'][0])

    def test_aggregate_fns_cnt_star(self):
        path = self.experiment.path
        tests = [
            ('cnt_table_star', Cnt, self.experiment, self.experiment),
            ('cnt_alias_star', Cnt, path,            path.Experiment)
        ]
        for name, Fn, path, instance in tests:
            entities = path.entities(cnt=Fn(instance))
            with self.subTest(name=name):
                entity = entities.fetch()[0]
                self.assertIn('cnt', entity)
                self.assertEqual(entity['cnt'], TEST_EXP_MAX)

    def test_attributegroup_fns(self):
        group_key = self.experiment.column_definitions['Type']
        tests = [
            ('min_amount',      Min,    0),
            ('max_amount',      Max,    TEST_EXP_MAX-TEST_EXPTYPE_MAX),
            ('avg_amount',      Avg,    sum(range(0, TEST_EXP_MAX, TEST_EXPTYPE_MAX))/TEST_EXPTYPE_MAX),
            ('cnt_amount',      Cnt,    TEST_EXPTYPE_MAX),
            ('cnt_d_amount',    CntD,   TEST_EXPTYPE_MAX),
            ('array_amount',    Array,  list(range(0, TEST_EXP_MAX, TEST_EXPTYPE_MAX))),
            ('array_d_amount',  ArrayD, list(range(0, TEST_EXP_MAX, TEST_EXPTYPE_MAX)))
        ]
        for name, Fn, value in tests:
            with self.subTest(name=name):
                entities = self.experiment.entities(group_key=self.experiment.column_definitions['Type'],
                                                    **{name: Fn(self.experiment.column_definitions['Amount'])}
                                                    ).fetch(sort=[group_key])
                entity = entities[0]
                self.assertIn(group_key.name, entity)
                self.assertIn(name, entity)
                self.assertEqual(entity[name], value)

    def test_link(self):
        entities = self.experiment.link(self.experiment_type).entities()
        self.assertEqual(len(entities), TEST_EXPTYPE_MAX)

    def test_filter_equality(self):
        entities = self.experiment.filter(
            self.experiment.column_definitions['Name'] == TEST_EXP_NAME_FORMAT.format(1)
        ).entities()
        self.assertEqual(len(entities), 1)

    def test_filter_inequality(self):
        entities = self.experiment.filter(
            self.experiment.column_definitions['Amount'] < 10
        ).entities()
        self.assertEqual(len(entities), 10)

    def test_filter_ciregexp(self):
        entities = self.experiment.filter(
            self.experiment.column_definitions['Name'].ciregexp(TEST_EXP_NAME_FORMAT.format(0)[10:])
        ).entities()
        self.assertEqual(len(entities), 1)

    def test_filter_negation(self):
        entities = self.experiment.filter(
            ~ (self.experiment.column_definitions['Name'].ciregexp(TEST_EXP_NAME_FORMAT.format(0)[10:]))
        ).entities()
        self.assertEqual(len(entities), TEST_EXP_MAX - 1)

    def test_filter_conjunction(self):
        entities = self.experiment.filter(
            self.experiment.column_definitions['Name'].ciregexp(TEST_EXP_NAME_FORMAT.format(0)[10:])
            & (self.experiment.column_definitions['Amount'] == 0)
        ).entities()
        self.assertEqual(len(entities), 1)

    def test_attribute_rename(self):
        entities = self.experiment.entities(
            self.experiment.column_definitions['Name'],
            howmuch=self.experiment.column_definitions['Amount']
        )
        entity = entities.fetch(limit=1)[0]
        self.assertIn('Name', entity)
        self.assertIn('howmuch', entity)

    def test_context(self):
        path = self.experiment.link(self.experiment_type)
        entities = path.Experiment.entities()
        self.assertEqual(len(entities), TEST_EXP_MAX)

    def test_path_project(self):
        path = self.experiment.link(self.experiment_type)
        entities = path.Experiment.entities(
            path.Experiment,
            path.Experiment_Type.column_definitions['URI'],
            exptype=path.Experiment_Type.column_definitions['Name']
        )
        entity = entities.fetch(limit=1)[0]
        self.assertIn('Experiment:Name', entity)
        self.assertIn('Experiment:Time', entity)
        self.assertIn('URI', entity)
        self.assertIn('exptype', entity)

    @unittest.skipUnless(HAS_PANDAS, "pandas library not available")
    def test_dataframe(self):
        entities = self.experiment.entities()
        df = entities.dataframe
        self.assertEqual(len(df), TEST_EXP_MAX)

    def test_insert_empty_entities(self):
        entities = self.experiment_copy.insert(None)
        self.assertEqual(len(entities), 0)
        entities = self.experiment_copy.insert([])
        self.assertEqual(len(entities), 0)

    def test_insert_entities_not_iterable(self):
        with self.assertRaises(ValueError):
            self.experiment_type.insert(1)

    def test_insert_entities0_not_dict(self):
        with self.assertRaises(ValueError):
            self.experiment_type.insert([1])
        with self.assertRaises(ValueError):
            self.experiment_type.insert('this is not a dict')

    def test_insert(self):
        entities = self.experiment_copy.insert(_generate_experiment_entities(self.types, 10))
        self.assertEqual(len(entities), 10)

    def test_update(self):
        inserted = self.experiment_copy.insert(_generate_experiment_entities(self.types, 10))
        self.assertEqual(len(inserted), 10)
        # now change something in the first entity
        updates = [dict(**inserted[0])]
        updates[0]['Name'] = '**CHANGED**'
        updated = self.experiment_copy.update(updates)
        self.assertEqual(len(updated), 1)
        self.assertEqual(inserted[0]['RID'], updated[0]['RID'])
        self.assertNotEqual(inserted[0]['Name'], updated[0]['Name'])

    def test_update_empty_entities(self):
        entities = self.experiment_copy.update(None)
        self.assertEqual(len(entities), 0)
        entities = self.experiment_copy.update([])
        self.assertEqual(len(entities), 0)

    def test_update_entities_not_iterable(self):
        with self.assertRaises(ValueError):
            self.experiment_type.update(1)

    def test_update_entities0_not_dict(self):
        with self.assertRaises(ValueError):
            self.experiment_type.update([1])
        with self.assertRaises(ValueError):
            self.experiment_type.update('this is not a dict')

    def test_nondefaults(self):
        nondefaults = {'RID', 'RCB', 'RCT'}
        entities = self.experiment.entities()
        self.assertEqual(len(entities), TEST_EXP_MAX)
        entities_copy = self.experiment_copy.insert(entities, nondefaults=nondefaults, add_system_defaults=False)
        self.assertEqual(len(entities), len(entities_copy), 'entities not copied completely')
        ig = itemgetter(*nondefaults)
        for i in range(TEST_EXP_MAX):
            self.assertEqual(ig(entities[i]), ig(entities_copy[i]), 'copied values do not match')

    def test_nondefaults_w_add_sys_defaults(self):
        nondefaults = {'RID', 'RCB', 'RCT'}
        entities = self.experiment.entities()
        self.assertEqual(len(entities), TEST_EXP_MAX)
        entities_copy = self.experiment_copy.insert(entities, nondefaults=nondefaults)
        self.assertEqual(len(entities), len(entities_copy), 'entities not copied completely')
        ig = itemgetter(*nondefaults)
        for i in range(TEST_EXP_MAX):
            self.assertEqual(ig(entities[i]), ig(entities_copy[i]), 'copied values do not match')
