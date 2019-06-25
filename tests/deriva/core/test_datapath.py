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
import sys
from deriva.core import DerivaServer, get_credential, ermrest_model as _em
from deriva.core.datapath import DataPathException, Min, Max, Avg, Cnt, CntD, Array, ArrayD

# unittests did not support 'subTests' until 3.4
if sys.version_info[0] < 3 or sys.version_info[1] < 4:
    HAS_SUBTESTS = False
else:
    HAS_SUBTESTS = True

# unittests did not support 'assertWarns' until 3.2
if sys.version_info[0] < 3 or sys.version_info[1] < 2:
    HAS_ASSERTWARNS = False
else:
    HAS_ASSERTWARNS = True

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

    # create 'Project' table
    table_def = _em.Table.define(
        "Project",
        column_defs=[
            _em.Column.define(cname, ctype) for (cname, ctype) in [
                ('Investigator', _em.builtin_types.text),
                ('Num', _em.builtin_types.int4)
            ]
        ],
        key_defs=[
            _em.Key.define(['Investigator', 'Num'])
        ]
    )
    isa.create_table(catalog, table_def)

    # create 'Experiment' table
    table_def = _em.Table.define(
        "Experiment",
        column_defs=[
            _em.Column.define(cname, ctype) for (cname, ctype) in [
                ('Name', _em.builtin_types.text),
                ('Amount', _em.builtin_types.int4),
                ('Time', _em.builtin_types.timestamptz),
                ('Type', _em.builtin_types.text),
                ('Project_Investigator', _em.builtin_types.text),
                ('Project_Num', _em.builtin_types.int4)
            ]
        ],
        key_defs=[
            _em.Key.define(['Name'])
        ],
        fkey_defs=[
            _em.ForeignKey.define(['Type'], 'Vocab', 'Experiment_Type', ['ID']),
            _em.ForeignKey.define(['Project_Investigator', 'Project_Num'], 'ISA', 'Project', ['Investigator', 'Num'])
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
            "Type": types[i % TEST_EXPTYPE_MAX]['ID'],
            "Project_Investigator": TEST_PROJ_INVESTIGATOR,
            "Project_Num": TEST_PROJ_NUM
        }
        for i in range(count)
    ]


def populate_test_catalog(catalog):
    """Populate the test catalog."""
    paths = catalog.getPathBuilder()
    logger.debug("Inserting project...")
    logger.debug("Inserting experiment types...")
    proj_table = paths.schemas['ISA'].tables['Project']
    proj_table.insert([
        {"Investigator": TEST_PROJ_INVESTIGATOR, "Num": TEST_PROJ_NUM}
    ])
    type_table = paths.schemas['Vocab'].tables['Experiment_Type']
    types = type_table.insert([
        {"Name": "{}".format(name), "Description": "NA"} for name in range(TEST_EXPTYPE_MAX)
    ], defaults=['ID', 'URI'])
    logger.debug("Inserting experiments...")
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
        self.project = self.paths.schemas['ISA'].tables['Project']
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

    def test_catalog_dir_base(self):
        self.assertIn('schemas', dir(self.paths))

    def test_schema_dir_base(self):
        self.assertLess({'name', 'tables', 'describe'}, set(dir(self.paths.schemas['ISA'])))

    def test_datapath_dir_base(self):
        self.assertLess({'aggregates', 'attributegroups', 'attributes', 'context', 'delete', 'entities', 'filter',
                         'link', 'table_instances', 'uri'}, set(dir(self.paths.schemas['ISA'].tables['Experiment'].path)))

    def test_table_dir_base(self):
        self.assertLess({'aggregates', 'alias', 'attributegroups', 'attributes', 'catalog', 'describe', 'entities',
                         'filter', 'fqname', 'fromname', 'insert', 'instancename', 'link', 'name', 'path', 'sname',
                         'uname', 'update', 'uri'}, set(dir(self.paths.schemas['ISA'].tables['Experiment'])))

    def test_catalog_dir_with_schemas(self):
        self.assertLess({'ISA', 'Vocab'}, set(dir(self.paths)))

    def test_schema_dir_with_tables(self):
        self.assertIn('Experiment', dir(self.paths.ISA))

    def test_table_dir_with_columns(self):
        self.assertLess({'Name', 'Amount', 'Time', 'Type'}, set(dir(self.paths.ISA.Experiment)))

    def test_dir_path(self):
        self.assertIn('Experiment', dir(self.paths.ISA.Experiment.path))

    def test_describe_schema(self):
        self.assertTrue(self.paths.schemas['ISA'].describe())

    def test_describe_table(self):
        self.assertTrue(self.paths.schemas['ISA'].tables['Experiment'].describe())

    def test_describe_column(self):
        self.assertTrue(self.paths.schemas['ISA'].tables['Experiment'].column_definitions['Name'].describe())

    def test_unfiltered_fetch(self):
        results = self.experiment.entities()
        self.assertEqual(len(results), TEST_EXP_MAX)

    def test_fetch_with_limit(self):
        results = self.experiment.entities()
        limit = TEST_EXP_MAX / 5
        results.fetch(limit=limit)
        self.assertEqual(len(results), limit)

    def test_fetch_with_sort(self):
        results = self.experiment.entities()
        results.fetch(sort=[self.experiment.column_definitions['Amount']])
        self.assertEqual(results[0]['Amount'], 0)

    def test_fetch_attributes_with_sort(self):
        results = self.experiment.attributes(self.experiment.RID, self.experiment.Amount)
        results.fetch(sort=[self.experiment.Amount])
        self.assertEqual(results[0]['Amount'], 0)

    def test_fetch_all_attributes_with_sort(self):
        results = self.experiment.attributes(self.experiment)
        results.fetch(sort=[self.experiment.Amount])
        self.assertEqual(results[0]['Amount'], 0)

    def test_fetch_all_attributes_with_sort_desc(self):
        results = self.experiment.attributes(self.experiment)
        results.fetch(sort=[self.experiment.Amount.desc])
        self.assertEqual(results[0]['Amount'], TEST_EXP_MAX-1)

    def test_fetch_from_path_attributes_with_sort_on_alias(self):
        path = self.experiment.path
        results = path.Experiment.attributes(path.Experiment.RID, path.Experiment.Amount)
        results.fetch(sort=[path.Experiment.Amount])
        self.assertEqual(results[0]['Amount'], 0)

    def test_fetch_from_path_attributes_with_sort_on_alias_desc(self):
        path = self.experiment.path
        results = path.Experiment.attributes(path.Experiment.RID, path.Experiment.Amount)
        results.fetch(sort=[path.Experiment.Amount.desc])
        self.assertEqual(results[0]['Amount'], TEST_EXP_MAX-1)

    def test_fetch_from_path_all_attributes_with_sort_on_alias(self):
        path = self.experiment.path
        results = path.Experiment.attributes(*path.Experiment.column_definitions.values())
        results.fetch(sort=[path.Experiment.Amount])
        self.assertEqual(results[0]['Amount'], 0)

    def test_fetch_from_path_all_attributes_with_sort_on_alias_desc(self):
        path = self.experiment.path
        results = path.Experiment.attributes(*path.Experiment.column_definitions.values())
        results.fetch(sort=[path.Experiment.Amount.desc])
        self.assertEqual(results[0]['Amount'], TEST_EXP_MAX-1)

    def test_attribute_projection(self):
        results = self.experiment.attributes(
            self.experiment.column_definitions['Name'],
            self.experiment.column_definitions['Amount']
        )
        result = results.fetch(limit=1)[0]
        self.assertIn('Name', result)
        self.assertIn('Amount', result)

    def test_attribute_err_table_attr(self):
        table_attr = ['name', 'sname', 'catalog']
        for attr in table_attr:
            with self.assertRaises(ValueError):
                self.experiment.attributes(getattr(self.experiment, attr))

    def test_attribute_rename_err_table_attr(self):
        table_attr = ['name', 'sname', 'catalog']
        for attr in table_attr:
            with self.assertRaises(ValueError):
                self.experiment.attributes(**{attr: getattr(self.experiment, attr)})

    def test_attribute_err_no_targets(self):
        entities = [{'RID': 1234}]
        with self.assertRaises(ValueError):
            self.experiment.update(entities)

    @unittest.skipUnless(HAS_ASSERTWARNS, "This tests is not available unless running python 3.2+")
    def test_deprecated_entities_projection(self):
        with self.assertWarns(DeprecationWarning):
            self.experiment.entities(
                self.experiment.column_definitions['Name'],
                self.experiment.column_definitions['Amount']
            )

    def test_aggregate_w_invalid_attributes(self):
        with self.assertRaises(TypeError):
            self.experiment.aggregates(Min(self.experiment.column_definitions['Amount']))

    def test_aggregate_w_invalid_renames(self):
        with self.assertRaises(TypeError):
            self.experiment.aggregates(
                self.experiment.column_definitions['Name'],
                Min(self.experiment.column_definitions['Amount'])
            )

    @unittest.skipUnless(HAS_SUBTESTS, "This tests is not available unless running python 3.4+")
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
                results = self.experiment.aggregates(**{name: Fn(self.experiment.column_definitions['Amount'])})
                result = results.fetch()[0]
                self.assertIn(name, result)
                self.assertEqual(result[name], value)

    def test_aggregate_w_2_fns(self):
        results = self.experiment.aggregates(
            min_amount=Min(self.experiment.column_definitions['Amount']),
            max_amount=Max(self.experiment.column_definitions['Amount'])
        )
        result = results.fetch()[0]
        self.assertIn('min_amount', result)
        self.assertEqual(result['min_amount'], 0)
        self.assertIn('max_amount', result)
        self.assertEqual(result['max_amount'], TEST_EXP_MAX-1)

    @unittest.skipUnless(HAS_SUBTESTS, "This tests is not available unless running python 3.4+")
    def test_aggregate_fns_array_star(self):
        path = self.experiment.path
        tests = [
            ('array_table_star',  Array,  self.experiment, self.experiment),
            ('array_alias_star',  Array,  path,            path.Experiment),
            ('arrayd_table_star', ArrayD, self.experiment, self.experiment),
            ('arrayd_alias_star', ArrayD, path,            path.Experiment)
        ]
        for name, Fn, path, instance in tests:
            results = path.aggregates(arr=Fn(instance))
            with self.subTest(name=name):
                result = results.fetch()[0]
                self.assertIn('arr', result)
                self.assertEqual(len(result['arr']), TEST_EXP_MAX)
                self.assertIn('Time', result['arr'][0])

    @unittest.skipUnless(HAS_SUBTESTS, "This tests is not available unless running python 3.4+")
    def test_aggregate_fns_cnt_star(self):
        path = self.experiment.path
        tests = [
            ('cnt_table_star', Cnt, self.experiment, self.experiment),
            ('cnt_alias_star', Cnt, path,            path.Experiment)
        ]
        for name, Fn, path, instance in tests:
            results = path.aggregates(cnt=Fn(instance))
            with self.subTest(name=name):
                result = results.fetch()[0]
                self.assertIn('cnt', result)
                self.assertEqual(result['cnt'], TEST_EXP_MAX)

    @unittest.skipUnless(HAS_SUBTESTS, "This tests is not available unless running python 3.4+")
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
                results = self.experiment.attributegroups(group_key=self.experiment.column_definitions['Type'],
                                                    **{name: Fn(self.experiment.column_definitions['Amount'])}
                                                    ).fetch(sort=[group_key])
                result = results[0]
                self.assertIn(group_key.name, result)
                self.assertIn(name, result)
                self.assertEqual(result[name], value)

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

    def test_attribute_rename(self):
        results = self.experiment.attributes(
            self.experiment.column_definitions['Name'],
            howmuch=self.experiment.column_definitions['Amount']
        )
        result = results.fetch(limit=1)[0]
        self.assertIn('Name', result)
        self.assertIn('howmuch', result)

    def test_attribute_rename_special_chars(self):
        # first test with only the `:` character present which would trigger a lexical error from ermrest
        special_character_out_alias = self.experiment.name + ':' + self.experiment.column_definitions['Name'].name
        renames = {special_character_out_alias: self.experiment.column_definitions['Name']}
        results = self.experiment.attributes(**renames)
        result = results.fetch(limit=1)[0]
        self.assertIn(special_character_out_alias, result)

        # second test with url unsafe characters present which would trigger a bad request from the web server
        special_character_out_alias = '`~!@#$%^&*()_+-={}|[]\\;:"\',./<>?'
        renames = {special_character_out_alias: self.experiment.column_definitions['Name']}
        results = self.experiment.attributes(**renames)
        result = results.fetch(limit=1)[0]
        self.assertIn(special_character_out_alias, result)

    def test_context(self):
        path = self.experiment.link(self.experiment_type)
        results = path.Experiment.entities()
        self.assertEqual(len(results), TEST_EXP_MAX)

    def test_path_table_instances(self):
        path = self.experiment.link(self.experiment_type)
        results = path.table_instances['Experiment'].entities()
        self.assertEqual(len(results), TEST_EXP_MAX)

    def test_path_project(self):
        path = self.experiment.link(self.experiment_type)
        results = path.Experiment.attributes(
            path.Experiment,
            path.Experiment_Type.column_definitions['URI'],
            exptype=path.Experiment_Type.column_definitions['Name']
        )
        result = results.fetch(limit=1)[0]
        self.assertIn('Experiment:Name', result)
        self.assertIn('Experiment:Time', result)
        self.assertIn('URI', result)
        self.assertIn('exptype', result)

    @unittest.skipUnless(HAS_PANDAS, "pandas library not available")
    def test_dataframe(self):
        results = self.experiment.entities()
        df = results.dataframe
        self.assertEqual(len(df), TEST_EXP_MAX)

    def test_insert_empty_entities(self):
        results = self.experiment_copy.insert(None)
        self.assertEqual(len(results), 0)
        results = self.experiment_copy.insert([])
        self.assertEqual(len(results), 0)

    def test_insert_entities_not_iterable(self):
        with self.assertRaises(ValueError):
            self.experiment_type.insert(1)

    def test_insert_entities0_not_dict(self):
        with self.assertRaises(ValueError):
            self.experiment_type.insert([1])
        with self.assertRaises(ValueError):
            self.experiment_type.insert('this is not a dict')

    def test_insert(self):
        results = self.experiment_copy.insert(_generate_experiment_entities(self.types, 10))
        self.assertEqual(len(results), 10)

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
        with self.assertRaises(ValueError):
            self.experiment_type.update(1)

    def test_update_entities0_not_dict(self):
        with self.assertRaises(ValueError):
            self.experiment_type.update([1])
        with self.assertRaises(ValueError):
            self.experiment_type.update('this is not a dict')

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
