# Tests for the datapath module.
#
# Environment variables:
#  DERIVA_PY_TEST_HOSTNAME: hostname of the test server
#  DERIVA_PY_TEST_CREDENTIAL: user credential, if none, it will attempt to get credentail for given hostname
#  DERIVA_PY_TEST_VERBOSE: set for verbose logging output to stdout

import logging
import os
import unittest
from deriva.core import DerivaServer, get_credential, ermrest_model as _em

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
    isa.create_table(catalog, _em.Table.define(
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
    ))


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
    exp.insert([
        {
            "Name": TEST_EXP_NAME_FORMAT.format(i),
            "Amount": i,
            "Time": "2018-01-{}T01:00:00.0".format(1 + (i % 31)),
            "Type": types[i % 10]['ID']
        }
        for i in range(TEST_EXP_MAX)
    ])


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

    def test_dir_model(self):
        self.assertIn('ISA', dir(self.paths))

    def test_dir_schema(self):
        self.assertIn('Experiment', dir(self.paths.ISA))

    def test_dir_table(self):
        self.assertIn('Name', dir(self.paths.ISA.Experiment))

    def test_dir_path(self):
        self.assertIn('Experiment', dir(self.paths.ISA.Experiment.path))

    def test_describe_schema(self):
        self.assert_(self.paths.schemas['ISA'].describe())

    def test_describe_table(self):
        self.assert_(self.paths.schemas['ISA'].tables['Experiment'].describe())

    def test_describe_column(self):
        self.assert_(self.paths.schemas['ISA'].tables['Experiment'].column_definitions['Name'].describe())

    def test_unfiltered_fetch(self):
        entities = self.experiment.entities()
        self.assertEquals(len(entities), TEST_EXP_MAX)

    def test_fetch_with_limit(self):
        entities = self.experiment.entities()
        limit = TEST_EXP_MAX / 5
        entities.fetch(limit=limit)
        self.assertEquals(len(entities), limit)

    def test_attribute_projection(self):
        entities = self.experiment.entities(
            self.experiment.column_definitions['Name'],
            self.experiment.column_definitions['Amount']
        )
        entity = entities.fetch(limit=1)[0]
        self.assertIn('Name', entity)
        self.assertIn('Amount', entity)

    def test_link(self):
        entities = self.experiment.link(self.experiment_type).entities()
        self.assertEquals(len(entities), TEST_EXPTYPE_MAX)

    def test_filter_equality(self):
        entities = self.experiment.filter(
            self.experiment.column_definitions['Name'] == TEST_EXP_NAME_FORMAT.format(1)
        ).entities()
        self.assertEquals(len(entities), 1)

    def test_filter_inequality(self):
        entities = self.experiment.filter(
            self.experiment.column_definitions['Amount'] < 10
        ).entities()
        self.assertEquals(len(entities), 10)

    def test_filter_ciregexp(self):
        entities = self.experiment.filter(
            self.experiment.column_definitions['Name'].ciregexp(TEST_EXP_NAME_FORMAT.format(0)[10:])
        ).entities()
        self.assertEquals(len(entities), 1)

    def test_filter_negation(self):
        entities = self.experiment.filter(
            ~ (self.experiment.column_definitions['Name'].ciregexp(TEST_EXP_NAME_FORMAT.format(0)[10:]))
        ).entities()
        self.assertEquals(len(entities), TEST_EXP_MAX - 1)

    def test_filter_conjunction(self):
        entities = self.experiment.filter(
            self.experiment.column_definitions['Name'].ciregexp(TEST_EXP_NAME_FORMAT.format(0)[10:])
            & (self.experiment.column_definitions['Amount'] == 0)
        ).entities()
        self.assertEquals(len(entities), 1)

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
        self.assertEquals(len(entities), TEST_EXP_MAX)

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
