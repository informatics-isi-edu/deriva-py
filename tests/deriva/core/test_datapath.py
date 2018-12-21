import logging
import os
import unittest
from deriva.core import DerivaServer, get_credential, ermrest_model as _em

TEST_HOSTNAME = os.getenv("DERIVA_PY_TEST_HOSTNAME")
TEST_SNAME = "test_datapath"
TEST_TNAME = "test1"

logger = logging.getLogger(__name__)
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


@unittest.skipUnless(TEST_HOSTNAME, "Test host not specified")
class DatapathTests (unittest.TestCase):
    catalog = None

    @classmethod
    def setUpClass(cls):
        logger.debug("setupUpClass begin")
        credentials = get_credential(TEST_HOSTNAME)
        server = DerivaServer('https', TEST_HOSTNAME, credentials)
        cls.catalog = server.create_ermrest_catalog()
        try:
            define_test_schema(cls.catalog)
        except Exception:
            # If this fails, delete catalog and re-raise exception
            cls.catalog.delete_ermrest_catalog(really=True)
            raise
        logger.debug("setupUpClass done")

    @classmethod
    def tearDownClass(cls):
        logger.debug("tearDownClass begin")
        cls.catalog.delete_ermrest_catalog(really=True)
        logger.debug("tearDownClass done")

    def test_A(self):
        logger.debug("running test A")

    def test_B(self):
        logger.debug("running test B")
