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
from deriva.core import DerivaServer, get_credential, ermrest_model

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
        schema = self.model.create_schema(ermrest_model.Schema.define('schema_with_fkeys'))
        schema.create_table(ermrest_model.Table.define(
            'parent',
            column_defs=[
                ermrest_model.Column.define('id', ermrest_model.builtin_types.text)
            ],
            key_defs=[
                ermrest_model.Key.define(['id'], constraint_name='parent_id_key')
                # ermrest_model.Key.define(['id'], constraint_names=[[schema.name, 'parent_id_key']])
            ]
        ))
        schema.create_table(ermrest_model.Table.define(
            'child',
            column_defs=[
                ermrest_model.Column.define('parent_id', ermrest_model.builtin_types.text)
            ],
            fkey_defs=[
                ermrest_model.ForeignKey.define(
                    ['parent_id'], 'schema_with_fkeys', 'parent', ['id']
                )
            ]
        ))
        # refresh the local state of the model
        self.model = self.catalog.getCatalogModel()

    def test_key_drop_cascading(self):
        self._create_schema_with_fkeys()
        schema = self.model.schemas['schema_with_fkeys']
        self.model.schemas['schema_with_fkeys'].tables['parent'].keys[(schema, 'parent_id_key')].drop(cascade=True)

    def test_column_drop_cascading(self):
        self._create_schema_with_fkeys()
        self.model.schemas['schema_with_fkeys'].tables['parent'].columns['id'].drop(cascade=True)

    def test_table_drop_cascading(self):
        self._create_schema_with_fkeys()
        self.model.schemas['schema_with_fkeys'].tables['parent'].drop(cascade=True)

    def test_schema_drop_cascading(self):
        self._create_schema_with_fkeys()
        self.model.schemas['schema_with_fkeys'].drop(cascade=True)


if __name__ == '__main__':
    unittest.main()
