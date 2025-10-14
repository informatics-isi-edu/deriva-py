# Tests for the hatrac store module.
#
# Environment variables:
#  DERIVA_PY_TEST_HOSTNAME: hostname of the test server
#  DERIVA_PY_TEST_CREDENTIAL: user credential, if none, it will attempt to get credential for given hostname (optional)
#  DERIVA_PY_TEST_VERBOSE: set for verbose logging output to stdout (optional)

import logging
import os
import unittest
from deriva.core import DerivaServer, get_credential

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
if os.getenv("DERIVA_PY_TEST_VERBOSE"):
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

hostname = os.getenv("DERIVA_PY_TEST_HOSTNAME")
credential = os.getenv("DERIVA_PY_TEST_CREDENTIAL") or get_credential(hostname)


@unittest.skipUnless(hostname, "Test host not specified")
class HatracStoreTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
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



import unittest


class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
