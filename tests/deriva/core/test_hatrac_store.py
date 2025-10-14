# Tests for the hatrac store module.
#
# Environment variables:
#  DERIVA_PY_TEST_HOSTNAME: hostname of the test server
#  DERIVA_PY_TEST_CREDENTIAL: user credential, if none, it will attempt to get credential for given hostname (optional)
#  DERIVA_PY_TEST_VERBOSE: set for verbose logging output to stdout (optional)

import io
import logging
import os
import tempfile
import unittest
import uuid
from deriva.core import get_credential, HatracStore
from deriva.core.utils import hash_utils as hu

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG if os.getenv("DERIVA_PY_TEST_VERBOSE") else logging.INFO)
hostname = os.getenv("DERIVA_PY_TEST_HOSTNAME")
credential = os.getenv("DERIVA_PY_TEST_CREDENTIAL") or get_credential(hostname)


@unittest.skipUnless(hostname, "Test host not specified")
class HatracStoreTestCase(unittest.TestCase):

    base_path = '/hatrac/' + str(uuid.uuid4())

    @classmethod
    def setUpClass(cls):
        hatrac = HatracStore('https', hostname, credentials=credential)
        hatrac.create_namespace(cls.base_path)

    @classmethod
    def tearDownClass(cls):
        hatrac = HatracStore('https', hostname, credentials=credential)
        hatrac.delete_namespace(cls.base_path)

    def setUp(self):
        self.hatrac = HatracStore('https', hostname, credentials=credential)

    def tearDown(self):
        self.hatrac = None

    def test_namespace_operations(self):
        test_path = self.base_path + '/ns_test'
        self.assertIsNone(self.hatrac.create_namespace(test_path))
        self.assertIn(test_path, self.hatrac.retrieve_namespace(self.base_path))
        self.assertListEqual(self.hatrac.retrieve_namespace(test_path), [])
        self.assertTrue(self.hatrac.is_valid_namespace(test_path))
        self.assertIsNone(self.hatrac.delete_namespace(test_path))
        self.assertNotIn(test_path, self.hatrac.retrieve_namespace(self.base_path))

    def test_object_operations(self):
        test_path = self.base_path + '/obj_test'
        content = b'temporary file contents ' * 100
        self.assertTrue(self.hatrac.put_obj(test_path, io.BytesIO(content)).startswith(test_path))
        self.assertEqual(self.hatrac.get_obj(test_path).content, content)
        self.assertTrue(
            self.hatrac.content_equals(test_path, md5=hu.compute_hashes(io.BytesIO(content), hashes=['md5'])['md5'][1])
        )
        self.assertIsNone(self.hatrac.del_obj(test_path))

    def test_acl_operations(self):
        access = 'create'
        role = 'test-role'
        self.assertIsNone(self.hatrac.set_acl(self.base_path, access, [role], add_role=True))
        self.assertIn(role, self.hatrac.get_acl(self.base_path, access=access)[access])
        self.assertIsNone(self.hatrac.del_acl(self.base_path, access, role=role))
        self.assertNotIn(role, self.hatrac.get_acl(self.base_path, access=access)[access])

    def test_chunked_upload(self):
        test_path = self.base_path + '/chunk_test'
        chunk_size = 1024
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(b'temp ' * chunk_size)
            temp_file.seek(0)
            self.assertTrue(
                self.hatrac.put_loc(test_path, temp_file.name, chunked=True, chunk_size=chunk_size).startswith(test_path)
            )
            temp_file.seek(0)
            self.assertTrue(
                self.hatrac.content_equals(test_path, md5=hu.compute_hashes(temp_file, hashes=['md5'])['md5'][1])
            )
        self.assertIsNone(self.hatrac.del_obj(test_path))


if __name__ == '__main__':
    unittest.main()
