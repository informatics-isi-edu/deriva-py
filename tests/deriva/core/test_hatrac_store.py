# Tests for the hatrac store module.
#
# Environment variables:
#  DERIVA_PY_TEST_HOSTNAME: hostname of the test server
#  DERIVA_PY_TEST_CREDENTIAL: user credential, if none, it will attempt to get credential for given hostname (optional)
#  DERIVA_PY_TEST_VERBOSE: set for verbose logging output to stdout (optional)

import io
import logging
import os
import requests
import tempfile
import unittest
import uuid
from deriva.core import get_credential, HatracStore
from deriva.core.utils import hash_utils as hu

HOSTNAME = os.getenv("DERIVA_PY_TEST_HOSTNAME")
CREDENTIAL = os.getenv("DERIVA_PY_TEST_CREDENTIAL") or get_credential(HOSTNAME)
CONTENT = b'temporary file contents ' * 100
CONTENT_MD5 = hu.compute_hashes(io.BytesIO(CONTENT), hashes=['md5'])['md5'][1]

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG if os.getenv("DERIVA_PY_TEST_VERBOSE") else logging.INFO)


@unittest.skipUnless(HOSTNAME, "Test host not specified")
class HatracStoreTestCase(unittest.TestCase):

    base_path = '/hatrac/' + str(uuid.uuid4())

    @classmethod
    def setUpClass(cls):
        hatrac = HatracStore('https', HOSTNAME, credentials=CREDENTIAL)
        hatrac.create_namespace(cls.base_path)

    @classmethod
    def tearDownClass(cls):
        hatrac = HatracStore('https', HOSTNAME, credentials=CREDENTIAL)
        hatrac.delete_namespace(cls.base_path)

    def setUp(self):
        self.hatrac = HatracStore('https', HOSTNAME, credentials=CREDENTIAL)

    def tearDown(self):
        self.hatrac = None

    def test_namespace_operations(self):
        test_path = self.base_path + '/namespace_test'
        r = self.hatrac.create_namespace(test_path)
        self.assertIsNone(r)
        r = self.hatrac.retrieve_namespace(self.base_path)
        self.assertIn(test_path, r)  # new path should be 'in' parent's set of children
        r = self.hatrac.retrieve_namespace(test_path)
        self.assertListEqual(r, [])  # new path should have empty contents
        r = self.hatrac.is_valid_namespace(test_path)
        self.assertTrue(r)
        r = self.hatrac.delete_namespace(test_path)
        self.assertIsNone(r)
        r = self.hatrac.retrieve_namespace(self.base_path)
        self.assertNotIn(test_path, r)  # new path should be gone from parent's set of children

    def test_object_operations(self):
        test_path = self.base_path + '/basic_object_test/obj1'
        versioned_url = self.hatrac.put_obj(test_path, io.BytesIO(CONTENT))
        self.assertTrue(versioned_url.startswith(test_path))
        r = self.hatrac.get_obj(versioned_url)
        self.assertEqual(r.content, CONTENT)
        r = self.hatrac.content_equals(versioned_url, md5=CONTENT_MD5)
        self.assertTrue(r)
        r = self.hatrac.del_obj(versioned_url)
        self.assertIsNone(r)
        r = self.hatrac.del_obj(test_path)
        self.assertIsNone(r)

    def test_acl_operations(self):
        access = 'create'
        role = 'dummy-role'
        r = self.hatrac.set_acl(self.base_path, access, [role], add_role=True)
        self.assertIsNone(r)
        r = self.hatrac.get_acl(self.base_path, access=access)
        self.assertIn(role, r[access])  # added role should be in acl
        r = self.hatrac.del_acl(self.base_path, access, role=role)
        self.assertIsNone(r)
        r = self.hatrac.get_acl(self.base_path, access=access)
        self.assertNotIn(role, r[access])  # removed role should not be in acl

    def test_chunked_upload(self):
        test_path = self.base_path + '/chunk_upload_test/obj1'
        chunk_size = 1024
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(b'temp ' * chunk_size)
            temp_file.seek(0)
            versioned_url = self.hatrac.put_loc(test_path, temp_file.name, chunked=True, chunk_size=chunk_size)
            self.assertTrue(versioned_url.startswith(test_path))
            temp_file.seek(0)
            expected_md5 = hu.compute_hashes(temp_file, hashes=['md5'])['md5'][1]
            r = self.hatrac.content_equals(versioned_url, md5=expected_md5)
            self.assertTrue(r)
        r = self.hatrac.del_obj(versioned_url)
        self.assertIsNone(r)
        r = self.hatrac.del_obj(test_path)
        self.assertIsNone(r)

    def _do_rename_test(
            self,
            base_path,
            before_rename_callback_fn=None,
            after_rename_callback_fn=None,
            copy_acls=None
    ):
        """Helper method to allow multiple variations on rename test with callbacks to modify behavior."""
        # create source object
        src1_path = base_path + '/src1'
        src1_path_ver = self.hatrac.put_obj(src1_path, io.BytesIO(CONTENT))
        self.assertTrue(src1_path_ver.startswith(src1_path))
        # optionally, modify source
        if before_rename_callback_fn:
            before_rename_callback_fn(src1_path, src1_path_ver)
        # test renaming object
        ren1_path = base_path + '/ren1'
        ren1_path_ver = self.hatrac.rename_obj(src1_path, ren1_path, copy_acls=copy_acls)
        self.assertTrue(ren1_path_ver.startswith(ren1_path))
        # test that contents match
        r = self.hatrac.get_obj(ren1_path_ver)
        self.assertEqual(r.content, CONTENT)
        # test that md5 sum matches header
        r = self.hatrac.content_equals(ren1_path_ver, md5=CONTENT_MD5)
        self.assertTrue(r)
        if after_rename_callback_fn:
            after_rename_callback_fn(src1_path, src1_path_ver, ren1_path, ren1_path_ver)

    def test_rename_object(self):
        """Basic rename test"""
        self._do_rename_test(self.base_path + '/rename_object')

    def test_rename_and_copy_acls(self):
        """Rename test with copy acls variations"""

        def _set_acls(src_path, src_path_ver):
            src_acls = self.hatrac.get_acl(src_path)
            self.assertListEqual(src_acls['update'], [])
            self.hatrac.set_acl(src_path, 'update', ['dummy-role'], add_role=True)
            src_acls = self.hatrac.get_acl(src_path)
            self.assertIn('update', src_acls)
            self.assertIn('dummy-role', src_acls['update'])

        def _confirm_acls_match(src_path, src_path_ver, ren_path, ren_path_ver):
            src_acls = self.hatrac.get_acl(src_path)
            copied_acls = self.hatrac.get_acl(ren_path)
            self.assertTrue(src_acls == copied_acls)

        def _confirm_acls_do_not_match(src_path, src_path_ver, ren_path, ren_path_ver):
            src_acls = self.hatrac.get_acl(src_path)
            copied_acls = self.hatrac.get_acl(ren_path)
            self.assertFalse(src_acls == copied_acls)

        with self.subTest(msg='rename copy acls == true'):
            self._do_rename_test(self.base_path + '/rename_copy_acls',
                                 before_rename_callback_fn=_set_acls,
                                 after_rename_callback_fn=_confirm_acls_match,
                                 copy_acls=True)

        with self.subTest(msg='rename copy acls == false'):
            self._do_rename_test(self.base_path + '/rename_dont_copy_acls1',
                                 before_rename_callback_fn=_set_acls,
                                 after_rename_callback_fn=_confirm_acls_do_not_match,
                                 copy_acls=False)

        with self.subTest(msg='rename copy acls == default/none'):
            self._do_rename_test(self.base_path + '/rename_dont_copy_acls2',
                                 before_rename_callback_fn=_set_acls,
                                 after_rename_callback_fn=_confirm_acls_do_not_match)

    def test_rename_and_delete(self):
        """Rename test with delete variations"""

        def _delete_source(src_path, src_path_ver, ren_path, ren_path_ver):
            # test delete of source before delete of rename
            r = self.hatrac.del_obj(src_path_ver)
            self.assertIsNone(r)
            # test that source was removed
            with self.assertRaises(requests.HTTPError):
                self.hatrac.get_obj(src_path_ver)
            # test that md5 sum matches header
            r = self.hatrac.content_equals(ren_path_ver, md5=CONTENT_MD5)
            self.assertTrue(r)
            # test that renamed object is still accessible
            r = self.hatrac.get_obj(ren_path_ver)
            self.assertEqual(r.content, CONTENT)

        def _delete_renamed(src_path, src_path_ver, ren_path, ren_path_ver):
            # test delete of rename before delete of source
            r = self.hatrac.del_obj(ren_path_ver)
            self.assertIsNone(r)
            # test that renamed obj was removed
            with self.assertRaises(requests.HTTPError):
                self.hatrac.get_obj(ren_path_ver)
            # test that source contents are no longer accessible too
            with self.assertRaises(requests.HTTPError):
                self.hatrac.get_obj(src_path_ver)
            # test delete of source object name
            r = self.hatrac.del_obj(src_path_ver)
            self.assertIsNone(r)

        with self.subTest(msg='delete source'):
            self._do_rename_test(self.base_path + '/rename_and_delete_source',
                                 after_rename_callback_fn=_delete_source)

        with self.subTest(msg='delete renamed'):
            self._do_rename_test(self.base_path + '/rename_and_delete_renamed',
                                 after_rename_callback_fn=_delete_renamed)


if __name__ == '__main__':
    unittest.main()
