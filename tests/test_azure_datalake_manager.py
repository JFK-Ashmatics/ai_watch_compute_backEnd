#!/usr/bin/env python3
"""
Integration tests for AzureDataLakeManager against Azurite (Azure Storage emulator).

Created: 2026-03-24
Ticket: ASHAIW-13
Part of the ChRIS CUBE Azure ADLS Gen2 storage adapter work
(feature/azure-adls-storage-adapter).

Validates all StorageManager interface methods implemented by AzureDataLakeManager
against a live Azurite instance — covers CRUD, atomic directory rename, recursive
delete, server-side blob copy, and comma-sanitization logic.

Run via docker compose (recommended):
    docker compose -f docker-compose.azurite-test.yml up -d azurite
    docker compose -f docker-compose.azurite-test.yml run --rm azure-test
    docker compose -f docker-compose.azurite-test.yml down -v

Or locally (with Azurite running on localhost:10000):
    AZURE_DATALAKE_ACCOUNT_URL=http://127.0.0.1:10000/devstoreaccount1 \
    AZURE_DATALAKE_ACCOUNT_KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==' \
    AZURE_DATALAKE_FILESYSTEM_NAME=chris-test \
    python tests/test_azure_datalake_manager.py
"""

import os
import sys
import importlib.util
import unittest

# Load azure_datalake_manager directly to avoid core/__init__.py pulling in Django/Celery.
_base = os.path.join(os.path.dirname(__file__), '..', 'chris_backend', 'core', 'storage')


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_storagemanager = _load_module('storagemanager', os.path.join(_base, 'storagemanager.py'))
sys.modules['core.storage.storagemanager'] = _storagemanager
AzureDataLakeManager = _load_module(
    'azure_datalake_manager', os.path.join(_base, 'azure_datalake_manager.py')
).AzureDataLakeManager


_AZURITE_CONN_STR = (
    'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;'
    'AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/'
    'K1SZFPTOtr/KBHBeksoGMGw==;'
    'BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;'
)


def get_test_manager():
    conn_string = os.environ.get('AZURE_DATALAKE_CONNECTION_STRING')
    if conn_string:
        conn_params = {'connection_string': conn_string}
    else:
        # Fall back to account_url + account_key for real Azure
        account_url = os.environ.get(
            'AZURE_DATALAKE_ACCOUNT_URL',
            'http://127.0.0.1:10000/devstoreaccount1')
        account_key = os.environ.get(
            'AZURE_DATALAKE_ACCOUNT_KEY',
            'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/'
            'K1SZFPTOtr/KBHBeksoGMGw==')
        conn_params = {
            'account_url': account_url,
            'account_key': account_key,
        }
    filesystem = os.environ.get('AZURE_DATALAKE_FILESYSTEM_NAME', 'chris-test')
    return AzureDataLakeManager(filesystem, conn_params)


class TestAzureDataLakeConnection(unittest.TestCase):

    def setUp(self):
        self.manager = get_test_manager()

    def test_create_container(self):
        """Filesystem creation should succeed (or be idempotent)."""
        self.manager.create_container()
        # call again to verify idempotency
        self.manager.create_container()


class TestAzureDataLakeCRUD(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.manager = get_test_manager()
        cls.manager.create_container()

    def tearDown(self):
        for key in self.manager.ls('test/'):
            try:
                self.manager.delete_obj(key)
            except Exception:
                pass

    def test_upload_and_download_bytes(self):
        data = b'hello ChRIS from ADLS Gen2!'
        self.manager.upload_obj('test/hello.txt', data, content_type='text/plain')
        result = self.manager.download_obj('test/hello.txt')
        self.assertEqual(result, data)

    def test_upload_and_download_str(self):
        data = 'string content gets encoded to utf-8'
        self.manager.upload_obj('test/string.txt', data, content_type='text/plain')
        result = self.manager.download_obj('test/string.txt')
        self.assertEqual(result, data.encode('utf-8'))

    def test_obj_exists(self):
        self.assertFalse(self.manager.obj_exists('test/nonexistent.txt'))
        self.manager.upload_obj('test/exists.txt', b'data')
        self.assertTrue(self.manager.obj_exists('test/exists.txt'))

    def test_path_exists(self):
        self.assertFalse(self.manager.path_exists('test/subdir/'))
        self.manager.upload_obj('test/subdir/file.txt', b'data')
        self.assertTrue(self.manager.path_exists('test/subdir'))
        self.assertTrue(self.manager.path_exists('test/subdir/file.txt'))

    def test_ls(self):
        self.manager.upload_obj('test/ls/a.txt', b'a')
        self.manager.upload_obj('test/ls/b.txt', b'b')
        self.manager.upload_obj('test/ls/sub/c.txt', b'c')
        result = self.manager.ls('test/ls')
        self.assertEqual(sorted(result),
                         ['test/ls/a.txt', 'test/ls/b.txt', 'test/ls/sub/c.txt'])

    def test_ls_empty_prefix(self):
        result = self.manager.ls('')
        self.assertEqual(result, [])

    def test_delete_obj(self):
        self.manager.upload_obj('test/del.txt', b'delete me')
        self.assertTrue(self.manager.obj_exists('test/del.txt'))
        self.manager.delete_obj('test/del.txt')
        self.assertFalse(self.manager.obj_exists('test/del.txt'))

    def test_copy_obj(self):
        """Server-side blob copy within the same filesystem."""
        self.manager.upload_obj('test/src.txt', b'copy me')
        self.manager.copy_obj('test/src.txt', 'test/dst.txt')
        self.assertTrue(self.manager.obj_exists('test/dst.txt'))
        self.assertEqual(self.manager.download_obj('test/dst.txt'), b'copy me')
        # source still exists
        self.assertTrue(self.manager.obj_exists('test/src.txt'))


class TestAzureDataLakePathOps(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.manager = get_test_manager()
        cls.manager.create_container()

    def tearDown(self):
        for key in self.manager.ls('test/'):
            try:
                self.manager.delete_obj(key)
            except Exception:
                pass

    def test_copy_path(self):
        self.manager.upload_obj('test/cp/a.txt', b'a')
        self.manager.upload_obj('test/cp/sub/b.txt', b'b')
        self.manager.copy_path('test/cp', 'test/cp2')
        # originals still exist
        self.assertTrue(self.manager.obj_exists('test/cp/a.txt'))
        # copies exist
        self.assertTrue(self.manager.obj_exists('test/cp2/a.txt'))
        self.assertTrue(self.manager.obj_exists('test/cp2/sub/b.txt'))
        self.assertEqual(self.manager.download_obj('test/cp2/sub/b.txt'), b'b')

    def test_move_path(self):
        """Atomic directory rename — the key ADLS Gen2 advantage."""
        self.manager.upload_obj('test/mv/a.txt', b'a')
        self.manager.upload_obj('test/mv/sub/b.txt', b'b')
        self.manager.move_path('test/mv', 'test/mv2')
        # originals gone
        self.assertFalse(self.manager.obj_exists('test/mv/a.txt'))
        self.assertFalse(self.manager.obj_exists('test/mv/sub/b.txt'))
        # moved objects exist
        self.assertTrue(self.manager.obj_exists('test/mv2/a.txt'))
        self.assertEqual(self.manager.download_obj('test/mv2/sub/b.txt'), b'b')

    def test_delete_path(self):
        """Recursive directory delete — single REST call."""
        self.manager.upload_obj('test/delpath/a.txt', b'a')
        self.manager.upload_obj('test/delpath/b.txt', b'b')
        self.manager.upload_obj('test/delpath/sub/c.txt', b'c')
        self.manager.delete_path('test/delpath')
        self.assertEqual(self.manager.ls('test/delpath'), [])

    def test_sanitize_obj_names(self):
        self.manager.upload_obj('test/san/fi,le.txt', b'data')
        self.manager.upload_obj('test/san/clean.txt', b'ok')
        result = self.manager.sanitize_obj_names('test/san')
        # comma-containing file should be renamed
        self.assertIn('test/san/fi,le.txt', result)
        self.assertEqual(result['test/san/fi,le.txt'], 'test/san/file.txt')
        # clean file should not appear in result
        self.assertNotIn('test/san/clean.txt', result)
        # new file exists, old one gone
        self.assertTrue(self.manager.obj_exists('test/san/file.txt'))
        self.assertFalse(self.manager.obj_exists('test/san/fi,le.txt'))

    def test_sanitize_deletes_comma_only_names(self):
        # file whose ENTIRE name is only commas and whitespace gets deleted
        self.manager.upload_obj('test/san2/,,', b'junk')
        # file with commas in stem but non-comma extension gets renamed
        self.manager.upload_obj('test/san2/,,.txt', b'renamed')
        self.manager.upload_obj('test/san2/good.txt', b'keep')
        result = self.manager.sanitize_obj_names('test/san2')
        # pure comma name -> deleted
        self.assertIn('test/san2/,,', result)
        self.assertEqual(result['test/san2/,,'], '')
        self.assertFalse(self.manager.obj_exists('test/san2/,,'))
        # comma stem + extension -> renamed
        self.assertIn('test/san2/,,.txt', result)
        self.assertEqual(result['test/san2/,,.txt'], 'test/san2/.txt')
        # clean file untouched
        self.assertTrue(self.manager.obj_exists('test/san2/good.txt'))


class TestAzureDataLakeDICOMHierarchy(unittest.TestCase):
    """
    Tests that validate the DICOM-oriented hierarchy use case:
    tenant/study-uid/series-uid/SOPInstanceUID.dcm
    """

    @classmethod
    def setUpClass(cls):
        cls.manager = get_test_manager()
        cls.manager.create_container()

    def tearDown(self):
        for key in self.manager.ls('tenant-001/'):
            try:
                self.manager.delete_obj(key)
            except Exception:
                pass

    def test_study_ingest_and_flush(self):
        """Simulate: ingest a DICOM study, process it, then flush the entire study."""
        study_path = 'tenant-001/1.2.840.113619.2.55/1.2.840.113619.2.55.1'

        # Ingest: upload multiple DICOM instances
        for i in range(10):
            self.manager.upload_obj(
                f'{study_path}/1.2.840.{i:04d}.dcm',
                b'\x00' * 1024,  # fake DICOM data
            )

        # Verify study is listed
        files = self.manager.ls('tenant-001/1.2.840.113619.2.55')
        self.assertEqual(len(files), 10)

        # Flush entire study — atomic recursive delete
        self.manager.delete_path('tenant-001/1.2.840.113619.2.55')
        self.assertEqual(self.manager.ls('tenant-001/1.2.840.113619.2.55'), [])

    def test_tenant_isolation(self):
        """Verify operations on one tenant don't affect another."""
        self.manager.upload_obj('tenant-001/study-a/file.dcm', b'tenant1')
        self.manager.upload_obj('tenant-002/study-b/file.dcm', b'tenant2')

        # Delete tenant-001's study
        self.manager.delete_path('tenant-001/study-a')

        # tenant-002 is untouched
        self.assertTrue(self.manager.obj_exists('tenant-002/study-b/file.dcm'))
        self.assertEqual(self.manager.download_obj('tenant-002/study-b/file.dcm'), b'tenant2')

        # cleanup
        self.manager.delete_path('tenant-002')


if __name__ == '__main__':
    print(f"Testing AzureDataLakeManager against: "
          f"{os.environ.get('AZURE_DATALAKE_ACCOUNT_URL', 'http://127.0.0.1:10000/devstoreaccount1')}")
    print(f"Filesystem: {os.environ.get('AZURE_DATALAKE_FILESYSTEM_NAME', 'chris-test')}")
    print()
    unittest.main(verbosity=2)
