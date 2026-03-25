"""
Azure Data Lake Storage Gen2 (ADLS Gen2) storage manager module.

Created: 2026-03-24
Ticket: ASHAIW-13

Uses the azure-storage-file-datalake SDK to leverage ADLS Gen2's hierarchical
namespace for atomic directory operations — critical for DICOM pipeline workloads
where studies must be ingested, processed, and flushed efficiently.

Key advantages over flat object storage:
    - Real directory hierarchy maps to DICOM structure
      (tenant/study-uid/series-uid/SOPInstanceUID.dcm)
    - Atomic rename for move_path (single metadata operation, O(1))
    - Recursive delete for delete_path (single REST call, server-side)
    - POSIX ACLs for multi-tenant isolation
    - Managed Identity / Entra ID auth (no shared secrets)

Authentication supports:
    - Storage account key (dev/test)
    - DefaultAzureCredential (managed identity on ACA/AKS, Azure CLI for local dev)

For copy_obj, falls through to the Azure Blob SDK's server-side copy
(start_copy_from_url) since ADLS Gen2 has no native copy operation.
"""

import logging
from pathlib import Path
from typing import Dict, List, AnyStr, Optional

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient

from core.storage.storagemanager import StorageManager

logger = logging.getLogger(__name__)


class AzureDataLakeManager(StorageManager):

    def __init__(self, filesystem_name: str, conn_params: dict):
        self.filesystem_name = filesystem_name
        self.conn_params = conn_params
        self._service_client = None
        self._fs_client = None
        self._blob_service_client = None

    def __get_service_client(self) -> DataLakeServiceClient:
        if self._service_client is not None:
            return self._service_client

        connection_string = self.conn_params.get('connection_string')
        if connection_string:
            # Azurite and dev environments use connection strings
            self._service_client = DataLakeServiceClient.from_connection_string(
                connection_string
            )
            return self._service_client

        credential = self.conn_params.get('credential')
        account_url = self.conn_params.get('account_url')

        if credential is None:
            # Try account key, then fall back to DefaultAzureCredential
            account_key = self.conn_params.get('account_key')
            if account_key:
                credential = account_key
            else:
                from azure.identity import DefaultAzureCredential
                credential = DefaultAzureCredential()

        self._service_client = DataLakeServiceClient(
            account_url=account_url,
            credential=credential,
        )
        return self._service_client

    def __get_fs_client(self):
        if self._fs_client is not None:
            return self._fs_client
        service = self.__get_service_client()
        self._fs_client = service.get_file_system_client(self.filesystem_name)
        return self._fs_client

    def __get_blob_service_client(self) -> BlobServiceClient:
        """
        Get a Blob SDK client for server-side copy operations.
        ADLS Gen2 and Blob Storage share the same underlying account; the blob
        endpoint is the DFS URL with 'dfs' replaced by 'blob'.
        """
        if self._blob_service_client is not None:
            return self._blob_service_client

        connection_string = self.conn_params.get('connection_string')
        if connection_string:
            self._blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            return self._blob_service_client

        credential = self.conn_params.get('credential')
        dfs_url = self.conn_params.get('account_url', '')
        blob_url = dfs_url.replace('.dfs.', '.blob.')

        if credential is None:
            account_key = self.conn_params.get('account_key')
            if account_key:
                credential = account_key
            else:
                from azure.identity import DefaultAzureCredential
                credential = DefaultAzureCredential()

        self._blob_service_client = BlobServiceClient(
            account_url=blob_url,
            credential=credential,
        )
        return self._blob_service_client

    def create_container(self) -> None:
        """
        Create the ADLS Gen2 filesystem (equivalent to a container/bucket).
        """
        fs = self.__get_fs_client()
        try:
            fs.create_file_system()
        except ResourceExistsError:
            pass  # idempotent

    def ls(self, path: str) -> List[str]:
        """
        Return a list of file paths under the given prefix.
        Directories are excluded — only files are returned.
        """
        if not path:
            return []
        fs = self.__get_fs_client()
        result = []
        try:
            paths = fs.get_paths(path=path, recursive=True)
            for p in paths:
                if not p.is_directory:
                    result.append(p.name)
        except ResourceNotFoundError:
            pass
        return result

    def path_exists(self, path: str) -> bool:
        """
        Return True if the path exists as either a file or directory.
        """
        fs = self.__get_fs_client()
        # Try as file first
        fc = fs.get_file_client(path)
        if fc.exists():
            return True
        # Try as directory
        dc = fs.get_directory_client(path)
        if dc.exists():
            return True
        # Check if it's a prefix (path without trailing content but has children)
        try:
            paths = fs.get_paths(path=path, recursive=False, max_results=1)
            for _ in paths:
                return True
        except ResourceNotFoundError:
            pass
        return False

    def obj_exists(self, file_path: str) -> bool:
        """
        Return True if a file exists at the exact path.
        """
        fs = self.__get_fs_client()
        fc = fs.get_file_client(file_path)
        return fc.exists()

    def upload_obj(self, file_path: str, contents: AnyStr,
                   content_type: Optional[str] = None) -> None:
        """
        Upload data to a file in ADLS Gen2.
        Creates parent directories implicitly.
        """
        if isinstance(contents, str):
            contents = contents.encode('utf-8')
        fs = self.__get_fs_client()
        fc = fs.get_file_client(file_path)
        kwargs = {'overwrite': True}
        if content_type:
            from azure.storage.filedatalake import ContentSettings
            kwargs['content_settings'] = ContentSettings(content_type=content_type)
        fc.upload_data(contents, **kwargs)

    def download_obj(self, file_path: str) -> bytes:
        """
        Download file data from ADLS Gen2.
        """
        fs = self.__get_fs_client()
        fc = fs.get_file_client(file_path)
        return fc.download_file().readall()

    def copy_obj(self, src: str, dst: str) -> None:
        """
        Copy a file within the same filesystem using server-side blob copy.
        No data is downloaded to the client.
        """
        blob_service = self.__get_blob_service_client()
        source_blob = blob_service.get_blob_client(self.filesystem_name, src)
        dest_blob = blob_service.get_blob_client(self.filesystem_name, dst)
        copy = dest_blob.start_copy_from_url(source_blob.url)
        # start_copy_from_url is asynchronous — poll until the copy completes
        # so callers can safely read dst immediately after this method returns.
        props = dest_blob.get_blob_properties()
        while props.copy.status == 'pending':
            props = dest_blob.get_blob_properties()
        if props.copy.status != 'success':
            raise RuntimeError(
                f"Server-side copy failed: {props.copy.status} — {props.copy.status_description}"
            )

    def delete_obj(self, file_path: str) -> None:
        """
        Delete a file from ADLS Gen2.
        """
        fs = self.__get_fs_client()
        fc = fs.get_file_client(file_path)
        fc.delete_file()

    def copy_path(self, src: str, dst: str) -> None:
        """
        Copy all files under src path to dst path.
        Uses server-side blob copy for each file (no client-side data transfer).
        """
        l_ls = self.ls(src)
        for obj_path in l_ls:
            new_path = obj_path.replace(src, dst, 1)
            self.copy_obj(obj_path, new_path)

    def move_path(self, src: str, dst: str) -> None:
        """
        Atomically move/rename a directory.
        Single metadata operation — O(1) regardless of directory size.
        Falls back to file-level rename if src is a single file.
        """
        fs = self.__get_fs_client()
        dc = fs.get_directory_client(src)
        try:
            dc.get_directory_properties()
            # src is a directory — atomic rename
            dc.rename_directory(f"{self.filesystem_name}/{dst}")
        except ResourceNotFoundError:
            # src might be a single file or a prefix without a real directory
            fc = fs.get_file_client(src)
            if fc.exists():
                fc.rename_file(f"{self.filesystem_name}/{dst}")
            else:
                # Prefix-based: fall back to copy + delete per object
                l_ls = self.ls(src)
                for obj_path in l_ls:
                    new_path = obj_path.replace(src, dst, 1)
                    file_client = fs.get_file_client(obj_path)
                    file_client.rename_file(f"{self.filesystem_name}/{new_path}")

    def delete_path(self, path: str) -> None:
        """
        Delete all data under a path.
        If path is a directory, uses recursive delete — single REST call,
        server-side recursion. If path is a file, deletes the file.
        """
        fs = self.__get_fs_client()
        dc = fs.get_directory_client(path)
        try:
            dc.get_directory_properties()
            dc.delete_directory()  # recursive by default
            return
        except ResourceNotFoundError:
            pass
        # Try as file
        fc = fs.get_file_client(path)
        if fc.exists():
            fc.delete_file()
            return
        # Prefix-based cleanup: delete each object individually
        l_ls = self.ls(path)
        for obj_path in l_ls:
            try:
                fs.get_file_client(obj_path).delete_file()
            except ResourceNotFoundError:
                pass

    def sanitize_obj_names(self, path: str) -> Dict[str, str]:
        """
        Removes commas from the paths of all files under the specified input
        path.
        Handles special cases:
            - Files with names that only contain commas and white spaces
              are deleted.
            - Files inside "folders" whose names only contain commas and
              white spaces are moved to the parent folder (the comma-only
              path component is collapsed).

        Returns a dictionary of modified file paths. Keys are original paths,
        values are new paths. Deleted files have the empty string as value.

        Uses atomic file renames for efficiency (no copy + delete needed).

        NOTE: On ADLS Gen2, directories are real resources. This method
        operates on files only (via ls()), so empty directory resources with
        comma-only names may remain after their contents are moved/deleted.
        """
        new_obj_paths = {}
        l_ls = self.ls(path)

        if len(l_ls) != 1 or l_ls[0] != path:  # path is a prefix
            p = Path(path)
            fs = self.__get_fs_client()

            for obj_path in l_ls:
                p_obj = Path(obj_path)

                if p_obj.name.replace(',', '').strip() == '':
                    self.delete_obj(obj_path)
                    new_obj_paths[obj_path] = ''
                else:
                    new_parts = []
                    for part in p_obj.relative_to(p).parts:
                        new_part = part.replace(',', '')
                        if new_part.strip() != '':
                            new_parts.append(new_part)

                    new_p_obj = p / Path(*new_parts)

                    if new_p_obj != p_obj:
                        new_obj_path = str(new_p_obj)
                        # Use atomic rename instead of copy + delete
                        fc = fs.get_file_client(obj_path)
                        fc.rename_file(
                            f"{self.filesystem_name}/{new_obj_path}"
                        )
                        new_obj_paths[obj_path] = new_obj_path
        return new_obj_paths
