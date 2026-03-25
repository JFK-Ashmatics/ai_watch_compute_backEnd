# AzureDataLakeManager Integration Test Results

**Date:** 2026-03-24
**Branch:** feature/azure-adls-storage-adapter
**Ticket:** ASHAIW-13
**Target:** Azurite (Azure Storage emulator) via docker-compose.azurite-test.yml

## Environment

- **Emulator:** Azurite latest (mcr.microsoft.com/azure-storage/azurite)
- **Connection:** Connection string with BlobEndpoint + DfsEndpoint
- **Filesystem:** chris-test
- **SDK:** azure-storage-file-datalake (latest), azure-storage-blob (latest)

## Results: 1/18 PASSED (Azurite limitation)

| Test Category | Status | Notes |
|---|---|---|
| `create_container` | PASS | Filesystem creation works (delegates to Blob SDK) |
| All DFS operations | FAIL | `upload_data`, `get_paths`, `delete_file`, etc. |

## Root Cause: Azurite Does Not Support ADLS Gen2 DFS API

Azurite implements the Azure **Blob** REST API but not the Azure **Data Lake Storage Gen2 DFS** REST API.
The `azure-storage-file-datalake` SDK uses DFS-specific endpoints (`/{filesystem}/{path}?resource=file`)
that Azurite's blob handler does not recognize, returning empty error responses.

- `create_file_system()` works because it delegates to the Blob SDK's `create_container()`
- `upload_data()`, `download_file()`, `get_paths()`, `rename_directory()`, `delete_directory()`
  all use DFS API paths which Azurite cannot serve

This is a **known Azurite limitation** — not a bug in our adapter.

## Validation Approach

The AzureDataLakeManager adapter is validated through:

1. **Structural equivalence**: Same `StorageManager` interface contract as the fully-tested
   S3Manager (16/16 passing against MinIO). Same factory wiring, same settings pattern.
2. **Connection string auth**: Verified working (Azurite accepts connection, creates filesystem)
3. **Code review**: All methods follow documented Azure SDK patterns from official Microsoft docs
4. **Test suite ready**: 18 tests written and ready to run against a real Azure Storage account
   with hierarchical namespace enabled

## Testing Against Real Azure ADLS Gen2

To run the full test suite against a real Azure account:

```bash
# Create a storage account with hierarchical namespace enabled:
# az storage account create --name <name> --resource-group <rg> \
#     --kind StorageV2 --hns true

# Option A: Account key auth
AZURE_DATALAKE_ACCOUNT_URL=https://<account>.dfs.core.windows.net \
AZURE_DATALAKE_ACCOUNT_KEY=<key> \
AZURE_DATALAKE_FILESYSTEM_NAME=chris-test \
python tests/test_azure_datalake_manager.py

# Option B: Managed identity (on ACA/AKS)
AZURE_DATALAKE_ACCOUNT_URL=https://<account>.dfs.core.windows.net \
AZURE_DATALAKE_FILESYSTEM_NAME=chris-test \
python tests/test_azure_datalake_manager.py
```

## How to Reproduce Azurite Results

```bash
docker compose -f docker-compose.azurite-test.yml up -d azurite
docker compose -f docker-compose.azurite-test.yml run --rm azure-test
docker compose -f docker-compose.azurite-test.yml down -v
```
