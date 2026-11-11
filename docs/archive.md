# Archive each monthly release in Azure Blob Storage

I keep each monthly download off my laptop so I can restore any month for a rerun.
`dpd_etl.archive` uploads the raw files and the Delta tables for one snapshot date
to a private Azure Blob Storage container. A manifest lists every file with its size
and SHA-256 checksum. Restore downloads the files again and checks each checksum,
so a bad copy never looks like a good one.

## Commands

```sh
export DPD_ARCHIVE_CONNECTION_STRING='...'   # from the storage account, see below
make archive-upload SNAPSHOT_DATE=2026-11-06   # raw files + data/lakehouse Delta tables
make archive-list                              # newest release first
make archive-restore SNAPSHOT_DATE=2026-11-06 RESTORE_DIR=data/restored
```

The same commands work with `.venv/bin/python -m dpd_etl.archive upload|list|restore`.
`--container` (or `DPD_ARCHIVE_CONTAINER`) picks the container, which defaults to
`dpd-releases`.

## Layout in the container

```
2026-11-06/MANIFEST.json      written last; a release without it is not listed
2026-11-06/raw/drug.txt       the downloaded files, unchanged
2026-11-06/outputs/...        the Delta tables for that run
```

Each blob also stores its SHA-256 in its metadata. Uploading the same release again
is safe when the files match. If a file changed, the upload stops with an error
instead of replacing the archived copy. Restore refuses to overwrite a local file.
If a downloaded file fails its checksum, the restore deletes it and stops.

## Create the storage account

The Bicep template in [`infra/azure/main.bicep`](../infra/azure/main.bicep) creates
a Standard LRS storage account with TLS 1.2 only, public blob access turned off,
blob versioning on and 30-day soft delete for blobs and the container.

```sh
az group create --name dpd-releases-rg --location canadacentral
az deployment group create --resource-group dpd-releases-rg \
  --template-file infra/azure/main.bicep \
  --parameters storageAccountName=dpdreleases$RANDOM
az storage account show-connection-string --resource-group dpd-releases-rg \
  --name <storage-account-name> --output tsv
```

Copy the connection string into `DPD_ARCHIVE_CONNECTION_STRING` and keep it out of
Git. To use an Azure identity instead of the account key, pass its object ID as
`writerPrincipalId` and its type as `writerPrincipalType`. The template then grants
Storage Blob Data Contributor on the container only. Setting `allowSharedKeyAccess`
to `false` blocks key access once that identity is in place. Then the connection
string stops working, so only do that after the identity has been tested.

## Test without an Azure account

The tests use Azurite, Microsoft's official local storage emulator. It has the same
blob API, with no account or cost.

```sh
docker compose -p dpd-archive -f azure_summary/compose.yml up -d --wait azurite
export AZURITE_CONNECTION_STRING='DefaultEndpointsProtocol=http;AccountName=dpdlocal;AccountKey=ZHBkLWxvY2FsLWVtdWxhdG9yLW9ubHkta2V5LTAwMDE=;BlobEndpoint=http://127.0.0.1:11000/dpdlocal;'
make archive-test
docker compose -p dpd-archive -f azure_summary/compose.yml down --volumes
```

`make azure-test` builds the sample warehouse first (`make fixture`), because one test reads
its run report. Without Azurite running, the Azurite tests are skipped.

Each test creates its own container and deletes it afterwards. The tests cover a
round trip with byte comparison, a rerun, a changed file being refused, a tampered
blob being caught, a half-finished upload being ignored, the refusal to overwrite,
and the list command. CI runs the same tests in the `azure-summary` job, and the
`azure-infra` job compiles and lints the Bicep template with the Azure CLI.

## Archive on a daily timer

The Functions app in `azure_summary/` has a second timer, `daily_release_archive`,
which runs `dpd_etl.archive.archive_accepted_release`. It is scheduled by
`DPD_ARCHIVE_SCHEDULE` (default `0 30 8 * * *`, UTC). It does four things:

1. Calls `accepted_summary`, so a rejected snapshot, a changed run receipt or
   changed duplicate-pair and unusual-strength counts stop it before any upload.
2. Uploads the raw files (`DPD_RAW_DIR`, default `<data>/raw/files`) and the outputs
   (`DPD_ARCHIVE_OUTPUT_DIR`, required) with checksums and a manifest.
3. If the manifest already exists, it does not upload again. It checks each local
   file against the stored checksum and raises `ReleaseMismatch` if one differs.
4. Returns the status (`archived` or `already_archived`) with the file count and
   the flagged-strength and duplicate-pair counts, which the Functions log shows.

Settings for `func start` are in `azure_summary/local.settings.json.example`. The
Compose host and the CI job set the same variables and check the manifest in
the `dpd-releases` container, using Azurite.

## What this does not do

I have not deployed the template to a real Azure subscription in this repository.
Azurite does not check RBAC roles, private endpoints or the billing side, so those
need a check in a real subscription. The archive is a copy, not a backup policy.
Retention beyond 30 days and cross-region copies are separate decisions.
