# Save a scheduled quality summary

I wanted one small result I could read without opening the full catalogue.
The Python Azure Functions app runs on a timer and writes the accepted run's
counts, null rates and duration as a JSON file. Azurite imitates Azure blob
storage, a cloud file store, so I can test the complete write without an account.

## Try the container setup

On a machine with Docker and Compose:

```sh
make install
make fixture SNAPSHOT_DATE=2026-11-01
export DPD_SNAPSHOT_DATE=2026-11-01
export DPD_SUMMARY_SCHEDULE='*/5 * * * * *'
docker compose -p dpd-summary -f azure_summary/compose.yml up -d --build
```

This test schedule runs every five seconds. The normal default is 08:00 UTC
every day. The Functions runtime starts the Python function; its storage client
writes `quality-summaries/2026-11-01/summary.json` to Azurite. Retrying replaces
that file. The input is mounted read-only. The function reads accepted outputs;
it does not download the catalogue or rerun Spark. I still schedule the pipeline
before this summary if I want a new snapshot each day.

To read the result with Python after installing the optional client:

```sh
uv pip install --python .venv/bin/python -e '.[azure]'
export AZURITE_CONNECTION_STRING='DefaultEndpointsProtocol=http;AccountName=dpdlocal;AccountKey=ZHBkLWxvY2FsLWVtdWxhdG9yLW9ubHkta2V5LTAwMDE=;BlobEndpoint=http://127.0.0.1:11000/dpdlocal;'
.venv/bin/python - <<'PY'
import os
from azure.storage.blob import BlobServiceClient
with BlobServiceClient.from_connection_string(os.environ['AZURITE_CONNECTION_STRING']) as client:
    print(client.get_blob_client('quality-summaries', '2026-11-01/summary.json').download_blob().readall().decode())
PY
docker compose -p dpd-summary -f azure_summary/compose.yml down --volumes
```

The committed key is public emulator configuration, not an Azure credential.
Azurite has no persistent volume here; stopping and removing the containers
removes the stored summaries. All published ports listen only on this computer.

## Use the Functions development host

I can also install Azure Functions Core Tools v4 and Azurite, copy
`azure_summary/local.settings.json.example` to `azure_summary/local.settings.json`,
set an absolute `DPD_DATA_DIR`, then run `func start` in `azure_summary/`.
I install `azure_summary/requirements.txt` into the active environment and install
the repo with `pip install --no-deps -e .` so its summary reader is importable.
For that template's `UseDevelopmentStorage=true`, Azurite must use its standard
local account and ports 10000-10002. The Compose setup instead configures its own
explicit emulator account and ports 11000-11002.

`DPD_SNAPSHOT_DATE` chooses a saved date. If empty, the function uses today's UTC
date. A missing receipt, failed check or changed totals raises an error before
any blob is written. I can see and retry the failure through the Functions host.
The date is not silently changed to the last successful date.

The SDK tests build the real registered Functions timer, check its bindings and
invoke its user function with a TimerRequest test double. The integration test
uses the same function with real Azurite, reads the blob back, checks JSON and
content type, and retries with a late timer. CI also starts Microsoft's Functions
host image and waits for an actual timer-triggered write. It always removes the
containers. This proves the local runtime and storage path, not an Azure cloud
deployment, IAM or service availability.

I followed the official [Python testing guide](https://learn.microsoft.com/en-us/azure/azure-functions/functions-reference-python#unit-testing)
and [timer binding reference](https://learn.microsoft.com/en-us/azure/azure-functions/functions-bindings-timer).
