"""Wait for a real Functions-host timer to publish the expected accepted summary."""

import json
import os
import time
from pathlib import Path

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

from dpd_etl.summary import accepted_summary

expected = accepted_summary(Path("data/fixture"), "2026-11-01")
with BlobServiceClient.from_connection_string(os.environ["AZURITE_CONNECTION_STRING"]) as service:
    blob = service.get_blob_client("quality-summaries", "2026-11-01/summary.json")
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            actual = json.loads(blob.download_blob().readall())
            assert actual == expected, (actual, expected)
            print(f"Real Functions timer wrote {actual['rows_processed']} products to Azurite")
            break
        except ResourceNotFoundError:
            time.sleep(2)
    else:
        raise SystemExit("Functions host did not publish within 180 seconds")
