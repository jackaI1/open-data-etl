"""Wait for the real Functions-host timers to publish the summary and the release manifest."""

import json
import os
import time
from pathlib import Path

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

from dpd_etl.summary import accepted_summary

DAY = "2026-11-01"
expected = accepted_summary(Path("data/fixture"), DAY)
connection = os.environ["AZURITE_CONNECTION_STRING"]


def wait_for(description, read):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            return read()
        except ResourceNotFoundError:
            time.sleep(2)
    raise SystemExit(f"Functions host did not publish {description} within 180 seconds")


with BlobServiceClient.from_connection_string(connection) as service:
    def read_summary():
        blob = service.get_blob_client("quality-summaries", f"{DAY}/summary.json")
        actual = json.loads(blob.download_blob().readall())
        assert actual == expected, (actual, expected)
        return actual

    summary = wait_for("the quality summary", read_summary)
    print(f"Real Functions timer wrote {summary['rows_processed']} products to Azurite")

    def read_manifest():
        blob = service.get_blob_client("dpd-releases", f"{DAY}/MANIFEST.json")
        return json.loads(blob.download_blob().readall())

    manifest = wait_for("the release manifest", read_manifest)
    assert manifest["snapshot_date"] == DAY and manifest["file_count"] > 0
    print(f"Real Functions archive timer stored {manifest['file_count']} checksummed files")
