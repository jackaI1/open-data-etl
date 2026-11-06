"""Write an accepted snapshot's quality summary on a configurable daily timer."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import azure.functions as func
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContentSettings

from dpd_etl.summary import accepted_summary

app = func.FunctionApp()


def write_summary():
    data_dir = Path(os.environ["DPD_DATA_DIR"])
    # Unset means today's UTC snapshot; a missing or rejected date raises and retries.
    snapshot = os.environ.get("DPD_SNAPSHOT_DATE") or datetime.now(timezone.utc).date().isoformat()
    payload = accepted_summary(data_dir, snapshot)
    container = os.environ.get("DPD_SUMMARY_CONTAINER", "quality-summaries")
    with BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"]) as service:
        client = service.get_container_client(container)
        try:
            client.create_container()
        except ResourceExistsError:
            pass
        # Stable name makes timer retries replace one result, without duplicate summaries.
        client.upload_blob(f"{snapshot}/summary.json", json.dumps(payload, sort_keys=True),
                           overwrite=True, content_settings=ContentSettings(
                               content_type="application/json"))
    return payload


@app.timer_trigger(schedule="%DPD_SUMMARY_SCHEDULE%", arg_name="timer",
                   run_on_startup=False, use_monitor=True)
def daily_quality_summary(timer: func.TimerRequest) -> None:
    write_summary()
