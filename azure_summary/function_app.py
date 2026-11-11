"""Write an accepted snapshot's summary and archive its release on configurable daily timers."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import azure.functions as func
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContentSettings

from dpd_etl import archive
from dpd_etl.summary import accepted_summary

app = func.FunctionApp()


def _snapshot_date() -> str:
    # Unset means today's UTC snapshot; a missing or rejected date raises and retries.
    return os.environ.get("DPD_SNAPSHOT_DATE") or datetime.now(timezone.utc).date().isoformat()


def write_summary():
    data_dir = Path(os.environ["DPD_DATA_DIR"])
    snapshot = _snapshot_date()
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


def archive_release() -> dict:
    """Upload the accepted snapshot's raw files and outputs to Blob Storage, once."""
    data_dir = Path(os.environ["DPD_DATA_DIR"])
    raw_dir = Path(os.environ.get("DPD_RAW_DIR") or data_dir / "raw" / "files")
    output_dir = Path(os.environ["DPD_ARCHIVE_OUTPUT_DIR"])
    with archive.container_client(
        os.environ["DPD_ARCHIVE_CONNECTION_STRING"],
        os.environ.get("DPD_ARCHIVE_CONTAINER", archive.DEFAULT_CONTAINER),
    ) as container:
        return archive.archive_accepted_release(
            container, data_dir, _snapshot_date(), raw_dir=raw_dir, output_dir=output_dir,
        )


@app.timer_trigger(schedule="%DPD_SUMMARY_SCHEDULE%", arg_name="timer",
                   run_on_startup=False, use_monitor=True)
def daily_quality_summary(timer: func.TimerRequest) -> None:
    write_summary()


@app.timer_trigger(schedule="%DPD_ARCHIVE_SCHEDULE%", arg_name="timer",
                   run_on_startup=False, use_monitor=True)
def daily_release_archive(timer: func.TimerRequest) -> None:
    archive_release()
