"""Render gcloud's Batch Job JSON from explicit, non-secret deployment settings."""

import argparse
import json
import re
from pathlib import Path

from dpd_etl.gcp_batch import valid_date, valid_run


def batch_config(project, region, bucket, image, snapshot, run_id, raw_prefix,
                 fixture=False, baseline=None):
    valid_date(snapshot)
    valid_run(run_id)
    if len(run_id) > 59:
        raise ValueError("deployment run ID must be at most 59 characters (job name adds dpd-)")
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,61}[a-z0-9]", project):
        raise ValueError("invalid project ID")
    commands = ["--bucket", bucket, "--raw-prefix", raw_prefix,
                "--snapshot-date", snapshot, "--run-id", run_id]
    if fixture:
        commands.append("--fixture")
    if baseline:
        commands.extend(["--baseline", baseline])
    return {
        "taskGroups": [{"taskCount": 1, "parallelism": 1, "taskSpec": {
            "runnables": [{"container": {"imageUri": image,
                            "entrypoint": "python", "commands": [
                                "-m", "dpd_etl.gcp_batch", *commands]}}],
            "computeResource": {"cpuMilli": 2000, "memoryMib": 6144},
            "maxRetryCount": 0, "maxRunDuration": "3600s",
        }}],
        "allocationPolicy": {
            "instances": [{"policy": {"machineType": "e2-standard-2",
                                        "bootDisk": {"sizeGb": 50}}}],
            "serviceAccount": {"email": f"dpd-batch@{project}.iam.gserviceaccount.com",
                               "scopes": ["https://www.googleapis.com/auth/cloud-platform"]},
            "network": {"networkInterfaces": [{
                "network": f"projects/{project}/global/networks/dpd-batch",
                "subnetwork": f"projects/{project}/regions/{region}/subnetworks/dpd-batch",
            }]},
        },
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
        "labels": {"pipeline": "dpd-etl"},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("project", "region", "bucket", "image", "snapshot", "run-id", "raw-prefix"):
        parser.add_argument(f"--{key}", required=True)
    parser.add_argument("--fixture", action="store_true")
    parser.add_argument("--baseline")
    parser.add_argument("--output", type=Path, required=True)
    args = vars(parser.parse_args())
    output = args.pop("output")
    config = batch_config(**args)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(config, indent=2) + "\n")


if __name__ == "__main__":
    main()
