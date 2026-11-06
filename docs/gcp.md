# Running the catalogue away from my laptop

I use the same cleaning code and checks locally and in the cloud. The cloud path
lets me keep raw files and finished snapshots together, and read a check report
without copying the whole database back to my computer.

## Try it without an account

After `make install` and the Java setup in [the local guide](pipeline.md#local-setup):

```sh
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"  # macOS
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
.venv/bin/python -m pytest -q tests/test_gcp.py
PROJECT_ID=sample-project BUCKET=sample-dpd-data RUN_ID=sample \
  FIXTURE=true scripts/deploy_gcp.sh render
cat data/gcp/job.json
```

The integration test runs real Spark processing and reads the resulting DuckDB
database. It mocks Google's storage client, including object creation conflicts.
It then restores the earlier database, rejects a deliberately smaller catalogue,
and reads that failure report through the web service. It does not contact GCP.

To inspect the infrastructure without credentials, install Terraform 1.9 or newer:

```sh
terraform -chdir=infra/gcp init -backend=false
terraform -chdir=infra/gcp validate
terraform -chdir=infra/gcp fmt -check
terraform -chdir=infra/gcp test
```

## Set up a cloud project

I need a Google Cloud project with billing enabled, Google Cloud CLI (`gcloud`),
Terraform, Docker with a running engine, and `make install`. This path creates
billable resources; there is no live cloud deployment bundled with this repo.
The tested account-free path above remains available without billing.

For a project I administer, the initial configuration is:

```sh
gcloud auth login
gcloud auth application-default login
export PROJECT_ID=my-project-id
export BUCKET=my-globally-unique-dpd-bucket
export REGION=us-central1
export SNAPSHOT_DATE=2026-11-01
scripts/deploy_gcp.sh deploy
```

Terraform asks me to review the resources before applying. It enables the APIs,
creates the bucket, a container-image repository, two service identities and a
dedicated Batch network. Docker builds Linux/AMD64 images, even on an Apple Silicon
Mac. The script pushes them to Artifact Registry, Google's image store, and deploys
the authenticated report service to Cloud Run, which runs it when requests arrive.
It does not submit a data-processing job yet.

The deploying identity needs permission to enable services, create these resources,
edit their IAM policies, upload images, deploy Cloud Run services, submit Batch jobs
and act as both service accounts. A project owner can perform this setup. In a
managed project, an administrator can grant narrower setup permissions. No service
account key files are needed: Google attaches each runtime's identity to it.
If an API or IAM policy has just been enabled, allow propagation before submitting.

Keep `infra/gcp/terraform.tfstate` private and backed up. It tracks the resources
for updates and teardown; it is ignored by Git. For shared deployments, move it
to a private Terraform backend before others apply changes. This directory assumes
one deployment per project, with fixed `dpd-*` resource names.

## Upload inputs and submit a job

Start with the included public-data sample:

```sh
gcloud storage cp tests/fixtures/raw/*.txt "gs://$BUCKET/raw/$SNAPSHOT_DATE/"
export RUN_ID=sample-001
export FIXTURE=true
scripts/deploy_gcp.sh submit
gcloud batch jobs describe "dpd-$RUN_ID" --project "$PROJECT_ID" --location "$REGION"
```

For the real catalogue I use the existing downloader, then upload its seven files:

```sh
make extract
export SNAPSHOT_DATE=$(date -u +%F)
export RUN_ID="catalogue-$(date -u +%Y%m%d-%H%M%S)"
unset FIXTURE
gcloud storage cp data/raw/files/*.txt "gs://$BUCKET/raw/$SNAPSHOT_DATE/"
scripts/deploy_gcp.sh submit
```

I finish uploading a dated raw folder before submitting and keep it unchanged for
that run. `RAW_PREFIX` can override the default `raw/$SNAPSHOT_DATE`. Each input's
object generation and SHA-256 checksum appear in the report, and downloads are
generation-pinned. There is no transaction across the seven raw objects, so I use
a new dated folder for a new input set.

The script writes the exact job configuration to `data/gcp/job.json`. Cloud Batch
allocates a two-CPU, 8 GB virtual machine for one task, with a one-hour task limit,
6 GB task memory allocation and a 50 GB boot disk. It runs the ETL container,
staging GCS files on temporary local disk because Spark and DuckDB already work
with local paths. Batch manages the VM lifecycle. It uses an external IP for
Google API access; projects that prohibit external IPs need Cloud NAT or a
reviewed private-egress setup. Standard Batch jobs are not free.

I inspect the job until its state is `SUCCEEDED` or `FAILED`, and use Logs Explorer
with `resource.type="batch_task"` for its task output. Exit 2 means a quality
rejection. Other nonzero exits indicate a processing, storage or setup error.

## Read a report and reuse an earlier snapshot

The submit command prints `/reports/DATE/RUN_ID`. The report service stays private.
I can view it locally through Google's authenticated proxy:

```sh
gcloud run services proxy dpd-report --project "$PROJECT_ID" --region "$REGION" --port 8080
# In my browser: http://localhost:8080/reports/2026-11-01/sample-001
# Append .json to download the same report in a machine-readable format.
# Ctrl-C stops the local proxy when I finish.
```

My caller needs `roles/run.invoker` on this service; the deploying identity normally
already has invocation permission. The service reads only report objects, and
returns 404 for a missing report and 503 if storage cannot be read. It does not list
the bucket or accept uploads. Failed input staging can leave only `started.json`;
in that case I read the Batch logs rather than expect a data-quality report.

Outputs live under `runs/DATE/RUN_ID/outputs/`: Parquet tables, the DuckDB database
and the original check and review reports. A separate `report.json` adds attempt
metadata for the web viewer. Only `complete.json` confirms all outputs uploaded.
An upload error can leave partial files, but never a completion record.
Rejected data gets a report and no new warehouse. I never treat a passing check
report alone as a completed job.

To retain database history and compare the next date's count with an accepted run:

```sh
export BASELINE=runs/2026-11-01/sample-001/complete.json
export SNAPSHOT_DATE=2026-11-08
export RUN_ID=sample-002
# Upload the new date's input files before submitting.
scripts/deploy_gcp.sh submit
```

The baseline must be an earlier completed snapshot in the same bucket. Its database
is restored before processing, so the existing sharp-drop check and historical SQL
tables still work. The new attempt contains the accumulated database and only the
new date's Parquet partitions. Without `BASELINE`, a job starts independent history
and cannot check changes from earlier runs. I keep sample and real-data baselines
separate because their product counts differ.

Each attempt reserves a unique ID with a conditional GCS write. Reusing an ID fails
rather than replacing its files. Automatic task retries are disabled; after a
failure I inspect the logs and submit a new ID. Concurrent attempts have separate
outputs and no shared `latest` pointer to race over. I explicitly choose the next
baseline from a successful attempt.

## Permissions, cost and cleanup

The Batch identity can read bucket objects and create objects under `runs/`, but
cannot overwrite or delete them. The report identity can read only `report.json`
objects under `runs/`. The bucket blocks public access. The repository has no keys,
project credentials or Terraform state. The Cloud Run service requires caller
authentication, scales to zero, and is capped at two instances.

I pay for Batch compute, disks, bucket storage, stored images, Cloud Run requests
and any applicable networking. The one-hour task limit does not bound queue time
or all costs. I watch job state and delete a stuck job explicitly:

```sh
gcloud batch jobs delete "dpd-$RUN_ID" --project "$PROJECT_ID" --location "$REGION"
```

For full teardown, I delete the report service, then remove cloud data and images
that I have decided not to keep, before destroying Terraform resources:

```sh
gcloud run services delete dpd-report --project "$PROJECT_ID" --region "$REGION"
# First back up any wanted gs://$BUCKET/runs/ objects and warehouse databases.
# Empty the bucket and dpd-etl Artifact Registry repository after reviewing their contents.
terraform -chdir=infra/gcp destroy \
  -var="project=$PROJECT_ID" -var="bucket=$BUCKET" -var="region=$REGION"
```

Terraform intentionally refuses to destroy a nonempty bucket. No retention cleanup
runs automatically, so I keep control of historical data and storage costs.

## Validation and references

CI runs the existing pipeline checks and the cloud tests, validates Terraform,
checks permissions with a mocked Terraform plan, parses the generated job with
Google's Batch client schema, builds the container,
runs the real sample inside it, and checks the report's HTTP listener. Local cloud
tests mock only storage; they do not prove real IAM policies, regional capacity or
an actual cloud deployment. CI does not require cloud credentials.

I followed Google's references for [container Batch jobs](https://docs.cloud.google.com/batch/docs/create-run-basic-job),
[custom Batch service accounts](https://docs.cloud.google.com/batch/docs/create-run-job-custom-service-account),
and [Cloud Run's container contract](https://docs.cloud.google.com/run/docs/container-contract).
