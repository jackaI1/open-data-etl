#!/usr/bin/env bash
# Usage: scripts/deploy_gcp.sh deploy|submit|render
set -euo pipefail
cd "$(dirname "$0")/.."
action="${1:-render}"
: "${PROJECT_ID:?Set PROJECT_ID to your GCP project ID}"
: "${BUCKET:?Set BUCKET to a globally unique bucket name}"
REGION="${REGION:-us-central1}"
PYTHON="${PYTHON:-.venv/bin/python}"
SNAPSHOT_DATE="${SNAPSHOT_DATE:-$(date -u +%F)}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dt%H%M%Sz)-${RANDOM}}"
RAW_PREFIX="${RAW_PREFIX:-raw/$SNAPSHOT_DATE}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short=12 HEAD)}"
image="$REGION-docker.pkg.dev/$PROJECT_ID/dpd-etl/batch:$IMAGE_TAG"
report_image="$REGION-docker.pkg.dev/$PROJECT_ID/dpd-etl/report:$IMAGE_TAG"
args=(--project "$PROJECT_ID" --region "$REGION" --bucket "$BUCKET" --image "$image"
      --snapshot "$SNAPSHOT_DATE" --run-id "$RUN_ID" --raw-prefix "$RAW_PREFIX"
      --output data/gcp/job.json)
if [[ "${FIXTURE:-false}" == true ]]; then args+=(--fixture); fi
if [[ -n "${BASELINE:-}" ]]; then args+=(--baseline "$BASELINE"); fi
# Render first so malformed dates and IDs fail before any resources are changed.
"$PYTHON" infra/gcp/render.py "${args[@]}"
case "$action" in
  render) ;;
  deploy)
    terraform -chdir=infra/gcp init -input=false
    terraform -chdir=infra/gcp apply -input=false \
      -var="project=$PROJECT_ID" -var="bucket=$BUCKET" -var="region=$REGION"
    gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet
    docker build --platform linux/amd64 --target batch -t "$image" .
    docker push "$image"
    docker build --platform linux/amd64 --target report -t "$report_image" .
    docker push "$report_image"
    gcloud run deploy dpd-report --project "$PROJECT_ID" --region "$REGION" \
      --image "$report_image" --service-account "dpd-report@$PROJECT_ID.iam.gserviceaccount.com" \
      --set-env-vars "DPD_BUCKET=$BUCKET" --port 8080 --memory 512Mi --cpu 1 \
      --min-instances 0 --max-instances 2 --concurrency 20 --timeout 60 \
      --no-allow-unauthenticated --quiet
    ;;
  submit)
    gcloud batch jobs submit "dpd-$RUN_ID" --project "$PROJECT_ID" \
      --location "$REGION" --config data/gcp/job.json
    echo "Report path: /reports/$SNAPSHOT_DATE/$RUN_ID"
    echo "Completion object: gs://$BUCKET/runs/$SNAPSHOT_DATE/$RUN_ID/complete.json"
    ;;
  *) echo "Usage: $0 deploy|submit|render" >&2; exit 2 ;;
esac
