#!/usr/bin/env bash
set -euo pipefail
PYTHON="${PYTHON:-.venv/bin/python}"
export SUPERSET_SECRET_KEY="${SUPERSET_SECRET_KEY:-$($PYTHON -c 'import secrets; print(secrets.token_hex(32))')}"
export SUPERSET_ADMIN_PASSWORD="${SUPERSET_ADMIN_PASSWORD:-$($PYTHON -c 'import secrets; print(secrets.token_urlsafe(24))')}"
compose=(docker compose -p dpd-superset-test -f superset/compose.yml)
cleanup() {
  if [[ "$?" != 0 ]]; then "${compose[@]}" logs --tail 60; fi
  "${compose[@]}" down --volumes --remove-orphans
}
trap cleanup EXIT
"$PYTHON" scripts/seed_dashboard.py
"${compose[@]}" up -d --wait --wait-timeout 300
# Importing twice must preserve object identity rather than create duplicates.
"${compose[@]}" exec -T superset python /app/dashboard/import_dashboard.py
"${compose[@]}" exec -T superset superset export-dashboards -f /tmp/roundtrip.zip
"${compose[@]}" cp superset:/tmp/roundtrip.zip data/superset/roundtrip.zip
"$PYTHON" scripts/check_superset.py
