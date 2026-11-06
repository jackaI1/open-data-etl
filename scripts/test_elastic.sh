#!/usr/bin/env bash
# Own one Compose project and always release it, even when a test fails.
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON=${PYTHON:-.venv/bin/python}
project="dpd-elastic-test-$$"
compose=(docker compose -p "$project" -f elastic/compose.yml)
export ES_PORT=${ES_PORT:-19200} KIBANA_PORT=${KIBANA_PORT:-15601}
export ELASTICSEARCH_URL="http://127.0.0.1:$ES_PORT" KIBANA_URL="http://127.0.0.1:$KIBANA_PORT"
cleanup() {
  result=$?
  trap - EXIT INT TERM
  if [ "$result" -ne 0 ]; then
    "${compose[@]}" logs --tail=30 || true
  fi
  ids=$("${compose[@]}" ps -aq)
  "${compose[@]}" down --volumes --remove-orphans
  for id in $ids; do
    if docker inspect "$id" >/dev/null 2>&1; then
      echo "Owned container still exists: $id" >&2
      result=1
    fi
  done
  "$PYTHON" - "$ES_PORT" "$KIBANA_PORT" <<'PYPORT' || result=1
import socket
import sys
for port in map(int, sys.argv[1:]):
    with socket.socket() as check:
        assert check.connect_ex(('127.0.0.1', port)) != 0, f'port {port} is still open'
print('Verified both owned host ports are closed')
PYPORT
  echo "Removed owned project=$project; ports=$ES_PORT,$KIBANA_PORT; exit=$result"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"${compose[@]}" up -d --wait --wait-timeout 600
for id in $("${compose[@]}" ps -q); do
  docker inspect --format 'Owned container={{.Id}} pid={{.State.Pid}} started={{.State.StartedAt}} ports={{json .NetworkSettings.Ports}}' "$id"
done
echo "Cleanup: docker compose -p $project -f elastic/compose.yml down --volumes"
"$PYTHON" -m pytest -q tests/test_elastic.py tests/test_elastic_integration.py "$@"
