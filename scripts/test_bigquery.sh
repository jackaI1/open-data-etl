#!/usr/bin/env bash
# Download a pinned emulator into an owned temporary directory, run tests, clean up.
set -euo pipefail
PYTHON="${PYTHON:-.venv/bin/python}"
if [[ -n "${BIGQUERY_EMULATOR_BIN:-}" ]]; then
  exec "$PYTHON" -m pytest -q "$@"
fi
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    platform=linux_amd64
    checksum=8dbe6d3ebe3e8963523bada4a88a012d6a1bbc2ea779fbdc2e31d2e8c8c51c36 ;;
  Darwin-arm64)
    platform=darwin_arm64
    checksum=62bf939aca2c0c2e77be09b7a218723d867592c070389de44fad6d8f73addc67 ;;
  *) echo 'Use BIGQUERY_EMULATOR_BIN with a v0.8.1 binary for this platform.' >&2; exit 1 ;;
esac
work=$(mktemp -d "${TMPDIR:-/tmp}/dpd-bigquery.XXXXXX")
trap 'rm -rf "$work"' EXIT
curl --fail --location --silent --show-error --max-time 180 \
  "https://github.com/goccy/bigquery-emulator/releases/download/v0.8.1/bigquery-emulator_v0.8.1_${platform}.tar.gz" \
  -o "$work/emulator.tar.gz"
"$PYTHON" - "$work/emulator.tar.gz" "$checksum" <<'PY'
import hashlib
import sys
from pathlib import Path
assert hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest() == sys.argv[2], 'checksum mismatch'
PY
tar -xzf "$work/emulator.tar.gz" -C "$work"
export BIGQUERY_EMULATOR_BIN="$work/bigquery-emulator"
# Pytest owns the process handle and closes the emulator even on test failure.
"$PYTHON" -m pytest -q "$@"
