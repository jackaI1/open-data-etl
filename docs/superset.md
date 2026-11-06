# Review catalogue quality in Superset

I use this dashboard to compare saved runs and find the classes with more
ingredient-strength flags. Apache Superset is an open-source charting tool,
serving the same purpose here as a Power BI, Looker or Tableau dashboard.
I built and tested this version in Superset, not those proprietary products.

## Start with example history

On a machine with Docker and Compose, after `make install`:

```sh
.venv/bin/python scripts/seed_dashboard.py
export SUPERSET_SECRET_KEY="$(.venv/bin/python -c 'import secrets; print(secrets.token_hex(32))')"
export SUPERSET_ADMIN_PASSWORD="$(.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(24))')"
docker compose -p dpd-superset -f superset/compose.yml up -d --wait
```

Open <http://127.0.0.1:18088/superset/dashboard/catalogue-quality/>. Sign in as
`admin` with the password in `SUPERSET_ADMIN_PASSWORD`. To see the password in
my terminal I can run `printf '%s\n' "$SUPERSET_ADMIN_PASSWORD"`.
The three invented example dates have nine product snapshots, three flagged
product snapshots, three suggested duplicate pairs and a mean duration of
10.5 seconds. These are examples to exercise the charts, not downloaded results.

The container initializes its metadata database, creates the local administrator
once, then imports the readable files in `superset/assets/`. I can repeat the
import after editing assets:

```sh
docker compose -p dpd-superset -f superset/compose.yml exec -T superset \
  python /app/dashboard/import_dashboard.py
```

The image is pinned to `apache/superset:4.1.2`. It needs about 2 GiB of memory.
The setup has one web worker and SQLite metadata, suitable for a local personal
review. It binds only to this computer. An internet deployment needs a supported
metadata database, HTTPS and proper identity management. Superset runs as root
in this disposable local container to initialize its named volume; it mounts
only the projected catalogue folder read-only, not the original downloads.

## Use accepted pipeline results

The usual pipeline now writes `reports/run_YYYY-MM-DD.json` after a successful
load. It records counts and measured elapsed seconds, including Spark startup,
transformation, checks and writes. A failed rerun removes that date's receipt.
Older outputs need one successful rerun before this dashboard can accept them.

```sh
make fixture SNAPSHOT_DATE=2026-11-01
.venv/bin/python -m dpd_etl.dashboard --data-dir data/fixture \
  --snapshot-date 2026-11-01
# For a full accepted run, use --data-dir data and that run's date.
```

The publisher checks the quality report and receipt against the current DuckDB
tables. It replaces one date in `data/superset/catalogue.db` in one transaction.
Other dates remain available. Importing assets is separate from loading data,
so I do not need to recreate charts after each run. Avoid mixing example and
real dates in the same file when interpreting totals. To start clean, stop the
stack and remove `data/superset/catalogue.db`, then publish only real dates.
The dashboard's runs dataset includes `source_kind` to distinguish examples.
The seed replaces only its three example dates, so use it in a separate file
with `--database` when preserving real data for those dates.

## Read and filter the charts

There are four KPI tiles, four time trends, two drug-class breakdowns and a
pair-review chart. A KPI is one summary number. Across several dates the product
tile counts product snapshots, so the same product can appear once each day.
Flagged records are distinct products with at least one flagged strength;
flagged strengths count ingredient records. Suggested duplicate pairs count
`is_match=true`, while the review-status chart includes all candidate pairs.

The snapshot-date filter applies to every chart. The drug-class filter applies
to the two class charts; the review-status filter applies to the pair chart.
The class-share metric divides flagged products by products in that class.
It is not flagged ingredients divided by products. Aggregating the ingredient
and flag tables before joining prevents multi-ingredient products from
multiplying the counts. Flags are review suggestions, never medical findings.

Every pair starts `pending`. The algorithm's `is_match` is not a human decision.
To record a real review, supply an optional JSON file to the publisher:

```json
[
  {"snapshot_date": "2026-11-01", "left_id": 1, "right_id": 2,
   "review_status": "confirmed"}
]
```

```sh
.venv/bin/python -m dpd_etl.dashboard --data-dir data/fixture \
  --snapshot-date 2026-11-01 --decisions /path/to/my-decisions.json
```

Allowed states are `pending`, `confirmed` and `dismissed`. Use actual pair IDs;
the publisher rejects unknown pairs and duplicate decisions. Keep and supply
this file on every rerun; omitting it resets that date to pending. Decisions
are supplied explicitly, not inferred from detector scores.

## Assets, validation and cleanup

The export contains one database, three datasets and eleven charts. Datasets
specify existing SQL columns and named SQL metrics. Stable UUIDs connect the
charts, layout and filters. `scripts/build_dashboard_assets.py` rebuilds these
YAML files, and `superset/import_dashboard.py` packages them into Superset's ZIP
format for its official import command.

```sh
.venv/bin/python scripts/build_dashboard_assets.py
.venv/bin/python -m pytest -q tests/test_dashboard.py
# Full container import, HTTP queries, filters and export round trip:
uv pip install --python .venv/bin/python playwright==1.56.0
.venv/bin/python -m playwright install chromium
PYTHON=.venv/bin/python scripts/test_superset.sh
# Stop my interactive preview and delete its metadata volume:
docker compose -p dpd-superset -f superset/compose.yml down --volumes
```

The automated script always removes its owned stack, including failure paths.
CI checks every exported metric against expected values, imports twice into a
fresh pinned instance, executes every chart through Superset's HTTP API,
checks date and class filtering, then exports the assets again. The regular
pipeline CI also publishes the real 60-product Spark fixture and compares the
SQLite totals with the source tables. The CI artifact keeps the round-trip ZIP
validation receipt and a Chromium screenshot. CI also opens the imported dashboard
in a browser and rejects failed chart responses. I do not claim production scale or hosted cloud usage.
