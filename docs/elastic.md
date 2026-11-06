# Searching and reviewing catalogue snapshots

I use Elasticsearch to search saved product records and Kibana to compare dates
on a dashboard. Elasticsearch stores searchable documents, and Kibana displays
charts and review tables. Together they give me a free local alternative to the
search and dashboard work I would do in Splunk. This project does not use Splunk
or claim that the products have identical features.

## Try the sample

I need Python 3.10 to 3.12, Java 17, uv, Make, curl and Docker with Compose.
On a Mac, I can use Docker Desktop or `brew install colima docker docker-compose`
and start Colima with at least 4 GiB of memory. I configure the Compose plugin
as described by Homebrew if `docker compose version` is unavailable.
The first start downloads two large images and can take several minutes.

```sh
make install
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
# On macOS with Homebrew Java:
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
make fixture SNAPSHOT_DATE=2026-11-01
make elastic-up
make elastic-load DATA_DIR=data/fixture SNAPSHOT_DATE=2026-11-01
make elastic-import
```

I open <http://127.0.0.1:15601/app/dashboards#/view/dpd-quality>.
The dashboard uses the last year by default. For older saved snapshots I choose
an absolute date range that includes them, then click **Refresh**.
I keep Kibana in UTC so calendar dates stay correct in every browser timezone.
The top charts count flagged ingredient rows and matched duplicate pairs per
snapshot date. Each pair counts once, not once per product. The tables below
show the flagged strengths, matched pairs, and searchable product catalogue.
I can open the saved searches from Discover to filter records or choose columns.
I use `drug_code` to find a flagged product in the catalogue search and `left_id`
and `right_id` to look up both sides of a pair.

The 60-product public sample may show zero flags. Zero is a valid result, not a
broken dashboard. I keep a summary document even when there are no review rows.
The integration tests deliberately create one extreme strength and one matched
pair, then clear both on an older date and verify that the newer date retains
its counts. These invented examples test the plumbing, not drug safety.
A flag asks me to review a record; it is not a clinical conclusion.

To compare a second snapshot, I process and export another date:

```sh
make fixture SNAPSHOT_DATE=2026-11-02
make elastic-load DATA_DIR=data/fixture SNAPSHOT_DATE=2026-11-02
# For the full current catalogue instead:
make pipeline
make elastic-load  # uses data/ and today's UTC date, like make pipeline
```

Repeating the same date replaces that date's searchable copy, including deleted
products and cleared flags. Other dates remain available. To finish:

```sh
make elastic-down  # removes this Compose project's containers and stored search data
# If I started a dedicated Colima profile, I stop it separately with colima stop --profile NAME.
```

My Parquet files, database and check reports remain available for another export.
I run `down` without `--volumes` if I deliberately want to retain the search data.
I do not run the test script against a preview I want to keep: it owns a separate
Compose project but needs the same host ports to be free.

## What gets indexed

`python -m dpd_etl.elastic` reads the accepted DuckDB tables and that date's
`dq_YYYY-MM-DD.json`. It refuses an empty product snapshot or any failed quality
check. It does not export a rejected run. Existing local, cloud and scheduled
pipeline commands keep working without Elasticsearch. After a successful daily
run I can call the export command as a separate step and retry it independently.

Each document has `snapshot_date` and `kind`:

| Kind | Contents | Dashboard use |
| --- | --- | --- |
| `product` | Product code, DIN, name, company, status, ingredient text, ATC group | Catalogue search |
| `strength` | Ingredient, original strength/unit, numeric strength, z-score and flag | Flagged-strength search |
| `duplicate_pair` | Both product codes, score, decision and reason | Matched-pair search |
| `quality_check` | Check name, pass/fail, measured value and threshold | Discover query `kind: quality_check` |
| `summary` | Product, flagged-strength and matched-pair counts | Date charts, including zero counts |

The mapping in `src/dpd_etl/elastic.py` defines the field types before loading.
DINs stay text so leading zeroes survive. Dates are strict calendar dates;
measurements are numeric and flags are booleans. Unexpected fields fail instead
of silently changing the schema. Non-finite strength values become null, while
the original strength text stays visible. I project selected fields rather than
indexing every source column. Original records stay in the existing outputs.

## Reruns and failure recovery

I read rows in batches and send newline-delimited JSON to the bulk API, which
loads many documents in one HTTP request. IDs use the ordered row position and a
content hash, so identical inputs keep the same IDs. Each request contains at
most 500 records by default. I check every returned item, since HTTP success
alone does not mean each document was accepted. A completed refresh and document
count check make the staged data searchable before publication.

A new physical index is named `dpd-YYYY-MM-DD-<random generation>`. The stable
`dpd-snapshots` alias, a name pointing to the published indexes, changes in one
atomic API call. I remove the previous generation of that date and add its new
generation together. After publication I delete the old index. This clears stale
records while preserving earlier and later dates. Failure before publication
removes the temporary index and leaves the old alias intact.

I serialize exports of the same date. This is a local single-writer tool, not a
distributed locking system. If a network timeout interrupts the alias update,
the server might already have applied it. I retain the generation rather than
risk deleting published data. I check `GET /_alias/dpd-snapshots` and rerun the
export; unreferenced generations can be removed by exact index name after
checking the alias. A crash can also leave an unreferenced generation. Disk usage
grows with retained dates; I choose which old dated indexes to remove.

The local stack pins Elasticsearch and Kibana to **8.19.6**, with no paid feature
or account required. Both services listen only on loopback; authentication is
disabled for this local setup. I do not expose these ports publicly. For a secured
external Elasticsearch service I set `ELASTICSEARCH_URL` to an HTTPS endpoint and
`ELASTIC_API_KEY` in the environment. I keep keys out of commands and files.
Custom `--prefix` exports need a matching Kibana data view; the shipped dashboard
uses `dpd-snapshots`. If I import the export into another Kibana instance, I also
set its Advanced Settings time zone (`dateFormat:tz`) to **UTC** so a midnight
snapshot is not shown under the previous day in Toronto. The included
`elastic/kibana.yml` locks this setting for the local stack. Host ports can be changed with `ES_PORT` and `KIBANA_PORT`;
I then set the export URL and adjust the curl/browser URL as well.

## Dashboard artifact and checks

`elastic/dashboard.ndjson` is a Kibana saved-object export: one data view,
two date charts, three saved searches and one dashboard. It was imported into
and exported back out of the pinned Kibana server. The data view, a description
of searchable fields, uses `snapshot_date` as its time field. The charts sum only
summary documents, avoiding extra counts from multiple ingredients or checks.
Global dashboard filters should use shared fields such as `snapshot_date`;
product-specific filters belong in Discover because summary documents do not
carry individual product fields.

```sh
make lint test
# Real stack tests, with automatic cleanup even on failure:
ELASTIC_SAMPLE_DIR=data/fixture scripts/test_elastic.sh
```

The optional sample integration test expects `2026-11-01`, so I first run
`make fixture SNAPSHOT_DATE=2026-11-01`. Without `ELASTIC_SAMPLE_DIR`, the stack
suite still tests small controlled DuckDB snapshots. Ordinary `make test` skips
the container tests unless their service URLs are set. CI always starts the
pinned real servers, exports the 60-product pipeline sample twice, tests stable
IDs, mapped search results, multi-date aggregation, cleared flags and rejected
bulk items, and imports/exports all seven Kibana objects with their references.
The test script records container ownership and start times, then removes only
its own Compose project and volume. It never stops unrelated Docker containers.

API references: [bulk indexing](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-bulk-1),
[aliases](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/aliases.html),
and [saved object import](https://www.elastic.co/docs/api/doc/kibana/v8/operation/operation-importsavedobjectsdefault).
