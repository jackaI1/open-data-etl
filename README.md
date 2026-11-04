# open-data-etl

I built a local copy of Canada's drug catalogue because I wanted to look up products and their history without piecing together several files each time.

## Overview

Health Canada's catalogue separates products, ingredients, companies and status
changes into different files. I wanted to explore them together and keep the
version I had downloaded, rather than repeat the joins whenever I had a question.

I download the marketed-product catalogue, clean the records and connect them
into tables I can search on my computer. I also get a review list of possible
duplicate products and unusual ingredient strengths, so I can check suspicious
entries before comparing counts. The original records stay available.
Each run keeps a dated snapshot, a copy of the data for that date. Checks stop a run when required data is missing
or product counts drop sharply. I can start with the included sample below
before downloading the current catalogue.

This is a personal project for exploring Canada's drug catalogue and keeping a
queryable copy of its history. It uses public product and company data.

## Run locally

Install Python 3.10 to 3.12, Java 17, curl, Make and
[uv](https://docs.astral.sh/uv/). See [local setup](docs/pipeline.md#local-setup)
for the Java configuration on macOS. Run these commands from the repository root:

```sh
make install
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
make fixture   # Try the included 60-product sample without a download.
make evaluate  # Check duplicate and unusual-strength detection against known examples.
make pipeline  # Download and process the current marketed-product catalogue.
make insights  # Print a few summaries of the downloaded data.
```

The full run writes a queryable database to `data/warehouse/dpd.duckdb`, dated
tables under `data/parquet/` and a check report under `data/reports/`.
The sample writes its results to `data/fixture/`. The evaluation writes precision
and recall, how many flags were right and how many known cases were found, to
`data/reports/detection_evaluation.json`. The [review guide](docs/detection.md)
shows how to inspect the duplicate links and strength flags with example queries.
Rerunning a date replaces that snapshot while preserving other dates. A weekly workflow can also download new
snapshots; the [pipeline reference](docs/pipeline.md) explains its storage limits.

## How it works

I use PySpark, Python's interface to the Spark table-processing engine, to clean
and join the source records. This is an ETL pipeline: extract the files,
transform their contents and load the results. Status changes become dated
intervals, so I can query which status applied during a period.

Parquet files store the dated tables in a compact format. DuckDB, a database
that runs locally, lets me query them with SQL, a language for asking questions
of tables. Quality checks run before either output is written. A failed database
load rolls back its changes, but the table files and database are separate
stores, so recovery can require rerunning that date.

## Check the project

```sh
make lint test
```

The tests use a small public-data sample and cover cleaning, history intervals,
failed downloads, rejected bad data, repeated runs, duplicate decisions and
unusual-strength flags. The labelled examples include cases the detector misses;
I report those misses alongside successful detections. The [pipeline reference](docs/pipeline.md)
contains the diagram, table descriptions, example queries, measured data
summaries and source links. Only marketed products are included; the approved,
cancelled and dormant catalogues are separate upstream downloads.
