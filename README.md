# open-data-etl

A local pipeline for exploring Health Canada's marketed drug products and their status history.

## Overview

Health Canada's drug catalogue is split across files for products, ingredients,
companies and status changes. Looking up a product's ingredients or following
its history means connecting those files and checking that the records line up.

This project downloads the marketed-product data, cleans it and joins the
related records into tables that can be queried locally. Each run keeps a dated
snapshot, so I can revisit an earlier download without repeating the preparation.
Checks stop a run when required data is missing or product counts drop sharply.

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
make pipeline  # Download and process the current marketed-product catalogue.
make insights  # Print a few summaries of the downloaded data.
```

The full run writes a queryable database to `data/warehouse/dpd.duckdb`, dated
tables under `data/parquet/` and a check report under `data/reports/`.
The sample writes its results to `data/fixture/`. Rerunning a date replaces that
snapshot while preserving other dates. A weekly workflow can also download new
snapshots; the [pipeline reference](docs/pipeline.md) explains its storage limits.

## Check the project

```sh
make lint test
```

The tests use a small public-data sample and cover cleaning, history intervals,
failed downloads, rejected bad data and repeated runs. The [pipeline reference](docs/pipeline.md)
contains the diagram, table descriptions, example queries, measured data
summaries and source links. Only marketed products are included; the approved,
cancelled and dormant catalogues are separate upstream downloads.
