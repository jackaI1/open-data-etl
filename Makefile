PYTHON ?= .venv/bin/python
SNAPSHOT_DATE ?= $(shell date -u +%F)
DATA_DIR ?= data

.PHONY: install test lint fixture evaluate extract transform pipeline insights monitor-demo
install:
	uv venv --python 3.12 .venv
	uv pip install --python $(PYTHON) -e '.[dev,gcp,cloud-test,bigquery,mongo,graph]'
test:
	$(PYTHON) -m pytest -q
lint:
	$(PYTHON) -m ruff check .
fixture:
	$(PYTHON) -m dpd_etl transform --fixture --raw-dir tests/fixtures/raw --data-dir $(DATA_DIR)/fixture --snapshot-date $(SNAPSHOT_DATE)
extract:
	$(PYTHON) -m dpd_etl extract --data-dir $(DATA_DIR)
transform:
	$(PYTHON) -m dpd_etl transform --data-dir $(DATA_DIR) --raw-dir $(DATA_DIR)/raw/files --snapshot-date $(SNAPSHOT_DATE)
pipeline: extract
	$(MAKE) transform
insights:
	$(PYTHON) scripts/insights.py $(DATA_DIR)/warehouse/dpd.duckdb

evaluate:
	$(PYTHON) -m dpd_etl.evaluate --output $(DATA_DIR)/reports/detection_evaluation.json

monitor-demo:
	$(PYTHON) scripts/monitor_demo.py

# Airflow's constraints are for Python 3.12, matching `make install` and CI.
.PHONY: airflow-install airflow-test
airflow-install:
	uv pip install --python $(PYTHON) 'apache-airflow==3.3.2' --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
	uv pip install --python $(PYTHON) -e '.[dev,gcp,cloud-test,bigquery,airflow]'
airflow-test:
	$(PYTHON) -c "from airflow.sdk import DAG"
	$(PYTHON) -m pytest -q tests/test_airflow_tasks.py tests/airflow

.PHONY: delta-install delta-fixture delta-test delta
delta-install:
	uv pip install --python $(PYTHON) -e '.[dev,delta]'
delta-fixture:
	$(PYTHON) notebooks/health_canada_delta.py --fixture --raw-dir tests/fixtures/raw --delta-root $(DATA_DIR)/fixture/delta --report-dir $(DATA_DIR)/fixture/reports --snapshot-date $(SNAPSHOT_DATE)
delta:
	$(PYTHON) notebooks/health_canada_delta.py --raw-dir $(DATA_DIR)/raw/files --delta-root $(DATA_DIR)/delta --report-dir $(DATA_DIR)/reports --snapshot-date $(SNAPSHOT_DATE)
delta-test:
	$(PYTHON) -c "import delta"
	$(PYTHON) -m pytest -q tests/delta

.PHONY: lakehouse-versions lakehouse-compare
LAKEHOUSE_ROOT ?= $(DATA_DIR)/lakehouse
OLD ?= 0
NEW ?= 1
lakehouse-versions:
	$(PYTHON) -m dpd_etl.delta_ops --delta-root $(LAKEHOUSE_ROOT) versions
lakehouse-compare:
	$(PYTHON) -m dpd_etl.delta_ops --delta-root $(LAKEHOUSE_ROOT) compare --old-version $(OLD) --new-version $(NEW)

.PHONY: elastic-up elastic-down elastic-load elastic-import elastic-test
elastic-up:
	docker compose -p dpd-elastic -f elastic/compose.yml up -d --wait
elastic-down:
	docker compose -p dpd-elastic -f elastic/compose.yml down --volumes
elastic-load:
	$(PYTHON) -m dpd_etl.elastic --data-dir $(DATA_DIR) --snapshot-date $(SNAPSHOT_DATE)
elastic-import:
	curl --fail --silent --show-error -X POST 'http://127.0.0.1:15601/api/saved_objects/_import?overwrite=true' -H 'kbn-xsrf: true' --form file=@elastic/dashboard.ndjson
elastic-test:
	PYTHON=$(PYTHON) scripts/test_elastic.sh

# Monthly release archive in Azure Blob Storage. Set DPD_ARCHIVE_CONNECTION_STRING first.
ARCHIVE_OUTPUT_DIR ?= $(DATA_DIR)/lakehouse
RESTORE_DIR ?= $(DATA_DIR)/restored
.PHONY: archive-upload archive-list archive-restore archive-test azure-test
archive-upload:
	$(PYTHON) -m dpd_etl.archive upload --snapshot-date $(SNAPSHOT_DATE) --raw-dir $(DATA_DIR)/raw/files --output-dir $(ARCHIVE_OUTPUT_DIR)
archive-list:
	$(PYTHON) -m dpd_etl.archive list
archive-restore:
	$(PYTHON) -m dpd_etl.archive restore --snapshot-date $(SNAPSHOT_DATE) --dest $(RESTORE_DIR)
archive-test:
	$(PYTHON) -m pytest -q tests/azure/test_archive.py
# Archive, summary and timer tests. Set AZURITE_CONNECTION_STRING to run the Azurite cases.
azure-test:
	$(MAKE) fixture PYTHON=$(PYTHON) SNAPSHOT_DATE=2026-11-01
	$(PYTHON) -m pytest -q tests/azure

# Graph neural network duplicate matcher, compared with the rule matcher on held-out pairs.
# The challenge file is committed; `graph-challenge` rebuilds it from the fixture warehouse.
.PHONY: graph-challenge graph-eval graph-test
graph-challenge:
	$(PYTHON) scripts/build_graph_challenge.py --warehouse $(DATA_DIR)/fixture/warehouse/dpd.duckdb --output tests/fixtures/graph_challenge.json
graph-eval:
	$(PYTHON) -m dpd_etl.graph_match --challenge tests/fixtures/graph_challenge.json --output $(DATA_DIR)/reports/graph_match_evaluation.json
graph-test:
	$(PYTHON) -m pytest -q tests/graph
