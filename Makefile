PYTHON ?= .venv/bin/python
SNAPSHOT_DATE ?= $(shell date -u +%F)
DATA_DIR ?= data

.PHONY: install test lint fixture evaluate extract transform pipeline insights monitor-demo
install:
	uv venv --python 3.12 .venv
	uv pip install --python $(PYTHON) -e '.[dev,gcp,cloud-test,bigquery]'
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
