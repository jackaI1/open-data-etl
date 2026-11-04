PYTHON ?= .venv/bin/python
SNAPSHOT_DATE ?= $(shell date -u +%F)
DATA_DIR ?= data

.PHONY: install test lint fixture extract transform pipeline insights
install:
	uv venv --python 3.12 .venv
	uv pip install --python $(PYTHON) -e '.[dev]'
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
