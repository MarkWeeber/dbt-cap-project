# The data window lives in exactly one place: scripts/config.py
PYTHON ?= python3

DBT_VARS := $(shell $(PYTHON) -c "import sys,json; sys.path.insert(0,'scripts'); import config; print(json.dumps({'date_from':config.DATE_FROM.isoformat(),'date_to':config.DATE_TO.isoformat()}))")

.PHONY: help check-vars extract seeds load dbt-deps dbt-build build build-offline clean

help:
	@echo "make build          - full refresh: hit the APIs, then build dbt"
	@echo "make build-offline  - no APIs: use the committed parquet and seeds"
	@echo "make check-vars     - show the dbt vars derived from config.py"
	@echo "make extract        - download OHLCV + trader data (needs KOPYON_API_KEY)"
	@echo "make seeds          - rebuild seed CSVs from raw trader JSON"
	@echo "make load           - load parquet into the DuckDB RAW schema"
	@echo "make dbt-build      - run dbt build with the window from config.py"
	@echo "make clean          - delete the local DuckDB file"

check-vars:
	@echo "$(DBT_VARS)"

extract:
	$(PYTHON) scripts/extract_data.py
	$(PYTHON) scripts/extract_traders.py

seeds:
	$(PYTHON) scripts/build_seeds.py

load:
	$(PYTHON) scripts/load_data.py

dbt-deps:
	cd dbt_project && dbt deps

dbt-build:
	cd dbt_project && dbt build --vars '$(DBT_VARS)'

build: extract seeds load dbt-deps dbt-build

build-offline: load dbt-deps dbt-build

clean:
	rm -f dev.duckdb dev.duckdb.wal