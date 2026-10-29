# The data window lives in exactly one place: scripts/config.py
PYTHON ?= python3

DBT_VARS := $(shell $(PYTHON) -c "import sys,json; sys.path.insert(0,'scripts'); import config; print(json.dumps({'date_from':config.DATE_FROM.isoformat(),'date_to':config.DATE_TO.isoformat()}))")

.PHONY: help check-vars extract-ohlcv extract-traders seeds load dbt-deps dbt-build build build-offline rebuild clean

help:
	@echo "make build           - full refresh: hit the APIs, then build dbt"
	@echo "make build-offline   - no APIs: use the committed parquet and seeds"
	@echo "make rebuild         - wipe DuckDB, rebuild from committed files"
	@echo "make check-vars      - show the dbt vars derived from config.py"
	@echo "make extract-ohlcv   - download candles (needs KOPYON_API_KEY)"
	@echo "make extract-traders - download trader history (needs KOPYON_API_KEY)"
	@echo "make seeds           - rebuild seed CSVs from raw trader JSON"
	@echo "make load            - load parquet into the DuckDB RAW schema"
	@echo "make dbt-deps        - install dbt packages"
	@echo "make dbt-build       - run dbt build with the window from config.py"
	@echo "make clean           - delete DuckDB and build artifacts"

check-vars:
	@echo "$(DBT_VARS)"

extract-ohlcv:
	$(PYTHON) scripts/extract_data.py

extract-traders:
	$(PYTHON) scripts/extract_traders.py

seeds:
	rm -f dbt_project/seeds/*.csv
	$(PYTHON) scripts/build_seeds.py

load:
	$(PYTHON) scripts/load_data.py

dbt-deps:
	cd dbt_project && dbt deps

dbt-build:
	cd dbt_project && dbt build --vars '$(DBT_VARS)'

build:
	$(PYTHON) scripts/extract_data.py
	$(PYTHON) scripts/extract_traders.py
	rm -f dbt_project/seeds/*.csv
	$(PYTHON) scripts/build_seeds.py
	$(PYTHON) scripts/load_data.py
	cd dbt_project && dbt deps
	cd dbt_project && dbt build --vars '$(DBT_VARS)'

build-offline:
	$(PYTHON) scripts/load_data.py
	cd dbt_project && dbt deps
	cd dbt_project && dbt build --vars '$(DBT_VARS)'

rebuild:
	rm -f dev.duckdb dev.duckdb.wal
	rm -rf dbt_project/target dbt_project/logs
	$(PYTHON) scripts/load_data.py
	cd dbt_project && dbt deps
	cd dbt_project && dbt build --vars '$(DBT_VARS)'

clean:
	rm -f dev.duckdb dev.duckdb.wal
	rm -rf dbt_project/target dbt_project/logs