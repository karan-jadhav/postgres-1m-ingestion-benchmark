PY := uv run python

DATASET ?= data/gharchive_events_1m.csv
ROWS ?= 1000000
RUNS ?= 3
METHODS ?=
BATCH_SIZE ?= 7000
POSTGRES_IMAGE ?= postgres:18
OUTPUT_DIR ?= results
START_HOUR ?= 2025-01-01-0
RAW_DIR ?= data/gharchive/raw
MAX_HOURS ?= 240

.PHONY: help sync schema methods pull download generate run run-methods cleanup compile smoke-data smoke-run smoke bench

help:
	@echo "PostgreSQL ingestion benchmark"
	@echo ""
	@echo "Common commands:"
	@echo "  make sync                 Install dependencies with uv"
	@echo "  make pull                 Pull configured PostgreSQL image"
	@echo "  make download             Download GH Archive CSV to DATASET"
	@echo "  make run                  Run benchmark for DATASET"
	@echo "  make run-methods METHODS=copy_no_indexes,raw_landing_copy"
	@echo "  make smoke                Generate 100k smoke data and run once"
	@echo "  make cleanup              Remove leftover Docker resources"
	@echo ""
	@echo "Useful variables:"
	@echo "  DATASET=$(DATASET)"
	@echo "  ROWS=$(ROWS)"
	@echo "  RUNS=$(RUNS)"
	@echo "  OUTPUT_DIR=$(OUTPUT_DIR)"

sync:
	uv sync

schema:
	$(PY) main.py schema

methods:
	$(PY) main.py list-methods

pull:
	docker pull $(POSTGRES_IMAGE)

download:
	$(PY) main.py download-gharchive $(DATASET) --rows $(ROWS) --start-hour $(START_HOUR) --raw-dir $(RAW_DIR) --max-hours $(MAX_HOURS)

generate:
	$(PY) main.py generate-data $(DATASET) --rows $(ROWS)

run:
	$(PY) main.py run $(DATASET) --runs $(RUNS) --batch-size $(BATCH_SIZE) --postgres-image $(POSTGRES_IMAGE) --output-dir $(OUTPUT_DIR)

run-methods:
	@if [ -z "$(METHODS)" ]; then echo "Set METHODS, for example: make run-methods METHODS=copy_no_indexes,raw_landing_copy"; exit 1; fi
	$(PY) main.py run $(DATASET) --runs $(RUNS) --methods $(METHODS) --batch-size $(BATCH_SIZE) --postgres-image $(POSTGRES_IMAGE) --output-dir $(OUTPUT_DIR)

cleanup:
	$(PY) main.py cleanup

compile:
	$(PY) -m compileall ingestion_benchmark main.py

smoke-data:
	$(MAKE) generate DATASET=data/events_smoke.csv ROWS=100000

smoke-run:
	$(MAKE) run DATASET=data/events_smoke.csv RUNS=1 OUTPUT_DIR=results/smoke

smoke: smoke-data smoke-run

bench: download run
