.PHONY: install run report maintain reprocess test lint clean

PY ?= python
DAYS ?= 14

install:
	$(PY) -m pip install -r requirements.txt

run:          ## generate DAYS of landing files and process them day by day
	$(PY) -m lakehouse run --days $(DAYS)

reprocess:    ## re-run one day to see idempotency, e.g. make reprocess DATE=2026-01-05
	$(PY) -m lakehouse process --date $(DATE)

report:
	$(PY) -m lakehouse report

maintain:     ## OPTIMIZE, Z-ORDER and VACUUM every table
	$(PY) -m lakehouse maintain

test:
	$(PY) -m pytest

lint:
	ruff check .

clean:
	rm -rf lake spark-warehouse metastore_db derby.log .pytest_cache
