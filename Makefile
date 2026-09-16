.PHONY: install data run test lint

install:
	pip install -r requirements-dev.txt && pip install -e .

data:
	python scripts/generate_data.py --out data/raw --customers 2000 --orders 20000

run: data
	python -m lakehouse.pipeline --raw data/raw --lake data/lake --format parquet

test:
	pytest -q

lint:
	ruff check src tests scripts
