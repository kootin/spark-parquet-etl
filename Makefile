.PHONY: install data run benchmark test clean
install:
	pip install -e ".[dev]"
data:
	python -m spark_etl.generate --out data/raw --trades 200000
run:
	python -m spark_etl.pipeline --raw data/raw --out data/lake
benchmark:
	python -m spark_etl.benchmark --lake data/lake --raw data/raw
test:
	pytest -q
clean:
	rm -rf data/raw data/lake spark-warehouse
