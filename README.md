# spark-parquet-etl

A PySpark batch pipeline that turns a messy, skewed trade feed into clean, partitioned Parquet and daily market statistics.

## Posting skills this demonstrates

From JPMorgan Chase's **Lead Data Engineer** posting:

| Posting asks for | Where it shows up here |
| --- | --- |
| Strong proficiency in Python and PySpark | The whole pipeline, `src/spark_etl/pipeline.py` |
| Big Data technologies, with a focus on performance optimization | AQE, broadcast joins, explicit schemas, caching, partition and column pruning, plus `benchmark.py` to measure them |
| Solid understanding of the Parquet file format | zstd-compressed Parquet partitioned by `trade_date`; the benchmark prints the `PartitionFilters` in the physical plan |
| Scalable data architectures and pipelines | A bronze, silver and gold (medallion) layout with a quarantine area for rejected rows |
| Data quality and integrity | Typed validation with a reject reason on every bad row, de-duplication of resent trades, and a row-count reconciliation test |
| Git and CI/CD for data engineering workflows | `.github/workflows/ci.yml` runs the Spark test suite on every push |

## What it does

```
data/raw/trades.csv ──► bronze (explicit schema) ──► typed + validated ──┬──► silver/trades        (Parquet, partitioned by trade_date)
data/raw/instruments.csv ─────────(broadcast join)────────────────────────┤
                                                                          ├──► quarantine/trades    (rows that failed, with reject_reason)
                                                                          └──► gold/daily_symbol_stats  (OHLC, volume, VWAP, buy/sell imbalance)
```

The generator deliberately makes the feed hard:

* **Skew:** two symbols carry about 70% of the trades.
* **Duplicates:** about 2% of trades are delivered twice.
* **Bad rows:** unparseable prices, negative quantities and unknown symbols.

## Run it

Requires Python 3.10+ and Java 17+.

```bash
pip install -e ".[dev]"
make data        # 200k synthetic trades over 5 days
make run         # writes data/lake/{silver,gold,quarantine}
make benchmark   # times pruning and join strategies, prints the physical plan
make test
```

Sample run on a laptop (200k trades):

```
RunStats(raw_rows=203999, silver_rows=194927, rejected_rows=5176, duplicates_removed=3896, gold_rows=40)

full scan, all columns                           0.71s
partition-pruned scan (one trade_date)           0.38s
join, shuffle (broadcast disabled)               1.72s
join, broadcast hint                             0.96s
```

The reconciliation always holds: `silver + rejected + duplicates == raw`. The tests assert it.

## Design notes

* **Explicit read schema.** Everything is read as a string, then cast with `try_cast`. A bad value becomes a quarantined row instead of a failed job, and Spark never runs a schema-inference pass over a large file.
* **Broadcast the reference data.** The instrument table is tiny, so broadcasting it means the skewed trade side never shuffles for the join. AQE skew-join handling stays on for the cases where both sides are large.
* **Dynamic partition overwrite.** Re-running a single day replaces only that day's partition. That makes backfills idempotent.
* **Quarantine instead of drop.** Every rejected row keeps a `reject_reason`, so data owners can see what went wrong upstream.
