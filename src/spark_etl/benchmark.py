"""Show the effect of common Spark optimisations on the silver layer.

1. Partition pruning: filter on trade_date reads one directory instead of all.
2. Column pruning: Parquet is columnar, so selecting 2 columns skips the rest.
3. Broadcast vs shuffle join against the instrument reference table.
"""
from __future__ import annotations

import argparse
import time

from pyspark.sql import functions as F

from spark_etl.session import build_session


def timed(label: str, fn) -> float:
    t0 = time.perf_counter()
    fn()
    dt = time.perf_counter() - t0
    print(f"{label:<45} {dt:6.2f}s")
    return dt


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--lake", default="data/lake")
    p.add_argument("--raw", default="data/raw")
    a = p.parse_args()
    spark = build_session("benchmark")
    silver = spark.read.parquet(f"{a.lake}/silver/trades")
    first_date = silver.select(F.min("trade_date")).first()[0]
    ref = spark.read.csv(f"{a.raw}/instruments.csv", header=True, inferSchema=True).withColumnRenamed("sector", "ref_sector")

    timed("full scan, all columns", lambda: silver.agg(F.sum("notional")).collect())
    timed("partition-pruned scan (one trade_date)", lambda: silver.filter(F.col("trade_date") == first_date).agg(F.sum("notional")).collect())
    timed("column-pruned scan (symbol, notional)", lambda: silver.select("symbol", "notional").groupBy("symbol").sum().collect())

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    timed("join, shuffle (broadcast disabled)", lambda: silver.join(ref, "symbol").groupBy("ref_sector").count().collect())
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
    timed("join, broadcast hint", lambda: silver.join(F.broadcast(ref), "symbol").groupBy("ref_sector").count().collect())

    print("\nPhysical plan for the pruned scan (look for PartitionFilters):")
    silver.filter(F.col("trade_date") == first_date).select("symbol", "notional").explain()
    spark.stop()


if __name__ == "__main__":
    main()
