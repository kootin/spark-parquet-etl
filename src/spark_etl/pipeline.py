"""Bronze -> silver -> gold batch pipeline.

bronze: raw CSV read with an explicit schema (no schema inference on large files)
silver: typed, validated, de-duplicated trades enriched with instrument reference data,
        written as Parquet partitioned by trade_date; rejects are quarantined with a reason
gold:   daily per-symbol OHLC, volume, VWAP and buy/sell imbalance
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_etl.session import build_session

RAW_SCHEMA = T.StructType([
    T.StructField("trade_id", T.StringType()),
    T.StructField("symbol", T.StringType()),
    T.StructField("side", T.StringType()),
    T.StructField("price", T.StringType()),
    T.StructField("quantity", T.StringType()),
    T.StructField("trade_ts", T.StringType()),
    T.StructField("venue", T.StringType()),
])


@dataclass
class RunStats:
    raw_rows: int
    silver_rows: int
    rejected_rows: int
    duplicates_removed: int
    gold_rows: int


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, schema=RAW_SCHEMA)


def to_typed(raw: DataFrame) -> DataFrame:
    return (
        raw.withColumn("price", F.expr("try_cast(price AS DECIMAL(18,4))"))
        .withColumn("quantity", F.expr("try_cast(quantity AS BIGINT)"))
        .withColumn("trade_ts", F.to_timestamp("trade_ts"))
        .withColumn("side", F.upper(F.trim("side")))
        .withColumn("symbol", F.upper(F.trim("symbol")))
    )


def validate(typed: DataFrame, instruments: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (valid, rejected). Rejected rows carry a reject_reason column."""
    known = instruments.select("symbol").withColumn("_known", F.lit(True))
    # Instrument reference is tiny: broadcast it so the skewed trade side never shuffles.
    checked = typed.join(F.broadcast(known), "symbol", "left")
    reason = (
        F.when(F.col("price").isNull(), "invalid_price")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
        .when(F.col("trade_ts").isNull(), "invalid_timestamp")
        .when(~F.col("side").isin("BUY", "SELL"), "invalid_side")
        .when(F.col("_known").isNull(), "unknown_symbol")
    )
    checked = checked.withColumn("reject_reason", reason).drop("_known")
    valid = checked.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = checked.filter(F.col("reject_reason").isNotNull())
    return valid, rejected


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the first delivery of each trade_id (upstream can resend)."""
    w = Window.partitionBy("trade_id").orderBy(F.col("trade_ts").asc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def build_silver(valid: DataFrame, instruments: DataFrame) -> DataFrame:
    return (
        deduplicate(valid)
        .join(F.broadcast(instruments), "symbol", "inner")
        .withColumn("notional", F.col("price") * F.col("quantity"))
        .withColumn("trade_date", F.to_date("trade_ts"))
    )


def build_gold(silver: DataFrame) -> DataFrame:
    w = Window.partitionBy("trade_date", "symbol").orderBy("trade_ts")
    w_all = w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    with_oc = silver.withColumn("_open", F.first("price").over(w_all)).withColumn(
        "_close", F.last("price").over(w_all)
    )
    signed_qty = F.when(F.col("side") == "BUY", F.col("quantity")).otherwise(-F.col("quantity"))
    return (
        with_oc.groupBy("trade_date", "symbol", "sector")
        .agg(
            F.first("_open").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.first("_close").alias("close"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trade_count"),
            (F.sum("notional") / F.sum("quantity")).cast("decimal(18,4)").alias("vwap"),
            F.sum(signed_qty).alias("net_buy_qty"),
        )
        .withColumn("imbalance_pct", F.round(F.col("net_buy_qty") / F.col("volume") * 100, 2))
    )


def run(spark: SparkSession, raw_dir: str, out_dir: str) -> RunStats:
    raw = read_raw(spark, f"{raw_dir}/trades.csv")
    instruments = spark.read.csv(f"{raw_dir}/instruments.csv", header=True, inferSchema=True)

    typed = to_typed(raw).cache()
    valid, rejected = validate(typed, instruments)
    silver = build_silver(valid, instruments).cache()
    gold = build_gold(silver)

    # Partition by date so downstream readers prune whole directories.
    silver.repartition("trade_date").write.mode("overwrite").partitionBy("trade_date").parquet(
        f"{out_dir}/silver/trades"
    )
    rejected.write.mode("overwrite").parquet(f"{out_dir}/quarantine/trades")
    gold.coalesce(1).write.mode("overwrite").partitionBy("trade_date").parquet(f"{out_dir}/gold/daily_symbol_stats")

    raw_n, valid_n, silver_n = raw.count(), valid.count(), silver.count()
    stats = RunStats(raw_n, silver_n, rejected.count(), valid_n - silver_n, gold.count())
    typed.unpersist()
    silver.unpersist()
    return stats


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--out", default="data/lake")
    a = p.parse_args()
    spark = build_session()
    stats = run(spark, a.raw, a.out)
    print(stats)
    spark.read.parquet(f"{a.out}/gold/daily_symbol_stats").orderBy("trade_date", "symbol").show(10)
    spark.stop()


if __name__ == "__main__":
    main()
