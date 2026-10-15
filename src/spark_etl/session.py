from pyspark.sql import SparkSession


def build_session(app_name: str = "spark-parquet-etl", shuffle_partitions: int = 8) -> SparkSession:
    """Local session with the tuning settings the pipeline relies on."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Adaptive Query Execution: coalesces small shuffle partitions and splits skewed ones.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
