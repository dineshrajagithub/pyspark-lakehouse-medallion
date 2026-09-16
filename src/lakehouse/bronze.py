"""Bronze: land raw files as-is (all strings) plus ingestion metadata."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SOURCES = ("customers", "products", "orders", "order_items")


def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    # Schema-on-read: keep every column as string so bad values are never lost.
    return spark.read.option("header", True).option("inferSchema", False).csv(path)


def add_ingestion_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def ingest(spark: SparkSession, raw_dir: str, bronze_dir: str, batch_id: str,
           fmt: str = "parquet") -> dict[str, int]:
    counts = {}
    for name in SOURCES:
        df = add_ingestion_metadata(read_raw_csv(spark, f"{raw_dir}/{name}.csv"), batch_id)
        df.write.mode("append").format(fmt).partitionBy("_batch_id").save(f"{bronze_dir}/{name}")
        counts[name] = df.count()
    return counts
