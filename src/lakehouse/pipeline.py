"""CLI entry point: python -m lakehouse.pipeline --raw data/raw --lake data/lake"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import functions as F

from . import bronze, gold, silver
from .spark import get_spark

log = logging.getLogger("lakehouse")

CLEANERS = {
    "customers": silver.clean_customers,
    "products": silver.clean_products,
    "orders": silver.clean_orders,
    "order_items": silver.clean_order_items,
}


def run(raw: str, lake: str, fmt: str = "parquet", batch_id: str | None = None) -> dict:
    spark = get_spark("medallion-pipeline", use_delta=(fmt == "delta"))
    batch_id = batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    metrics: dict = {"batch_id": batch_id}

    # ---- Bronze
    metrics["bronze"] = bronze.ingest(spark, raw, f"{lake}/bronze", batch_id, fmt)

    # ---- Silver (only the current batch is processed)
    silver_tables = {}
    metrics["silver"], metrics["quarantine"] = {}, {}
    for name, cleaner in CLEANERS.items():
        src = spark.read.format(fmt).load(f"{lake}/bronze/{name}").filter(
            f"_batch_id = '{batch_id}'")
        valid, bad = cleaner(src)
        valid.write.mode("overwrite").format(fmt).save(f"{lake}/silver/{name}")
        bad.write.mode("append").format(fmt).save(f"{lake}/quarantine/{name}")
        silver_tables[name] = spark.read.format(fmt).load(f"{lake}/silver/{name}")
        metrics["silver"][name] = silver_tables[name].count()
        metrics["quarantine"][name] = bad.count()

    # ---- Gold
    fact = gold.fact_sales(silver_tables["orders"], silver_tables["order_items"],
                           silver_tables["products"])
    fact.write.mode("overwrite").format(fmt).partitionBy("order_date").save(f"{lake}/gold/fact_sales")
    # partition values come back as strings (type inference is off), so restore the date type
    fact = (spark.read.format(fmt).load(f"{lake}/gold/fact_sales")
            .withColumn("order_date", F.to_date("order_date")))
    outputs = {
        "daily_sales": gold.daily_sales(fact),
        "category_performance": gold.category_performance(fact),
        "customer_rfm": gold.customer_rfm(fact),
    }
    for name, df in outputs.items():
        df.write.mode("overwrite").format(fmt).save(f"{lake}/gold/{name}")
    metrics["gold"] = {n: spark.read.format(fmt).load(f"{lake}/gold/{n}").count()
                       for n in ["fact_sales", *outputs]}
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--lake", default="data/lake")
    parser.add_argument("--format", default="parquet", choices=["parquet", "delta"])
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    metrics = run(args.raw, args.lake, args.format)
    for layer in ("bronze", "silver", "quarantine", "gold"):
        log.info("%-10s %s", layer, metrics[layer])


if __name__ == "__main__":
    main()
