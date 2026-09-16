"""Silver: typed, cleaned, de-duplicated, conformed data.

Invalid rows are not silently dropped: they are routed to a quarantine
table with the reason, which is how production pipelines stay auditable."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def _latest(df: DataFrame, key: str, order_col: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def split_valid(df: DataFrame, rules: dict[str, Column]) -> tuple[DataFrame, DataFrame]:
    """rules: {reason: condition_that_must_be_true}. Returns (valid, quarantined)."""
    reasons = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(reason)) for reason, cond in rules.items()]
    tagged = df.withColumn("_dq_errors", F.array_compact(F.array(*reasons)))
    valid = tagged.filter(F.size("_dq_errors") == 0).drop("_dq_errors")
    quarantined = tagged.filter(F.size("_dq_errors") > 0)
    return valid, quarantined


def clean_customers(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = bronze.select(
        F.trim("customer_id").alias("customer_id"),
        F.initcap(F.trim("full_name")).alias("full_name"),
        F.lower(F.trim("email")).alias("email"),
        F.initcap(F.trim("city")).alias("city"),
        F.upper(F.trim("state")).alias("state"),
        F.to_date("signup_date").alias("signup_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
    )
    valid, bad = split_valid(typed, {
        "missing_customer_id": F.col("customer_id").isNotNull() & (F.col("customer_id") != ""),
        "invalid_email": F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"),
    })
    return _latest(valid, "customer_id", "updated_at"), bad


def clean_products(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = bronze.select(
        F.trim("product_id").alias("product_id"),
        F.trim("product_name").alias("product_name"),
        F.initcap(F.trim("category")).alias("category"),
        F.col("list_price").cast("decimal(10,2)").alias("list_price"),
    )
    valid, bad = split_valid(typed, {
        "missing_product_id": F.col("product_id").isNotNull(),
        "invalid_price": F.col("list_price") >= 0,
    })
    return valid.dropDuplicates(["product_id"]), bad


def clean_orders(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = bronze.select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.to_timestamp("order_ts").alias("order_ts"),
        F.lower(F.trim("status")).alias("status"),
    )
    valid, bad = split_valid(typed, {
        "missing_order_id": F.col("order_id").isNotNull(),
        "invalid_timestamp": F.col("order_ts").isNotNull(),
        "unknown_status": F.col("status").isin("placed", "shipped", "delivered", "cancelled", "returned"),
    })
    return _latest(valid, "order_id", "order_ts").withColumn("order_date", F.to_date("order_ts")), bad


def clean_order_items(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = bronze.select(
        F.trim("order_id").alias("order_id"),
        F.col("line_number").cast("int").alias("line_number"),
        F.trim("product_id").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        F.coalesce(F.col("discount_pct").cast("decimal(5,2)"), F.lit(0).cast("decimal(5,2)"))
        .alias("discount_pct"),
    )
    valid, bad = split_valid(typed, {
        "non_positive_quantity": F.col("quantity") > 0,
        "negative_price": F.col("unit_price") >= 0,
        "discount_out_of_range": F.col("discount_pct").between(0, 100),
    })
    return valid.dropDuplicates(["order_id", "line_number"]), bad
