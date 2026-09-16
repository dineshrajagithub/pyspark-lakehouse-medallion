"""Gold: business-level models for BI and analytics."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

REVENUE_STATUSES = ("placed", "shipped", "delivered")


def fact_sales(orders: DataFrame, items: DataFrame, products: DataFrame) -> DataFrame:
    gross = F.col("quantity") * F.col("unit_price")
    return (
        items.join(orders, "order_id", "inner")
        .join(products.select("product_id", "category"), "product_id", "left")
        .withColumn("gross_amount", gross.cast("decimal(12,2)"))
        .withColumn("discount_amount", (gross * F.col("discount_pct") / 100).cast("decimal(12,2)"))
        .withColumn("net_amount", (F.col("gross_amount") - F.col("discount_amount")).cast("decimal(12,2)"))
        .withColumn("is_revenue", F.col("status").isin(*REVENUE_STATUSES))
        .withColumn("category", F.coalesce("category", F.lit("Unknown")))
        .select("order_id", "line_number", "order_date", "customer_id", "product_id",
                "category", "status", "quantity", "unit_price", "discount_pct",
                "gross_amount", "discount_amount", "net_amount", "is_revenue")
    )


def daily_sales(fact: DataFrame) -> DataFrame:
    rev = fact.filter("is_revenue")
    daily = rev.groupBy("order_date").agg(
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("customer_id").alias("customers"),
        F.sum("quantity").alias("units"),
        F.sum("net_amount").alias("revenue"),
    )
    w7 = Window.orderBy("order_date").rowsBetween(-6, 0)
    return (
        daily.withColumn("avg_order_value", F.round(F.col("revenue") / F.col("orders"), 2))
        .withColumn("revenue_7d_avg", F.round(F.avg("revenue").over(w7), 2))
        .orderBy("order_date")
    )


def category_performance(fact: DataFrame) -> DataFrame:
    by_cat = fact.groupBy("category").agg(
        F.sum(F.when(F.col("is_revenue"), F.col("net_amount")).otherwise(0)).alias("revenue"),
        F.sum(F.when(F.col("status") == "returned", F.col("quantity")).otherwise(0)).alias("returned_units"),
        F.sum("quantity").alias("total_units"),
    )
    total = by_cat.agg(F.sum("revenue").alias("_total_revenue"))
    return (
        by_cat.crossJoin(total)
        .withColumn("revenue_share_pct",
                    F.round(F.col("revenue") * 100 / F.col("_total_revenue"), 2))
        .drop("_total_revenue")
        .withColumn("return_rate_pct",
                    F.round(F.col("returned_units") * 100 / F.col("total_units"), 2))
        .orderBy(F.col("revenue").desc())
    )


def customer_rfm(fact: DataFrame, as_of: str | None = None) -> DataFrame:
    """Recency / Frequency / Monetary segmentation with 1-5 quintile scores."""
    rev = fact.filter("is_revenue")
    if as_of:
        ref = rev.sparkSession.range(1).select(F.to_date(F.lit(as_of)).alias("_ref"))
    else:
        # Reference date = day after the latest order in the data set.
        ref = rev.agg(F.date_add(F.max("order_date"), 1).alias("_ref"))
    base = (
        rev.groupBy("customer_id")
        .agg(
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("order_id").alias("frequency"),
            F.sum("net_amount").alias("monetary"),
        )
        .crossJoin(ref)
        .withColumn("recency_days", F.datediff("_ref", "last_order_date"))
        .drop("_ref")
    )
    scored = (
        base.withColumn("r_score", 6 - F.ntile(5).over(Window.orderBy("recency_days")))
        .withColumn("f_score", F.ntile(5).over(Window.orderBy("frequency")))
        .withColumn("m_score", F.ntile(5).over(Window.orderBy("monetary")))
    )
    segment = (
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4), "Champions")
        .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3), "Loyal")
        .when(F.col("r_score") >= 4, "New / Promising")
        .when(F.col("r_score") <= 2, "At Risk")
        .otherwise("Needs Attention")
    )
    return scored.withColumn("segment", segment)
