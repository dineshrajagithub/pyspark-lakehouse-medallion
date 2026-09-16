from datetime import date

import pytest


@pytest.fixture
def silver(spark):
    orders = spark.createDataFrame(
        [("O1", "C1", date(2025, 1, 1), "delivered"),
         ("O2", "C2", date(2025, 1, 1), "cancelled"),
         ("O3", "C1", date(2025, 1, 2), "shipped"),
         ("O4", "C3", date(2025, 1, 10), "delivered")],
        "order_id string, customer_id string, order_date date, status string",
    )
    items = spark.sql("""
        SELECT * FROM VALUES
          ('O1', 1, 'P1', 2, CAST(10.00 AS DECIMAL(10,2)), CAST(10 AS DECIMAL(5,2))),
          ('O2', 1, 'P1', 1, CAST(10.00 AS DECIMAL(10,2)), CAST(0 AS DECIMAL(5,2))),
          ('O3', 1, 'P2', 1, CAST(50.00 AS DECIMAL(10,2)), CAST(0 AS DECIMAL(5,2))),
          ('O4', 1, 'P9', 3, CAST(5.00 AS DECIMAL(10,2)), CAST(0 AS DECIMAL(5,2)))
        AS t(order_id, line_number, product_id, quantity, unit_price, discount_pct)
    """)
    products = spark.createDataFrame(
        [("P1", "Books"), ("P2", "Electronics")], "product_id string, category string")
    return orders, items, products


def test_fact_sales_amounts(silver):
    from lakehouse.gold import fact_sales

    rows = {r.order_id: r for r in fact_sales(*silver).collect()}
    assert float(rows["O1"].gross_amount) == 20.0
    assert float(rows["O1"].net_amount) == 18.0
    assert rows["O2"].is_revenue is False
    assert rows["O4"].category == "Unknown"


def test_daily_sales_excludes_cancelled(silver):
    from lakehouse.gold import daily_sales, fact_sales

    daily = {r.order_date: r for r in daily_sales(fact_sales(*silver)).collect()}
    assert float(daily[date(2025, 1, 1)].revenue) == 18.0
    assert daily[date(2025, 1, 1)].orders == 1
    assert float(daily[date(2025, 1, 2)].revenue_7d_avg) == pytest.approx((18 + 50) / 2)


def test_category_share_sums_to_100(silver):
    from lakehouse.gold import category_performance, fact_sales

    rows = category_performance(fact_sales(*silver)).collect()
    assert sum(float(r.revenue_share_pct) for r in rows) == pytest.approx(100, abs=0.05)


def test_rfm_segments(silver):
    from lakehouse.gold import customer_rfm, fact_sales

    rfm = {r.customer_id: r for r in customer_rfm(fact_sales(*silver), as_of="2025-01-11").collect()}
    assert set(rfm) == {"C1", "C3"}  # C2 only has a cancelled order
    assert rfm["C3"].recency_days == 1
    assert rfm["C1"].frequency == 2
    assert 1 <= rfm["C1"].r_score <= 5
