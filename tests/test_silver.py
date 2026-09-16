from datetime import date


def _rows(df):
    return [r.asDict() for r in df.collect()]


def test_customers_dedup_latest_and_quarantine(spark):
    from lakehouse.silver import clean_customers

    raw = spark.createDataFrame(
        [
            ("C1", " asha rao ", "ASHA@X.COM", "chennai", "tn", "2025-01-01", "2025-01-01 10:00:00"),
            ("C1", "asha rao", "asha@x.com", "pune", "mh", "2025-01-01", "2025-06-01 10:00:00"),
            ("C2", "bad email", "nope at x.com", "pune", "mh", "2025-01-01", "2025-01-01 10:00:00"),
            (None, "no id", "n@x.com", "pune", "mh", "2025-01-01", "2025-01-01 10:00:00"),
        ],
        "customer_id string, full_name string, email string, city string, state string,"
        " signup_date string, updated_at string",
    )
    valid, bad = clean_customers(raw)
    rows = _rows(valid)
    assert len(rows) == 1
    assert rows[0]["city"] == "Pune" and rows[0]["email"] == "asha@x.com"
    assert rows[0]["full_name"] == "Asha Rao"
    reasons = sorted(r["_dq_errors"][0] for r in _rows(bad))
    assert reasons == ["invalid_email", "missing_customer_id"]


def test_orders_reject_unknown_status_and_bad_ts(spark):
    from lakehouse.silver import clean_orders

    raw = spark.createDataFrame(
        [("O1", "C1", "2025-03-01 09:00:00", "Delivered"),
         ("O1", "C1", "2025-03-01 09:00:00", "delivered"),
         ("O2", "C1", "not-a-date", "placed"),
         ("O3", "C1", "2025-03-02 09:00:00", "lost_in_space")],
        "order_id string, customer_id string, order_ts string, status string",
    )
    valid, bad = clean_orders(raw)
    rows = _rows(valid)
    assert [r["order_id"] for r in rows] == ["O1"]
    assert rows[0]["order_date"] == date(2025, 3, 1)
    assert bad.count() == 2


def test_order_items_rules(spark):
    from lakehouse.silver import clean_order_items

    raw = spark.createDataFrame(
        [("O1", "1", "P1", "2", "10.00", ""),
         ("O1", "2", "P2", "-1", "10.00", "5"),
         ("O1", "3", "P2", "1", "10.00", "150")],
        "order_id string, line_number string, product_id string, quantity string,"
        " unit_price string, discount_pct string",
    )
    valid, bad = clean_order_items(raw)
    rows = _rows(valid)
    assert len(rows) == 1 and float(rows[0]["discount_pct"]) == 0.0
    assert sorted(r["_dq_errors"][0] for r in _rows(bad)) == [
        "discount_out_of_range", "non_positive_quantity"]
