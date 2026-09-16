def test_end_to_end(spark, tmp_path):
    from generate_data import generate
    from lakehouse.pipeline import run

    generate(tmp_path / "raw", n_customers=200, n_orders=1500, seed=1)
    m = run(str(tmp_path / "raw"), str(tmp_path / "lake"), batch_id="b001")
    assert m["bronze"]["orders"] > m["silver"]["orders"]  # duplicates + bad rows removed
    assert sum(m["quarantine"].values()) > 0
    assert m["gold"]["fact_sales"] > 0 and m["gold"]["customer_rfm"] > 0
