import csv

from generate_data import generate


def test_generator_is_deterministic_and_messy(tmp_path):
    counts = generate(tmp_path / "a", n_customers=300, n_orders=2000, seed=7)
    counts_again = generate(tmp_path / "b", n_customers=300, n_orders=2000, seed=7)
    assert counts == counts_again
    assert (tmp_path / "a/orders.csv").read_text() == (tmp_path / "b/orders.csv").read_text()

    customers = list(csv.DictReader(open(tmp_path / "a/customers.csv")))
    ids = [c["customer_id"] for c in customers]
    assert len(ids) > len(set(ids)), "expected duplicate customer updates"

    items = list(csv.DictReader(open(tmp_path / "a/order_items.csv")))
    assert any(int(i["quantity"]) <= 0 for i in items), "expected bad quantities"
    assert counts["products"] == 14
