"""Generate a realistic, deliberately *messy* retail data set.

The noise (duplicates, bad emails, negative quantities, unknown statuses,
malformed timestamps, stray whitespace) exercises the silver-layer rules.

    python scripts/generate_data.py --out data/raw --customers 2000 --orders 20000
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

FIRST = ["aarav", "diya", "arjun", "meera", "rohan", "priya", "karthik", "ananya",
         "vikram", "sneha", "james", "olivia", "liam", "emma", "noah", "sofia"]
LAST = ["sharma", "iyer", "reddy", "nair", "patel", "kumar", "smith", "johnson",
        "brown", "garcia", "wilson", "martin"]
CITIES = [("Chennai", "TN"), ("Bengaluru", "KA"), ("Hyderabad", "TS"), ("Mumbai", "MH"),
          ("Pune", "MH"), ("Austin", "TX"), ("Seattle", "WA"), ("Boston", "MA")]
CATEGORIES = {
    "electronics": [("Wireless Earbuds", 59.99), ("Smart Watch", 199.0), ("USB-C Hub", 34.5),
                    ("Mechanical Keyboard", 89.0), ("4K Monitor", 329.0)],
    "home": [("Air Fryer", 119.0), ("Coffee Maker", 79.0), ("LED Desk Lamp", 29.99)],
    "fashion": [("Running Shoes", 95.0), ("Denim Jacket", 70.0), ("Backpack", 45.0)],
    "books": [("Designing Data-Intensive Apps", 42.0), ("SQL Cookbook", 35.0),
              ("Fundamentals of Data Engineering", 48.0)],
}
STATUSES = ["placed"] * 2 + ["shipped"] * 3 + ["delivered"] * 12 + ["cancelled", "returned"]


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def generate(out: Path, n_customers: int, n_orders: int, seed: int = 42,
             start: datetime = datetime(2025, 1, 1), days: int = 365) -> dict[str, int]:
    rng = random.Random(seed)
    out.mkdir(parents=True, exist_ok=True)

    # --- products
    products = []
    for cat, items in CATEGORIES.items():
        for name, price in items:
            products.append({"product_id": f"P{len(products) + 1:04d}", "product_name": name,
                             "category": f" {cat} ", "list_price": f"{price:.2f}"})

    # --- customers (with updates = duplicates, and some bad emails)
    customers = []
    for i in range(1, n_customers + 1):
        first, last = rng.choice(FIRST), rng.choice(LAST)
        city, state = rng.choice(CITIES)
        signup = start - timedelta(days=rng.randint(0, 700))
        email = f"{first}.{last}{i}@example.com"
        if rng.random() < 0.02:
            email = email.replace("@", " at ")  # invalid
        row = {"customer_id": f"C{i:06d}", "full_name": f"  {first} {last} ",
               "email": email.upper() if rng.random() < 0.1 else email,
               "city": city.lower(), "state": state.lower(),
               "signup_date": signup.date().isoformat(), "updated_at": _ts(signup)}
        customers.append(row)
        if rng.random() < 0.08:  # a later profile update -> newer duplicate
            moved_city, moved_state = rng.choice(CITIES)
            customers.append({**row, "city": moved_city, "state": moved_state,
                              "updated_at": _ts(signup + timedelta(days=rng.randint(1, 300)))})

    # --- orders + items
    orders, items = [], []
    for o in range(1, n_orders + 1):
        oid = f"O{o:07d}"
        ts = start + timedelta(days=rng.randint(0, days - 1), seconds=rng.randint(0, 86399))
        status = rng.choice(STATUSES)
        ts_str = _ts(ts)
        if rng.random() < 0.005:
            status = "lost_in_space"  # unknown status
        if rng.random() < 0.003:
            ts_str = "not-a-date"
        orders.append({"order_id": oid, "customer_id": f"C{rng.randint(1, n_customers):06d}",
                       "order_ts": ts_str, "status": status})
        for line in range(1, rng.choice([1, 1, 1, 2, 2, 3, 4]) + 1):
            p = rng.choice(products)
            qty = rng.choice([1, 1, 1, 2, 2, 3])
            if rng.random() < 0.004:
                qty = -qty  # bad record
            discount = rng.choice(["", "", "", "5", "10", "15"])
            items.append({"order_id": oid, "line_number": line, "product_id": p["product_id"],
                          "quantity": qty, "unit_price": p["list_price"], "discount_pct": discount})
        if rng.random() < 0.01:  # exact duplicate order row (at-least-once source)
            orders.append(dict(orders[-1]))

    tables = {"products": products, "customers": customers, "orders": orders,
              "order_items": items}
    for name, rows in tables.items():
        with open(out / f"{name}.csv", "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    return {k: len(v) for k, v in tables.items()}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--customers", type=int, default=2000)
    ap.add_argument("--orders", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    print(generate(Path(a.out), a.customers, a.orders, a.seed))
