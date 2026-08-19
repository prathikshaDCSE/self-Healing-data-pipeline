#!/usr/bin/env python3
"""
Synthetic dataset generator for the "Self-Healing Data Pipeline Agent" intern project.

Generates a clean, internally-consistent multi-source dataset simulating a small
e-commerce business:
  - customers.csv
  - products.csv
  - orders.csv          (daily batches, one file per day)
  - events.jsonl         (clickstream-style events, served via mock_api.py separately)

Usage:
    python generate_data.py --out ./data --days 14 --customers 500 --products 60 --orders-per-day 300

Re-run with a different --seed to get a different-but-equally-valid dataset per
intern cohort, so cohorts can't simply diff each other's raw files.
"""
import argparse
import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta

REGIONS = ["NA", "EU", "APAC", "LATAM", "MEA"]
CATEGORIES = ["electronics", "home", "apparel", "grocery", "sports", "toys", "beauty"]
EVENT_TYPES = ["page_view", "add_to_cart", "checkout_start", "checkout_complete", "search"]
ORDER_STATUSES = ["placed", "shipped", "delivered", "cancelled", "refunded"]


def gen_customers(n, rng):
    rows = []
    for i in range(1, n + 1):
        signup = datetime(2024, 1, 1) + timedelta(days=rng.randint(0, 600))
        rows.append({
            "customer_id": f"CUST{i:06d}",
            "name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "region": rng.choice(REGIONS),
            "signup_date": signup.strftime("%Y-%m-%d"),
        })
    return rows


def gen_products(n, rng):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "product_id": f"PROD{i:05d}",
            "name": f"Product {i}",
            "category": rng.choice(CATEGORIES),
            "price": round(rng.uniform(5, 500), 2),
        })
    return rows


def gen_orders_for_day(day, order_id_start, customers, products, n, rng):
    rows = []
    oid = order_id_start
    for _ in range(n):
        cust = rng.choice(customers)
        prod = rng.choice(products)
        qty = rng.randint(1, 5)
        total = round(prod["price"] * qty, 2)
        ts = day + timedelta(seconds=rng.randint(0, 86399))
        rows.append({
            "order_id": f"ORD{oid:08d}",
            "customer_id": cust["customer_id"],
            "product_id": prod["product_id"],
            "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "quantity": qty,
            "order_total": total,
            "status": rng.choices(ORDER_STATUSES, weights=[50, 25, 15, 7, 3])[0],
        })
        oid += 1
    return rows, oid


def gen_events_for_day(day, customers, n, rng):
    rows = []
    for _ in range(n):
        cust = rng.choice(customers)
        ts = day + timedelta(seconds=rng.randint(0, 86399))
        rows.append({
            "event_id": str(uuid.uuid4()),
            "customer_id": cust["customer_id"],
            "event_type": rng.choices(
                EVENT_TYPES, weights=[50, 20, 15, 10, 5]
            )[0],
            "session_id": str(uuid.uuid4())[:8],
            "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return rows


def write_csv(path, rows, fieldnames):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./data")
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--customers", type=int, default=500)
    ap.add_argument("--products", type=int, default=60)
    ap.add_argument("--orders-per-day", type=int, default=300)
    ap.add_argument("--events-per-day", type=int, default=1200)
    ap.add_argument("--start-date", default="2026-06-01")
    ap.add_argument("--seed", type=int, default=42, help="Change per intern cohort")
    args = ap.parse_args()

    rng = random.Random(args.seed)

    customers = gen_customers(args.customers, rng)
    products = gen_products(args.products, rng)

    write_csv(os.path.join(args.out, "customers.csv"), customers,
              ["customer_id", "name", "email", "region", "signup_date"])
    write_csv(os.path.join(args.out, "products.csv"), products,
              ["product_id", "name", "category", "price"])

    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    order_id = 1
    for d in range(args.days):
        day = start + timedelta(days=d)
        day_str = day.strftime("%Y-%m-%d")

        orders, order_id = gen_orders_for_day(
            day, order_id, customers, products, args.orders_per_day, rng
        )
        write_csv(
            os.path.join(args.out, "orders", f"orders_{day_str}.csv"),
            orders,
            ["order_id", "customer_id", "product_id", "order_ts", "quantity", "order_total", "status"],
        )

        events = gen_events_for_day(day, customers, args.events_per_day, rng)
        write_jsonl(os.path.join(args.out, "events", f"events_{day_str}.jsonl"), events)

        print(f"[{day_str}] wrote {len(orders)} orders, {len(events)} events")

    print(f"\nDone. Dataset written to {args.out}/")
    print("Dimension tables: customers.csv, products.csv")
    print(f"Fact batches: orders/orders_<date>.csv (one file per day, {args.days} days)")
    print(f"Event stream: events/events_<date>.jsonl (one file per day, {args.days} days)")


if __name__ == "__main__":
    main()
