from agent import analyze_failure


test_cases = [

    {
        "name": "Missing File",
        "task": "ingest_orders",
        "error": (
            "FileNotFoundError: Orders file not found: "
            "/opt/airflow/data/orders/orders_2026-06-15.csv"
        )
    },

    {
        "name": "Invalid Row Count",
        "task": "ingest_orders",
        "error": (
            "ValueError: Orders row count 100 for 2026-06-15 "
            "is outside allowed range (240-360)"
        )
    },

    {
        "name": "Schema Drift",
        "task": "validate_schema",
        "error": (
            "ValueError: orders_2026-06-15.csv schema mismatch. "
            "Expected customer_id but found customer_identifier"
        )
    },

    {
        "name": "Null Value",
        "task": "validate_quality",
        "error": (
            "ValueError: NULL order_total exceeds 2% tolerance: 3.33%"
        )
    },

    {
        "name": "Referential Integrity",
        "task": "validate_quality",
        "error": (
            "ValueError: Referential integrity failure: "
            "1 orders have non-existent customer_ids"
        )
    },

    {
        "name": "Duplicate Data",
        "task": "load_orders_to_bq",
        "error": (
            "ValueError: Duplicate order_ids detected for "
            "2026-06-15. Expected 301 unique IDs, found 300."
        )
    },

    {
        "name": "Volume Anomaly",
        "task": "ingest_events",
        "error": (
            "ValueError: Events row count 2000 for 2026-06-15 "
            "is outside allowed range (900-1500)"
        )
    }
]


for test in test_cases:

    print("\n" + "=" * 70)
    print(test["name"])
    print("=" * 70)

    result = analyze_failure(
        test["task"],
        test["error"],
        "2026-06-15"
    )

    print(result)