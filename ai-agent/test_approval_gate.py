import sys
from remediation import execute_remediation

print("=" * 70)
print("INTERACTIVE SAFETY CONTROLLER & HUMAN APPROVAL GATE TEST")
print("=" * 70)

# Scenario 1: Schema Drift (fix_schema - Unsafe, requires human approval)
print("\n[SCENARIO 1] Schema Drift detected. Recommending 'fix_schema'...")
result1 = execute_remediation(
    dag_id="self_healing_pipeline",
    run_id="manual__test_run",
    task_id="validate_schema",
    failure_type="schema_drift",
    recommended_action="fix_schema",
    safe=False,
    logs="ValueError: schema mismatch",
    interactive=True
)
print("\nResult:")
print(result1)

# Scenario 2: Duplicate Data (remove_duplicates - Unsafe, requires human approval)
print("\n[SCENARIO 2] Duplicate Data detected. Recommending 'remove_duplicates'...")
result2 = execute_remediation(
    dag_id="self_healing_pipeline",
    run_id="manual__test_run",
    task_id="load_orders_to_bq",
    failure_type="duplicate_data",
    recommended_action="remove_duplicates",
    safe=False,
    logs="ValueError: Duplicate order_ids detected",
    interactive=True
)
print("\nResult:")
print(result2)

# Scenario 3: NotImplementedError (manual_review - Unsafe, always blocked without approval prompt)
print("\n[SCENARIO 3] NotImplementedError detected. Recommending 'manual_review'...")
result3 = execute_remediation(
    dag_id="self_healing_pipeline",
    run_id="manual__test_run",
    task_id="ingest_orders",
    failure_type="unknown",
    recommended_action="manual_review",
    safe=False,
    logs="NotImplementedError",
    interactive=True
)
print("\nResult:")
print(result3)
