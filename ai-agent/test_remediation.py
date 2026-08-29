from remediation import execute_remediation


print("=" * 70)
print("TEST 1 - UNSAFE REMEDIATION")
print("=" * 70)

result = execute_remediation(
    task_id="ingest_orders",
    failure_type="missing_file",
    recommended_action="check_file",
    safe=False
)

print("\nResult:")
print(result)


print("\n" + "=" * 70)
print("TEST 2 - SAFE RETRY")
print("=" * 70)

result = execute_remediation(
    task_id="ingest_orders",
    failure_type="invalid_row_count",
    recommended_action="retry_task",
    safe=True
)

print("\nResult:")
print(result)


print("\n" + "=" * 70)
print("TEST 3 - UNAPPROVED ACTION")
print("=" * 70)

result = execute_remediation(
    task_id="validate_schema",
    failure_type="schema_drift",
    recommended_action="fix_schema",
    safe=True
)

print("\nResult:")
print(result)