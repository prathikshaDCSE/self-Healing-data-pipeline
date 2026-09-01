import json

from ai_agent import analyze_failure


# ============================================================
# TEST CASES
# ============================================================

TEST_CASES = [

    {
        "name": "Missing File",

        "task_id": "ingest_orders",

        "error": """
FileNotFoundError:
Orders file missing:
/opt/airflow/data/orders/orders_2026-08-29.csv
""",

        "expected_failure_type": "missing_file",

        "expected_action": "check_file",
    },

    {
        "name": "Schema Drift",

        "task_id": "validate_schema",

        "error": """
ValueError:
orders_2026-06-15.csv schema mismatch.
Expected customer_id but found customer_identifier
""",

        "expected_failure_type": "schema_drift",

        "expected_action": "fix_schema",
    },

    {
        "name": "Duplicate Data",

        "task_id": "validate_quality",

        "error": """
ValueError:
Duplicate order_ids detected for 2026-06-15
""",

        "expected_failure_type": "duplicate_data",

        "expected_action": "remove_duplicates",
    },

    {
        "name": "Code Error",

        "task_id": "ingest_orders",

        "error": """
Traceback (most recent call last):

File "/opt/airflow/dags/pipeline_dag_starter.py",
line 53, in ingest_orders

raise NotImplementedError

NotImplementedError
""",

        "expected_failure_type": "unknown",

        "expected_action": "manual_review",
    },
]


# ============================================================
# RUN TESTS
# ============================================================

def run_tests():

    print("=" * 70)
    print("AI AGENT CLASSIFICATION TEST SUITE")
    print("=" * 70)

    passed = 0
    failed = 0

    for index, test in enumerate(TEST_CASES, start=1):

        print("\n")
        print("-" * 70)
        print(
            f"TEST {index}: {test['name']}"
        )
        print("-" * 70)

        print(
            f"Expected failure type: "
            f"{test['expected_failure_type']}"
        )

        print(
            f"Expected action: "
            f"{test['expected_action']}"
        )

        try:

            result = analyze_failure(
                task_id=test["task_id"],
                error_message=test["error"],
                execution_date="2026-08-29T00:00:00+00:00",
            )

            print("\nAI RESULT:")

            print(
                json.dumps(
                    result,
                    indent=4
                )
            )

            actual_type = result.get(
                "failure_type"
            )

            actual_action = result.get(
                "recommended_action"
            )

            remediation_action = result.get(
                "remediation_action"
            )

            type_correct = (
                actual_type
                == test["expected_failure_type"]
            )

            action_correct = (
                actual_action
                == test["expected_action"]
            )

            remediation_correct = (
                remediation_action
                == actual_action
            )

            if (
                type_correct
                and action_correct
                and remediation_correct
            ):

                print("\nRESULT: PASS")

                passed += 1

            else:

                print("\nRESULT: FAIL")

                if not type_correct:

                    print(
                        "  Failure type mismatch:"
                    )

                if not action_correct:

                    print(
                        "  Recommended action mismatch:"
                    )

                if not remediation_correct:

                    print(
                        "  remediation_action does "
                        "not match recommended_action."
                    )

                failed += 1

        except Exception as e:

            print(
                f"\nTEST ERROR: {e}"
            )

            failed += 1

    # ========================================================
    # SUMMARY
    # ========================================================

    print("\n")
    print("=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    print(
        f"Total tests : {len(TEST_CASES)}"
    )

    print(
        f"Passed      : {passed}"
    )

    print(
        f"Failed      : {failed}"
    )

    print("=" * 70)

    if failed == 0:

        print(
            "\nALL CLASSIFICATION TESTS PASSED."
        )

    else:

        print(
            "\nSOME CLASSIFICATION TESTS FAILED."
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    run_tests()
