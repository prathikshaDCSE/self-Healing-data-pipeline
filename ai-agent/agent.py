import requests
import json


OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3.2:3b"


def analyze_failure(task_id, error_message, execution_date):

    prompt = f"""
You are an AI Data Pipeline Monitoring Agent for an Airflow self-healing pipeline.

Your responsibility is to:
1. Analyze the Airflow failure.
2. Identify the failure type.
3. Determine the root cause using ONLY the provided error.
4. Recommend a safe remediation action.
5. Decide whether the remediation is safe for automatic execution.
6. Return a structured remediation plan.

Pipeline:
self_healing_pipeline

Task:
{task_id}

Execution date:
{execution_date}

Error:
{error_message}


Allowed failure types:

- missing_file
- invalid_row_count
- schema_drift
- null_value
- referential_integrity
- duplicate_data
- volume_anomaly
- unknown


Allowed remediation actions:

- check_file
- retry_task
- regenerate_data
- fix_schema
- clean_invalid_data
- investigate_foreign_key
- remove_duplicates
- investigate_volume
- manual_review
- no_action


IMPORTANT SAFETY RULES:

1. Do not invent information.
2. Base your diagnosis ONLY on the provided Airflow error.
3. Do not execute commands.
4. Do not modify files.
5. Do not delete data.
6. Do not change database schemas automatically.
7. Do not modify BigQuery data automatically.
8. If the failure could cause data loss or corruption, set
   "auto_remediation_safe" to false.
9. Missing files should normally NOT be automatically recreated unless
   the error clearly indicates that regeneration is safe.
10. Schema drift should normally require manual review.
11. Referential integrity failures should normally require manual review.
12. Duplicate data should normally require manual review.
13. Invalid row count may be retried only when the problem could be transient.
14. A retry is NOT the same as fixing the underlying problem.
15. The AI only recommends an action. A separate Python remediation layer
    will decide whether to actually execute it.


Return ONLY valid JSON.

Use exactly this structure:

{{
    "failure_type": "",
    "root_cause": "",
    "recommended_action": "",
    "auto_remediation_safe": false,
    "remediation_action": "",
    "reason": "",
    "requires_human_approval": true
}}
"""

    response = requests.post(
        OLLAMA_URL,
        json={
            "model": MODEL,
            "prompt": prompt,
            "stream": False,
            "format": "json"
        },
        timeout=120
    )

    response.raise_for_status()

    result = response.json()

    ai_response = result["response"]

    # Validate that Ollama returned valid JSON
    try:
        parsed_response = json.loads(ai_response)
    except json.JSONDecodeError:
        return {
            "failure_type": "unknown",
            "root_cause": "AI returned invalid JSON",
            "recommended_action": "manual_review",
            "auto_remediation_safe": False,
            "remediation_action": "manual_review",
            "reason": "The AI response could not be parsed safely.",
            "requires_human_approval": True
        }

    return parsed_response


if __name__ == "__main__":

    task_id = "ingest_orders"

    error_message = (
        "FileNotFoundError: Orders file not found: "
        "/opt/airflow/data/orders/orders_2026-06-15.csv"
    )

    execution_date = "2026-06-15"

    print("=" * 60)
    print("SELF-HEALING DATA PIPELINE AI AGENT")
    print("=" * 60)

    print("\nSending Airflow failure to AI...\n")

    result = analyze_failure(
        task_id,
        error_message,
        execution_date
    )

    print("AI AGENT RESPONSE")
    print("-" * 60)

    print(json.dumps(result, indent=4))

    print("-" * 60)