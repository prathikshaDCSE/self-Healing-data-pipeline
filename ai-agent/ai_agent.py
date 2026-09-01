import requests
import json


# ============================================================
# OLLAMA CONFIGURATION
# ============================================================

OLLAMA_URL = "http://localhost:11434/api/generate"

MODEL = "llama3.2:3b"


# ============================================================
# ALLOWED VALUES
# ============================================================

ALLOWED_FAILURE_TYPES = {
    "missing_file",
    "invalid_row_count",
    "schema_drift",
    "null_value",
    "referential_integrity",
    "duplicate_data",
    "volume_anomaly",
    "unknown",
}

ALLOWED_ACTIONS = {
    "check_file",
    "retry_task",
    "regenerate_data",
    "fix_schema",
    "clean_invalid_data",
    "investigate_foreign_key",
    "remove_duplicates",
    "investigate_volume",
    "manual_review",
    "no_action",
}


# ============================================================
# SAFE FALLBACK
# ============================================================

def safe_fallback(reason):
    """
    Fail closed.

    If anything goes wrong with the AI response,
    never allow automatic remediation.
    """

    return {
        "failure_type": "unknown",
        "root_cause": "Unable to safely determine root cause",
        "recommended_action": "manual_review",
        "auto_remediation_safe": False,
        "remediation_action": "manual_review",
        "reason": reason,
        "requires_human_approval": True,
    }


# ============================================================
# VALIDATE AI RESPONSE
# ============================================================

def validate_diagnosis(data):
    """
    Validate and normalize the AI response.

    The AI is allowed to recommend an action,
    but unsafe or malformed responses are rejected.
    """

    if not isinstance(data, dict):
        return safe_fallback(
            "AI response was not a JSON object."
        )

    required_fields = [
        "failure_type",
        "root_cause",
        "recommended_action",
        "auto_remediation_safe",
        "remediation_action",
        "reason",
        "requires_human_approval",
    ]

    for field in required_fields:
        if field not in data:
            return safe_fallback(
                f"AI response missing required field: {field}"
            )

    failure_type = data.get("failure_type")
    recommended_action = data.get("recommended_action")
    remediation_action = data.get("remediation_action")

    # --------------------------------------------------------
    # Validate failure type
    # --------------------------------------------------------

    if failure_type not in ALLOWED_FAILURE_TYPES:
        return safe_fallback(
            f"Invalid failure_type returned by AI: {failure_type}"
        )

    # --------------------------------------------------------
    # Validate recommended action
    # --------------------------------------------------------

    if recommended_action not in ALLOWED_ACTIONS:
        return safe_fallback(
            f"Invalid recommended_action returned by AI: "
            f"{recommended_action}"
        )

    # --------------------------------------------------------
    # remediation_action MUST match recommendation
    # --------------------------------------------------------

    if remediation_action != recommended_action:
        return safe_fallback(
            "remediation_action does not match "
            "recommended_action."
        )

    # --------------------------------------------------------
    # Validate boolean fields
    # --------------------------------------------------------

    if not isinstance(
        data.get("auto_remediation_safe"),
        bool
    ):
        return safe_fallback(
            "auto_remediation_safe must be boolean."
        )

    if not isinstance(
        data.get("requires_human_approval"),
        bool
    ):
        return safe_fallback(
            "requires_human_approval must be boolean."
        )

    # --------------------------------------------------------
    # SAFETY RULES
    # --------------------------------------------------------

    unsafe_actions = {
        "regenerate_data",
        "fix_schema",
        "clean_invalid_data",
        "investigate_foreign_key",
        "remove_duplicates",
        "investigate_volume",
        "manual_review",
    }

    if recommended_action in unsafe_actions:

        data["auto_remediation_safe"] = False
        data["requires_human_approval"] = True

    # Unknown failures are always unsafe unless they recommend a retry.

    if failure_type == "unknown" and recommended_action != "retry_task":

        data["auto_remediation_safe"] = False
        data["requires_human_approval"] = True
        data["recommended_action"] = "manual_review"
        data["remediation_action"] = "manual_review"


    # Schema drift is always manual review.

    if failure_type == "schema_drift":

        data["auto_remediation_safe"] = False
        data["requires_human_approval"] = True

    # Duplicate data is always manual review.

    if failure_type == "duplicate_data":

        data["auto_remediation_safe"] = False
        data["requires_human_approval"] = True

    # Referential integrity is always manual review.

    if failure_type == "referential_integrity":

        data["auto_remediation_safe"] = False
        data["requires_human_approval"] = True

    return data


# ============================================================
# AI FAILURE ANALYSIS
# ============================================================

def analyze_failure(
    task_id,
    error_message,
    execution_date
):
    """
    Send Airflow failure information to Ollama
    and receive a structured diagnosis.
    """

    prompt = f"""
You are an AI Data Pipeline Monitoring Agent
for an Airflow self-healing data pipeline.

Your job is ONLY to analyze the supplied Airflow
failure message.

Do not invent information.

Do not execute commands.

Do not modify files.

Do not modify databases.

Do not change schemas.

Do not delete data.

Do not assume information that is not present
in the error message.

==================================================
PIPELINE INFORMATION
==================================================

Pipeline:
self_healing_pipeline

Task:
{task_id}

Execution date:
{execution_date}

Airflow error:
{error_message}

==================================================
ALLOWED FAILURE TYPES
==================================================

You MUST choose exactly ONE:

missing_file
invalid_row_count
schema_drift
null_value
referential_integrity
duplicate_data
volume_anomaly
unknown

==================================================
ALLOWED ACTIONS
==================================================

You MUST choose exactly ONE:

check_file
retry_task
regenerate_data
fix_schema
clean_invalid_data
investigate_foreign_key
remove_duplicates
investigate_volume
manual_review
no_action

==================================================
CLASSIFICATION RULES
==================================================

1. FileNotFoundError or an explicitly missing
   input file:
   failure_type = missing_file
   recommended_action = check_file

2. A clear row-count mismatch:
   failure_type = invalid_row_count

3. A clear schema mismatch, missing column,
   unexpected column, or incompatible schema:
   failure_type = schema_drift
   recommended_action = fix_schema

4. A clear null/None value violation:
   failure_type = null_value
   recommended_action = clean_invalid_data

5. A clear foreign-key or referential integrity
   violation:
   failure_type = referential_integrity
   recommended_action = investigate_foreign_key

6. A clear duplicate record/order ID violation:
   failure_type = duplicate_data
   recommended_action = remove_duplicates

7. A clear abnormal volume issue:
   failure_type = volume_anomaly
   recommended_action = investigate_volume

8. If the error clearly mentions "Simulated transient failure" or "transient failure", it is a transient error:
   failure_type = unknown
   recommended_action = retry_task

9. If the error does not clearly match one of
   the categories above:
   failure_type = unknown
   recommended_action = manual_review

==================================================
REMEDIATION ACTION RULE
==================================================

IMPORTANT:

"remediation_action" MUST ALWAYS contain
the EXACT SAME VALUE as "recommended_action".

For example:

recommended_action:
check_file

remediation_action:
check_file

OR:

recommended_action:
fix_schema

remediation_action:
fix_schema

Never leave remediation_action empty.

==================================================
SAFETY RULES
==================================================

The AI only recommends actions.

Automatic remediation is NOT allowed for:

- schema changes
- deleting data
- cleaning data
- duplicate removal
- referential integrity fixes
- unknown failures (unless clearly a transient failure)
- complex logic
- data regeneration

These must have:

"auto_remediation_safe": false

and

"requires_human_approval": true

A simple retry may be considered safe ONLY when
the supplied error clearly indicates a transient
failure (such as "Simulated transient failure").
In this specific transient failure case:

"auto_remediation_safe": true

and

"requires_human_approval": false

If there is not enough information to prove that
a retry is safe:

"auto_remediation_safe": false

and

"requires_human_approval": true

Missing files should be investigated and should
NOT automatically be recreated.


==================================================
OUTPUT
==================================================

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

    print("\n[AI AGENT] Sending failure to Ollama...")

    try:

        response = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False,
                "format": "json",
            },
            timeout=300,
        )

        response.raise_for_status()

        result = response.json()

        ai_response = result.get("response", "")

        print("[AI AGENT] Ollama response received.")

        if not ai_response:
            return safe_fallback(
                "Ollama returned an empty response."
            )

        # ----------------------------------------------------
        # Parse JSON
        # ----------------------------------------------------

        try:

            parsed_response = json.loads(
                ai_response
            )

        except json.JSONDecodeError:

            return safe_fallback(
                "Ollama returned invalid JSON."
            )

        # ----------------------------------------------------
        # Validate
        # ----------------------------------------------------

        validated_response = validate_diagnosis(
            parsed_response
        )

        print(
            "[AI AGENT] Response passed safety validation."
        )

        return validated_response

    except requests.exceptions.Timeout:

        print(
            "[AI AGENT] Ollama request timed out."
        )

        return safe_fallback(
            "Ollama request timed out."
        )

    except requests.exceptions.ConnectionError:

        print(
            "[AI AGENT] Could not connect to Ollama."
        )

        return safe_fallback(
            "Could not connect to Ollama."
        )

    except requests.exceptions.HTTPError as e:

        print(
            "[AI AGENT] Ollama HTTP error."
        )

        return safe_fallback(
            f"Ollama HTTP error: {e}"
        )

    except Exception as e:

        print(
            "[AI AGENT] Unexpected AI error."
        )

        return safe_fallback(
            f"Unexpected AI error: {e}"
        )


# ============================================================
# MAIN TEST
# ============================================================

if __name__ == "__main__":

    TASK_ID = "ingest_orders"

    EXECUTION_DATE = (
        "2026-08-20T02:00:00+00:00"
    )

    ERROR_MESSAGE = """
Traceback (most recent call last):

File "/opt/airflow/dags/pipeline_dag_starter.py",
line 53, in ingest_orders

raise NotImplementedError

NotImplementedError
"""

    print("=" * 70)
    print("SELF-HEALING DATA PIPELINE AI AGENT")
    print("=" * 70)

    print("\nTask:")
    print(TASK_ID)

    print("\nExecution date:")
    print(EXECUTION_DATE)

    print("\nFailure:")
    print(ERROR_MESSAGE)

    result = analyze_failure(
        task_id=TASK_ID,
        error_message=ERROR_MESSAGE,
        execution_date=EXECUTION_DATE,
    )

    print("\n")
    print("=" * 70)
    print("AI AGENT DIAGNOSIS")
    print("=" * 70)

    print(
        json.dumps(
            result,
            indent=4
        )
    )

    print("=" * 70)