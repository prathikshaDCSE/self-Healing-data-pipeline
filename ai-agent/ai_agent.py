import requests
import json
import re


# ============================================================
# OLLAMA CONFIGURATION
# ============================================================

OLLAMA_URL = "http://localhost:11434/api/generate"

MODEL = "llama3.2:3b"

# Local LLM can be slow on a laptop.
OLLAMA_TIMEOUT = 300


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
    Fail safely if the AI response cannot be trusted.
    """

    return {
        "failure_type": "unknown",
        "root_cause": "Unable to safely determine the root cause",
        "recommended_action": "manual_review",
        "auto_remediation_safe": False,
        "remediation_action": "manual_review",
        "reason": reason,
        "requires_human_approval": True
    }


# ============================================================
# CLEAN AI RESPONSE
# ============================================================

def clean_json_response(ai_response):
    """
    Remove common markdown formatting accidentally returned
    by the LLM.
    """

    ai_response = ai_response.strip()

    # Remove markdown code fences
    ai_response = re.sub(
        r"^```json\s*",
        "",
        ai_response,
        flags=re.IGNORECASE
    )

    ai_response = re.sub(
        r"^```\s*",
        "",
        ai_response
    )

    ai_response = re.sub(
        r"\s*```$",
        "",
        ai_response
    )

    return ai_response.strip()


# ============================================================
# VALIDATE AI RESPONSE
# ============================================================

def validate_ai_response(response):
    """
    Strictly validate the AI response before allowing it
    to reach the remediation controller.
    """

    required_fields = {
        "failure_type",
        "root_cause",
        "recommended_action",
        "auto_remediation_safe",
        "remediation_action",
        "reason",
        "requires_human_approval",
    }

    # --------------------------------------------------------
    # Check fields
    # --------------------------------------------------------

    if not isinstance(response, dict):
        return False, "AI response is not a JSON object."

    missing_fields = required_fields - set(response.keys())

    if missing_fields:
        return False, (
            "AI response is missing fields: "
            + ", ".join(sorted(missing_fields))
        )

    # --------------------------------------------------------
    # Validate failure type
    # --------------------------------------------------------

    if response["failure_type"] not in ALLOWED_FAILURE_TYPES:

        return False, (
            f"Invalid failure_type: "
            f"{response['failure_type']}"
        )

    # --------------------------------------------------------
    # Validate recommended action
    # --------------------------------------------------------

    if response["recommended_action"] not in ALLOWED_ACTIONS:

        return False, (
            f"Invalid recommended_action: "
            f"{response['recommended_action']}"
        )

    # --------------------------------------------------------
    # Validate remediation action
    # --------------------------------------------------------

    if response["remediation_action"] not in ALLOWED_ACTIONS:

        return False, (
            f"Invalid remediation_action: "
            f"{response['remediation_action']}"
        )

    # --------------------------------------------------------
    # Validate booleans
    # --------------------------------------------------------

    if not isinstance(
        response["auto_remediation_safe"],
        bool
    ):

        return False, (
            "auto_remediation_safe must be boolean."
        )

    if not isinstance(
        response["requires_human_approval"],
        bool
    ):

        return False, (
            "requires_human_approval must be boolean."
        )

    # --------------------------------------------------------
    # Safety enforcement
    # --------------------------------------------------------

    dangerous_failure_types = {
        "schema_drift",
        "referential_integrity",
        "duplicate_data",
    }

    if response["failure_type"] in dangerous_failure_types:

        response["auto_remediation_safe"] = False

        response["requires_human_approval"] = True

        response["recommended_action"] = "manual_review"

        response["remediation_action"] = "manual_review"

    # --------------------------------------------------------
    # Manual review must always be unsafe
    # --------------------------------------------------------

    if response["recommended_action"] == "manual_review":

        response["auto_remediation_safe"] = False

        response["requires_human_approval"] = True

        response["remediation_action"] = "manual_review"

    # --------------------------------------------------------
    # If AI says unsafe, force human approval
    # --------------------------------------------------------

    if response["auto_remediation_safe"] is False:

        response["requires_human_approval"] = True

    return True, response


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
for an Airflow self-healing pipeline.

Your job is ONLY to analyze the provided
Airflow failure and produce a safe remediation
recommendation.

You MUST NOT execute anything.

You MUST NOT invent information.

You MUST use ONLY the supplied Airflow error.

Pipeline:
self_healing_pipeline

Task:
{task_id}

Execution date:
{execution_date}

Airflow error:
{error_message}


============================================================
ALLOWED FAILURE TYPES
============================================================

You MUST choose exactly one:

missing_file
invalid_row_count
schema_drift
null_value
referential_integrity
duplicate_data
volume_anomaly
unknown


============================================================
ALLOWED REMEDIATION ACTIONS
============================================================

You MUST choose exactly one:

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


============================================================
IMPORTANT CLASSIFICATION RULES
============================================================

If the error says a file does not exist,
the failure_type should be:

missing_file

If the error indicates that the expected number
of rows is wrong, use:

invalid_row_count

If the error indicates a changed or incompatible
table structure, use:

schema_drift

If the error indicates NULL or missing values
inside records, use:

null_value

If the error indicates a foreign key or relationship
problem, use:

referential_integrity

If the error indicates duplicate records, use:

duplicate_data

If the error indicates an unusual increase or decrease
in data volume, use:

volume_anomaly

If the error is something else that cannot safely
be classified using the above categories, use:

unknown


============================================================
SPECIAL RULE FOR CODE ERRORS
============================================================

Errors such as:

NotImplementedError
AttributeError
NameError
TypeError
SyntaxError
ImportError
ModuleNotFoundError

are NOT data-quality failures.

If the supplied error is a code or implementation
error and does not match one of the allowed data
failure types:

failure_type = "unknown"

recommended_action = "manual_review"

remediation_action = "manual_review"

auto_remediation_safe = false

requires_human_approval = true


============================================================
SAFETY RULES
============================================================

1. Do not invent information.

2. Use ONLY the supplied Airflow error.

3. Do not execute commands.

4. Do not modify files.

5. Do not delete data.

6. Do not modify BigQuery data.

7. Do not change database schemas automatically.

8. Do not claim that a file exists unless the error
   explicitly proves it.

9. Missing files should normally require investigation.

10. Schema drift must require manual review.

11. Referential integrity failures must require
    manual review.

12. Duplicate data failures must require manual review.

13. Code/implementation errors must require manual review.

14. If there is uncertainty, choose unknown
    and manual_review.

15. retry_task is allowed only when the error clearly
    suggests a transient failure.

16. A retry does NOT fix the underlying problem.

17. The AI only recommends an action.

18. A separate Python remediation controller decides
    whether an action can actually execute.

19. Never mark a destructive or data-changing action
    as automatically safe.

20. If human approval is required, auto_remediation_safe
    MUST be false.


============================================================
OUTPUT
============================================================

Return ONLY valid JSON.

Do NOT use markdown.

Do NOT add explanations outside JSON.

Use EXACTLY this structure:

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
                "format": "json"
            },
            timeout=OLLAMA_TIMEOUT
        )

        response.raise_for_status()

    except requests.exceptions.ConnectionError:

        print(
            "[AI AGENT] ERROR: "
            "Could not connect to Ollama."
        )

        return safe_fallback(
            "Ollama is not reachable."
        )

    except requests.exceptions.Timeout:

        print(
            "[AI AGENT] ERROR: "
            "Ollama request timed out."
        )

        return safe_fallback(
            "Ollama request timed out."
        )

    except requests.exceptions.HTTPError as e:

        print(
            "[AI AGENT] ERROR: "
            "Ollama HTTP error."
        )

        return safe_fallback(
            f"Ollama HTTP error: {e}"
        )

    except requests.exceptions.RequestException as e:

        print(
            "[AI AGENT] ERROR: "
            "Ollama request failed."
        )

        return safe_fallback(
            f"Ollama request failed: {e}"
        )

    print("[AI AGENT] Ollama response received.")

    # --------------------------------------------------------
    # Read response
    # --------------------------------------------------------

    try:

        result = response.json()

    except json.JSONDecodeError:

        print(
            "[AI AGENT] ERROR: "
            "Ollama returned invalid API response."
        )

        return safe_fallback(
            "Ollama API response was not valid JSON."
        )

    ai_response = result.get(
        "response",
        ""
    )

    if not ai_response:

        print(
            "[AI AGENT] ERROR: "
            "Ollama returned an empty response."
        )

        return safe_fallback(
            "Ollama returned an empty response."
        )

    # --------------------------------------------------------
    # Clean response
    # --------------------------------------------------------

    ai_response = clean_json_response(
        ai_response
    )

    # --------------------------------------------------------
    # Parse JSON
    # --------------------------------------------------------

    try:

        parsed_response = json.loads(
            ai_response
        )

    except json.JSONDecodeError:

        print(
            "[AI AGENT] ERROR: "
            "Ollama returned invalid JSON."
        )

        print(
            "[AI AGENT] Raw response:"
        )

        print(ai_response)

        return safe_fallback(
            "The AI response could not be parsed safely."
        )

    # --------------------------------------------------------
    # Validate response
    # --------------------------------------------------------

    valid, validation_result = validate_ai_response(
        parsed_response
    )

    if not valid:

        print(
            "[AI AGENT] ERROR: "
            "AI response failed safety validation."
        )

        print(
            f"[AI AGENT] Reason: "
            f"{validation_result}"
        )

        return safe_fallback(
            f"AI response failed safety validation: "
            f"{validation_result}"
        )

    print(
        "[AI AGENT] Response passed safety validation."
    )

    return validation_result


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

    print(
        "SELF-HEALING DATA PIPELINE AI AGENT"
    )

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
        execution_date=EXECUTION_DATE
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