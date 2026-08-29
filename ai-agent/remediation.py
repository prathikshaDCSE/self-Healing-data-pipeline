import os
import time


def check_file_exists(file_path):
    """
    Check whether the expected data file exists.
    """
    exists = os.path.exists(file_path)

    print(f"[REMEDIATION] Checking file: {file_path}")
    print(f"[REMEDIATION] File exists: {exists}")

    return exists


def retry_task(task_id):
    """
    Placeholder for Airflow task retry.

    We will connect this to Airflow after
    testing the remediation logic.
    """
    print(f"[REMEDIATION] Requesting retry for task: {task_id}")

    return {
        "action": "retry_task",
        "task_id": task_id,
        "status": "retry_requested"
    }


def execute_remediation(task_id, failure_type, recommended_action, safe):
    """
    Execute only predefined and approved remediation actions.
    """

    print("\n" + "=" * 70)
    print("REMEDIATION CONTROLLER")
    print("=" * 70)

    print(f"Task: {task_id}")
    print(f"Failure type: {failure_type}")
    print(f"AI recommendation: {recommended_action}")
    print(f"AI marked safe: {safe}")

    # Never execute remediation if AI says it is unsafe.
    if not safe:
        print("[REMEDIATION] Automatic remediation NOT allowed.")

        return {
            "status": "blocked",
            "reason": "AI marked remediation as unsafe"
        }

    # Only allow explicitly approved actions.
    allowed_actions = {
        "retry_task",
        "check_file_then_retry"
    }

    if recommended_action not in allowed_actions:
        print("[REMEDIATION] Action is not in the approved allowlist.")

        return {
            "status": "blocked",
            "reason": "Action not approved"
        }

    if recommended_action == "retry_task":

        return retry_task(task_id)

    if recommended_action == "check_file_then_retry":

        print("[REMEDIATION] File check + retry selected.")

        return {
            "action": "check_file_then_retry",
            "task_id": task_id,
            "status": "ready"
        }

    return {
        "status": "blocked",
        "reason": "Unknown action"
    }