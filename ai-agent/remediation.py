import os
import time
import re
import sys

from airflow_tools import get_task_status, clear_task_instance


# ============================================================
# SAFETY CLASSIFICATION GROUPS
# ============================================================

SAFE_ACTIONS = {
    "retry_task",
    "check_file",
}

UNSAFE_APPROVAL_ACTIONS = {
    "fix_schema",
    "remove_duplicates",
    "clean_invalid_data",
    "regenerate_data",
    "investigate_foreign_key",
    "investigate_volume",
}

ALWAYS_BLOCKED_ACTIONS = {
    "manual_review",
    "no_action",
}


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def check_file_exists(file_path):
    """
    Check whether the expected data file exists.
    """
    exists = os.path.exists(file_path)

    print(f"[REMEDIATION] Checking file: {file_path}")
    print(f"[REMEDIATION] File exists: {exists}")

    return exists


def extract_file_path(logs):
    """
    Extract file path from task logs using regex.
    Identifies patterns like FileNotFoundError or Orders file missing.
    """
    patterns = [
        r"FileNotFoundError:\s*Orders file missing:\s*([^\s'\"]+)",
        r"FileNotFoundError:\s*.*No such file or directory:\s*['\"]?([^\s'\"]+)['\"]?",
        r"Orders file missing:\s*([^\s'\"]+)",
        r"file missing:\s*([^\s'\"]+)",
        r"File not found:\s*([^\s'\"]+)"
    ]

    for pattern in patterns:
        match = re.search(pattern, logs, re.IGNORECASE)
        if match:
            return match.group(1).strip()

    return None


# ============================================================
# MAIN REMEDIATION CONTROLLER
# ============================================================

def execute_remediation(
    dag_id,
    run_id,
    task_id,
    failure_type,
    recommended_action,
    safe,
    logs,
    interactive=True
):
    """
    Deterministic Safety Controller.

    Enforces the Golden Policy Matrix:
    - Safe actions (retry_task, check_file) are executed automatically if budget/state checks pass.
    - Unsafe actions (fix_schema, remove_duplicates, etc.) trigger an interactive operator confirmation.
    - Manual review & no action recommendations are immediately blocked.
    """

    print("\n" + "=" * 70)
    print("REMEDIATION CONTROLLER")
    print("=" * 70)
    print(f"DAG:                {dag_id}")
    print(f"Run ID:             {run_id}")
    print(f"Task:               {task_id}")
    print(f"Failure type:       {failure_type}")
    print(f"AI recommendation:  {recommended_action}")

    # 1. Rule 1: Always Blocked Actions (e.g., manual_review, no_action)
    if (recommended_action in ALWAYS_BLOCKED_ACTIONS or 
        (recommended_action not in SAFE_ACTIONS and recommended_action not in UNSAFE_APPROVAL_ACTIONS)):
        print(f"[REMEDIATION] Action '{recommended_action}' is UNSAFE and always BLOCKED.")
        return {
            "status": "blocked",
            "action": recommended_action,
            "reason": f"Action '{recommended_action}' requires manual investigation and cannot be automated"
        }

    # 2. Rule 2: Unsafe Actions requiring Human Approval
    if recommended_action in UNSAFE_APPROVAL_ACTIONS:
        if not interactive:
            print(
                f"[REMEDIATION] Non-interactive mode: Action '{recommended_action}' "
                f"requires human approval. BLOCKED."
            )
            return {
                "status": "blocked",
                "action": recommended_action,
                "reason": (
                    f"Action '{recommended_action}' requires human approval "
                    f"but run in non-interactive mode"
                )
            }
        
        print("\n" + "!" * 70)
        print(f"[HUMAN APPROVAL REQUIRED] Proposed Action: {recommended_action}")
        print("!" * 70)
        
        # Flush output stream to ensure user sees prompt
        sys.stdout.flush()
        choice = input(
            f"Do you want to approve and execute this remediation? (y/N): "
        ).strip().lower()

        if choice in ["y", "yes"]:
            print(
                f"[REMEDIATION] Approved! Simulating execution "
                f"of '{recommended_action}'..."
            )
            time.sleep(2)
            print(f"[REMEDIATION] Simulation of '{recommended_action}' succeeded.")
            return {
                "status": "remediated",
                "action": recommended_action,
                "initial_state": "failed",
                "final_state": "success",
                "approved_by_human": True
            }
        else:
            print("[REMEDIATION] Action rejected by human operator. BLOCKED.")
            return {
                "status": "blocked",
                "action": recommended_action,
                "reason": "Rejected by human operator"
            }

    # 3. Rule 3: SAFE Actions (retry_task, check_file)
    # Double check state in Airflow
    try:
        current_status = get_task_status(dag_id, run_id, task_id)
        current_state = current_status.get("state")
        try_number = current_status.get("try_number") or 1
        max_tries = current_status.get("max_tries") or 0
        print(
            f"[REMEDIATION] Current state in Airflow: {current_state} "
            f"(try {try_number} of {max_tries + 1})"
        )
    except Exception as e:
        print(f"[REMEDIATION] ERROR: Failed to double check task state: {e}")
        return {
            "status": "blocked",
            "action": recommended_action,
            "reason": f"Could not verify current task status: {e}"
        }

    # Only execute remediation if task is actually failed.
    if current_state != "failed":
        print(
            f"[REMEDIATION] Current state is '{current_state}', "
            f"not 'failed'. Doing nothing."
        )
        return {
            "status": "blocked",
            "action": recommended_action,
            "reason": f"Task state is '{current_state}', expected 'failed'"
        }

    # Loop protection limit check
    limit = 1 if dag_id == "self_healing_test_pipeline" else 3
    if int(try_number) > limit:
        print(
            f"[REMEDIATION] Retry budget exceeded "
            f"(try {try_number} > {limit}). "
            f"Blocking to prevent infinite loops."
        )
        return {
            "status": "blocked",
            "action": recommended_action,
            "reason": f"Retry budget exceeded (attempts: {try_number}/{limit})"
        }

    # File path validation for check_file
    if recommended_action == "check_file":
        file_path_container = extract_file_path(logs)
        if not file_path_container:
            print("[REMEDIATION] ERROR: Could not extract file path from logs.")
            return {
                "status": "blocked",
                "action": recommended_action,
                "reason": "Could not extract file path from logs"
            }
        
        # Strip the container directory prefix (/opt/<any>/data/)
        relative_path = re.sub(r"^/opt/[^/]+/data/", "", file_path_container)
        # Combine with host data directory
        host_dir = "c:\\Users\\dhana\\Desktop\\data-pipeline\\self-Healing-data-pipeline\\data\\"
        file_path_host = os.path.join(host_dir, relative_path)
        # Handle slash conversions for Windows
        file_path_host = file_path_host.replace("/", "\\")


        print(f"[REMEDIATION] Extracted file path (container): {file_path_container}")
        print(f"[REMEDIATION] Translated host path:            {file_path_host}")

        if not check_file_exists(file_path_host):
            print("[REMEDIATION] File is still missing. Remediation blocked.")
            return {
                "status": "blocked",
                "action": recommended_action,
                "reason": f"File is still missing at: {file_path_host}"
            }
        print("[REMEDIATION] File check passed! Task is ready for clear/retry.")

    # 4. Clear Task Instance (Schedule Retry)
    print(f"[REMEDIATION] Clearing task instance {task_id} in Airflow to trigger retry...")
    try:
        clear_result = clear_task_instance(dag_id, run_id, task_id)
        print(f"[REMEDIATION] Clear API response: {clear_result}")
    except Exception as e:
        print(f"[REMEDIATION] ERROR: Failed to clear task instance: {e}")
        return {
            "status": "blocked",
            "action": recommended_action,
            "reason": f"Failed to clear task instance: {e}"
        }

    # 5. Verification after remediation (poll status success vs failed)
    print("[REMEDIATION] Waiting for Airflow scheduler to execute the retried task...")
    max_wait = 180  # 3 minutes
    poll_interval = 10
    start_time = time.time()
    
    final_state = "unknown"
    while time.time() - start_time < max_wait:
        time.sleep(poll_interval)
        try:
            status = get_task_status(dag_id, run_id, task_id)
            state = status.get("state")
            print(f"[REMEDIATION] Polling task state: {state}")
            if state in ["success", "failed"]:
                final_state = state
                break
        except Exception as e:
            print(f"[REMEDIATION] WARNING: Failed to poll task state: {e}")

    if final_state == "success":
        print("[REMEDIATION] Task HEALED successfully!")
        return {
            "status": "remediated",
            "action": recommended_action,
            "initial_state": "failed",
            "final_state": "success"
        }
    elif final_state == "failed":
        print("[REMEDIATION] Task FAILED again. Remediation failed.")
        return {
            "status": "blocked",
            "action": recommended_action,
            "initial_state": "failed",
            "final_state": "failed",
            "reason": "Automatic retry did not recover the task"
        }
    else:
        print("[REMEDIATION] Task execution timed out or state is unknown.")
        return {
            "status": "remediation_started",
            "action": recommended_action,
            "initial_state": "failed",
            "final_state": "running"
        }