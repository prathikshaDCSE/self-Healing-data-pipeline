import argparse
import json
import sys

from airflow_tools import (
    check_airflow_connection,
    get_dag_runs,
    get_dag_run_status,
    get_task_status,
    get_failed_tasks,
    get_task_logs,
)
from ai_agent import analyze_failure
from remediation import execute_remediation


def main():
    parser = argparse.ArgumentParser(
        description="Airflow Self-Healing Pipeline - Agent Runner Orchestrator"
    )
    parser.add_argument(
        "--dag",
        type=str,
        default="self_healing_pipeline",
        help="Target DAG ID to analyze (default: self_healing_pipeline)"
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Target DAG run ID (optional, defaults to latest failed run)"
    )
    parser.add_argument(
        "--task-id",
        type=str,
        default=None,
        help="Target Task ID (optional, defaults to first failed task in resolved run)"
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Run in non-interactive mode (automatically blocks unsafe actions without operator prompt)"
    )

    args = parser.parse_args()


    print("=" * 70)
    print("AGENT RUNNER ORCHESTRATOR")
    print("=" * 70)

    # 1. Connect to Airflow
    print("\n[ORCHESTRATOR] Connecting to Airflow...")
    try:
        health = check_airflow_connection()
        print("[ORCHESTRATOR] Airflow Connection: SUCCESS")
    except Exception as e:
        print(f"[ORCHESTRATOR] ERROR: Failed to connect to Airflow: {e}")
        sys.exit(1)

    # 2. Find/Resolve DAG run and task
    dag_id = args.dag
    run_id = args.run_id
    task_id = args.task_id

    # Resolve DAG Run ID
    if not run_id:
        print(f"[ORCHESTRATOR] Auto-discovering failed runs for DAG: {dag_id}...")
        try:
            dag_runs = get_dag_runs(dag_id)
        except Exception as e:
            print(f"[ORCHESTRATOR] ERROR: Failed to fetch DAG runs: {e}")
            sys.exit(1)

        failed_runs = [r for r in dag_runs if r.get("state") == "failed"]
        if not failed_runs:
            print(f"[ORCHESTRATOR] No failed runs found for DAG: {dag_id}. Exiting.")
            sys.exit(0)

        # Sort by logical_date/execution_date descending (newest first)
        # Note: Airflow REST API returns 'logical_date' for run date representation.
        failed_runs.sort(key=lambda r: r.get("logical_date", ""), reverse=True)
        resolved_run = failed_runs[0]
        run_id = resolved_run["dag_run_id"]
        print(f"[ORCHESTRATOR] Resolved to latest failed Run ID: {run_id}")
    else:
        print(f"[ORCHESTRATOR] Using specified Run ID: {run_id}")

    # Get execution date for context
    try:
        run_status = get_dag_run_status(dag_id, run_id)
        execution_date = run_status.get("execution_date")
    except Exception as e:
        print(f"[ORCHESTRATOR] WARNING: Could not fetch DAG run status details: {e}")
        execution_date = "Unknown"

    # Resolve Task ID
    try:
        failed_tasks = get_failed_tasks(dag_id, run_id)
    except Exception as e:
        print(f"[ORCHESTRATOR] ERROR: Failed to get task instances: {e}")
        sys.exit(1)

    resolved_task = None
    if not task_id:
        if not failed_tasks:
            print(f"[ORCHESTRATOR] No failed tasks found in Run ID: {run_id}. Exiting.")
            sys.exit(0)
        resolved_task = failed_tasks[0]
        task_id = resolved_task["task_id"]
        print(f"[ORCHESTRATOR] Auto-discovered failed Task ID: {task_id}")
    else:
        # Match user-provided task_id
        for t in failed_tasks:
            if t["task_id"] == task_id:
                resolved_task = t
                break
        
        if not resolved_task:
            print(f"[ORCHESTRATOR] Task '{task_id}' was not marked as failed in run '{run_id}'.")
            print("[ORCHESTRATOR] Checking actual status...")
            try:
                task_status = get_task_status(dag_id, run_id, task_id)
                resolved_task = task_status
                print(f"[ORCHESTRATOR] Task current state: {task_status.get('state')}")
            except Exception as e:
                print(f"[ORCHESTRATOR] ERROR: Failed to fetch task status for {task_id}: {e}")
                sys.exit(1)

    # 3. Retrieve task logs
    try_number = resolved_task.get("try_number") or 1
    # Make sure we use a valid integer attempt
    try:
        try_number = int(try_number)
        if try_number < 1:
            try_number = 1
    except ValueError:
        try_number = 1

    print(f"[ORCHESTRATOR] Fetching logs for task '{task_id}' (attempt {try_number})...")
    try:
        logs = get_task_logs(dag_id, run_id, task_id, try_number)
        print(f"[ORCHESTRATOR] Retrieved logs successfully ({len(logs)} characters)")
    except Exception as e:
        print(f"[ORCHESTRATOR] ERROR: Failed to retrieve task logs: {e}")
        sys.exit(1)

    # 4. AI Diagnosis
    print(f"[ORCHESTRATOR] Initiating AI diagnosis for task '{task_id}'...")
    ai_result = analyze_failure(
        task_id=task_id,
        error_message=logs,
        execution_date=execution_date
    )

    print("\n" + "=" * 70)
    print("AI DIAGNOSIS RESULTS")
    print("=" * 70)
    print(json.dumps(ai_result, indent=4))

    # 5. Remediation Controller
    print("\n[ORCHESTRATOR] Sending diagnosis to Remediation Controller...")
    remediation_result = execute_remediation(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        failure_type=ai_result.get("failure_type", "unknown"),
        recommended_action=ai_result.get("recommended_action", "manual_review"),
        safe=ai_result.get("auto_remediation_safe", False),
        logs=logs,
        interactive=not args.non_interactive
    )


    # 6. Print Complete Decision
    print("\n" + "=" * 70)
    print("FINAL REMEDIATION DECISION")
    print("=" * 70)
    print(f"Target DAG:          {dag_id}")
    print(f"Target Run ID:      {run_id}")
    print(f"Target Task ID:     {task_id}")
    print(f"AI Failure Type:    {ai_result.get('failure_type')}")
    print(f"AI Recommendation:  {ai_result.get('recommended_action')}")
    print(f"Safety Validation:  {'APPROVED & EXECUTED' if remediation_result.get('status') in ['remediated', 'remediation_started'] else 'BLOCKED (Requires Manual Review)'}")
    print("-" * 70)
    print("Remediation Status Output:")
    print(json.dumps(remediation_result, indent=4))
    print("=" * 70)



if __name__ == "__main__":
    main()
