import requests


# ============================================================
# AIRFLOW CONNECTION
# ============================================================

AIRFLOW_API_URL = "http://localhost:8080/api/v1"

AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"


# ============================================================
# CREATE AUTHENTICATED AIRFLOW SESSION
# ============================================================

def get_airflow_session():
    """
    Create an authenticated session for the Airflow REST API.
    """

    session = requests.Session()

    session.auth = (
        AIRFLOW_USERNAME,
        AIRFLOW_PASSWORD
    )

    session.headers.update({
        "Content-Type": "application/json"
    })

    return session


# ============================================================
# CHECK AIRFLOW CONNECTION
# ============================================================

def check_airflow_connection():
    """
    Check whether the Airflow REST API is reachable.
    """

    session = get_airflow_session()

    url = f"{AIRFLOW_API_URL}/health"

    response = session.get(
        url,
        timeout=10
    )

    response.raise_for_status()

    return response.json()


# ============================================================
# GET DAG RUN STATUS
# ============================================================

def get_dag_run_status(dag_id, dag_run_id):
    """
    Get the status and execution information
    for a specific DAG run.
    """

    session = get_airflow_session()

    url = (
        f"{AIRFLOW_API_URL}/dags/"
        f"{dag_id}/dagRuns/"
        f"{dag_run_id}"
    )

    response = session.get(
        url,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    return {
        "dag_id": data.get("dag_id"),
        "dag_run_id": data.get("dag_run_id"),
        "state": data.get("state"),
        "execution_date": data.get("logical_date"),
        "start_date": data.get("start_date"),
        "end_date": data.get("end_date")
    }


# ============================================================
# GET ALL TASK INSTANCES
# ============================================================

def get_task_instances(dag_id, dag_run_id):
    """
    Get all task instances belonging to a DAG run.
    """

    session = get_airflow_session()

    url = (
        f"{AIRFLOW_API_URL}/dags/"
        f"{dag_id}/dagRuns/"
        f"{dag_run_id}/taskInstances"
    )

    response = session.get(
        url,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    return data.get("task_instances", [])


# ============================================================
# GET TASK STATUS
# ============================================================

def get_task_status(dag_id, dag_run_id, task_id):
    """
    Get the current status and retry information
    for a specific Airflow task.
    """

    session = get_airflow_session()

    url = (
        f"{AIRFLOW_API_URL}/dags/"
        f"{dag_id}/dagRuns/"
        f"{dag_run_id}/taskInstances/"
        f"{task_id}"
    )

    response = session.get(
        url,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    return {
        "task_id": data.get("task_id"),
        "state": data.get("state"),
        "try_number": data.get("try_number"),
        "max_tries": data.get("max_tries"),
        "operator": data.get("operator"),
        "start_date": data.get("start_date"),
        "end_date": data.get("end_date")
    }


# ============================================================
# FIND FAILED TASKS
# ============================================================

def get_failed_tasks(dag_id, dag_run_id):
    """
    Find tasks whose actual state is 'failed'.

    Tasks with 'upstream_failed' are not treated as
    root failures because they failed as a consequence
    of another task failure.
    """

    task_instances = get_task_instances(
        dag_id,
        dag_run_id
    )

    failed_tasks = []

    for task in task_instances:

        state = task.get("state")

        if state == "failed":

            failed_tasks.append({
                "task_id": task.get("task_id"),
                "state": task.get("state"),
                "try_number": task.get("try_number"),
                "max_tries": task.get("max_tries"),
                "operator": task.get("operator"),
                "start_date": task.get("start_date"),
                "end_date": task.get("end_date")
            })

    return failed_tasks


# ============================================================
# GET TASK LOGS
# ============================================================

def get_task_logs(
    dag_id,
    dag_run_id,
    task_id,
    try_number=1
):
    """
    Retrieve logs for a specific Airflow task attempt.

    These logs will later be sent to the AI diagnosis layer.
    """

    session = get_airflow_session()

    url = (
        f"{AIRFLOW_API_URL}/dags/"
        f"{dag_id}/dagRuns/"
        f"{dag_run_id}/taskInstances/"
        f"{task_id}/logs/"
        f"{try_number}"
    )

    response = session.get(
        url,
        timeout=30
    )

    response.raise_for_status()

    return response.text


# ============================================================
# COLLECT FAILURE EVIDENCE
# ============================================================

def get_failure_evidence(
    dag_id,
    dag_run_id,
    task_id
):
    """
    Collect all information required by the AI agent
    to analyze a task failure.

    This function only collects evidence.

    It does NOT:
        - modify Airflow
        - retry tasks
        - modify files
        - modify BigQuery
        - execute remediation
    """

    print("\n[AGENT] Collecting DAG run information...")

    dag_run = get_dag_run_status(
        dag_id,
        dag_run_id
    )

    print("[AGENT] Collecting task information...")

    task_status = get_task_status(
        dag_id,
        dag_run_id,
        task_id
    )

    try_number = task_status.get("try_number")

    # Make sure we request a valid log attempt.
    if not try_number or try_number < 1:
        try_number = 1

    print(
        f"[AGENT] Collecting task logs "
        f"(attempt {try_number})..."
    )

    logs = get_task_logs(
        dag_id,
        dag_run_id,
        task_id,
        try_number
    )

    return {
        "dag_run": dag_run,
        "task": task_status,
        "logs": logs
    }


# ============================================================
# PRINT FAILURE EVIDENCE
# ============================================================

def print_failure_evidence(evidence):
    """
    Print collected failure evidence in a readable format.
    """

    print("\n")
    print("=" * 70)
    print("FAILURE EVIDENCE")
    print("=" * 70)

    print("\nDAG RUN")
    print("-" * 70)

    for key, value in evidence["dag_run"].items():
        print(f"{key}: {value}")

    print("\nTASK")
    print("-" * 70)

    for key, value in evidence["task"].items():
        print(f"{key}: {value}")

    print("\nAIRFLOW LOGS")
    print("-" * 70)

    print(evidence["logs"])

    print("=" * 70)


# ============================================================
# MAIN TEST
# ============================================================

if __name__ == "__main__":

    DAG_ID = "self_healing_pipeline"

    # Existing failed DAG run from your Airflow instance.
    DAG_RUN_ID = (
        "scheduled__2026-08-20T02:00:00+00:00"
    )

    # Actual failed task from the run.
    TASK_ID = "ingest_orders"

    print("=" * 70)
    print("AIRFLOW AGENT TOOL TEST")
    print("=" * 70)

    print("\nAirflow API:")
    print(AIRFLOW_API_URL)

    print("\nDAG:")
    print(DAG_ID)

    print("\nDAG Run:")
    print(DAG_RUN_ID)

    print("\nTask:")
    print(TASK_ID)

    # --------------------------------------------------------
    # 1. Test Airflow connection
    # --------------------------------------------------------

    print("\n")
    print("=" * 70)
    print("STEP 1 - TEST AIRFLOW CONNECTION")
    print("=" * 70)

    try:

        health = check_airflow_connection()

        print("\nAirflow API connection: SUCCESS")

        print("\nAirflow response:")
        print(health)

    except Exception as e:

        print("\nAirflow API connection: FAILED")
        print(f"Error: {e}")

        raise SystemExit(1)

    # --------------------------------------------------------
    # 2. Get DAG run status
    # --------------------------------------------------------

    print("\n")
    print("=" * 70)
    print("STEP 2 - GET DAG RUN STATUS")
    print("=" * 70)

    try:

        dag_run = get_dag_run_status(
            DAG_ID,
            DAG_RUN_ID
        )

        print("\nDAG Run status:")
        print(dag_run)

    except Exception as e:

        print("\nFailed to get DAG run status.")
        print(f"Error: {e}")

        raise SystemExit(1)

    # --------------------------------------------------------
    # 3. Find failed tasks
    # --------------------------------------------------------

    print("\n")
    print("=" * 70)
    print("STEP 3 - FIND FAILED TASKS")
    print("=" * 70)

    try:

        failed_tasks = get_failed_tasks(
            DAG_ID,
            DAG_RUN_ID
        )

        if not failed_tasks:

            print("\nNo failed tasks found.")

        else:

            print(
                f"\nFound {len(failed_tasks)} failed task(s):"
            )

            for task in failed_tasks:

                print("\nTask:")
                print(f"  Task ID: {task['task_id']}")
                print(f"  State: {task['state']}")
                print(f"  Try number: {task['try_number']}")
                print(f"  Max tries: {task['max_tries']}")
                print(f"  Operator: {task['operator']}")

    except Exception as e:

        print("\nFailed to retrieve task instances.")
        print(f"Error: {e}")

        raise SystemExit(1)

    # --------------------------------------------------------
    # 4. Collect evidence for ingest_orders
    # --------------------------------------------------------

    print("\n")
    print("=" * 70)
    print("STEP 4 - COLLECT FAILURE EVIDENCE")
    print("=" * 70)

    try:

        evidence = get_failure_evidence(
            DAG_ID,
            DAG_RUN_ID,
            TASK_ID
        )

        print_failure_evidence(evidence)

    except Exception as e:

        print("\nFailed to collect failure evidence.")
        print(f"Error: {e}")

        raise SystemExit(1)

    # --------------------------------------------------------
    # 5. Finish
    # --------------------------------------------------------

    print("\n")
    print("=" * 70)
    print("AIRFLOW AGENT TOOL TEST COMPLETED")
    print("=" * 70)

    print("\nNext step:")
    print("Send this failure evidence to the AI diagnosis layer.")