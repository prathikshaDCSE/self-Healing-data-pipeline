from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "intern",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

def run_transient(**context):
    ti = context["ti"]
    print(f"Executing test_transient_failure - try_number: {ti.try_number}")
    if ti.try_number == 1:
        raise RuntimeError("Simulated transient failure")
    else:
        print("Recovered successfully!")

def run_permanent(**context):
    ti = context["ti"]
    print(f"Executing test_permanent_failure - try_number: {ti.try_number}")
    raise RuntimeError("Simulated permanent failure")

with DAG(
    dag_id="self_healing_test_pipeline",
    description="Controlled self-healing verification pipeline",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["test", "self-healing"],
) as dag:

    task_transient = PythonOperator(
        task_id="test_transient_failure",
        python_callable=run_transient,
        provide_context=True,
    )

    task_permanent = PythonOperator(
        task_id="test_permanent_failure",
        python_callable=run_permanent,
        provide_context=True,
    )
