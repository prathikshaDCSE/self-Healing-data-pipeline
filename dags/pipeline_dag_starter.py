"""
Starter DAG skeleton — self_healing_pipeline

This is scaffolding only. It shows the REQUIRED shape (task lineage,
retries, on_failure_callback, BigQuery operators) but leaves the actual
validation / transform / agent logic as TODOs for the intern to implement.

Do not hand out a filled-in version of this file — the point is for
interns to build the logic themselves.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)


def on_task_failure(context):
    """
    Hook called whenever any task in this DAG fails. This is where the
    agent should be invoked to diagnose + (auto-fix or escalate).

    context contains: task_instance, dag_run, exception, etc.
    TODO(intern): call your agent here, passing context so it can pull
    logs/state for the failed task run.
    """
    raise NotImplementedError("TODO: wire this up to your agent")


default_args = {
    "owner": "intern",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_task_failure,
    "sla": timedelta(hours=26),  # ties to freshness SLA in pipeline_config.yaml
}

with DAG(
    dag_id="self_healing_pipeline",
    description="E2E ingestion -> validation -> transform -> BigQuery load, with agent monitoring",
    schedule="0 2 * * *",   # daily at 02:00, i.e. after the "day" closes
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["intern-project", "self-healing"],
) as dag:

    def ingest_orders(**context):
        """TODO(intern): read the day's orders CSV, do light parsing, stage it."""
        raise NotImplementedError

    def ingest_events(**context):
        """TODO(intern): pull the day's events (mock API or file), stage it."""
        raise NotImplementedError

    def validate_schema(**context):
        """
        TODO(intern): compare staged data's schema against pipeline_config.yaml.
        Should raise on drift, not silently coerce.
        """
        raise NotImplementedError

    def validate_quality(**context):
        """
        TODO(intern): row-count range, null-rate, referential integrity checks
        against pipeline_config.yaml thresholds.
        """
        raise NotImplementedError

    t_ingest_orders = PythonOperator(
        task_id="ingest_orders",
        python_callable=ingest_orders,
    )

    t_ingest_events = PythonOperator(
        task_id="ingest_events",
        python_callable=ingest_events,
    )

    t_validate_schema = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
    )

    t_validate_quality = PythonOperator(
        task_id="validate_quality",
        python_callable=validate_quality,
    )

    # TODO(intern): replace with your actual load — this is a placeholder
    # showing the required operator + WRITE_TRUNCATE-per-partition pattern
    # to keep loads idempotent (prevents the duplicate_ingestion fault from
    # silently doubling data on retries/backfills).
    t_load_orders_bq = BigQueryInsertJobOperator(
        task_id="load_orders_to_bq",
        configuration={
            "query": {
                "query": "SELECT 1",  # TODO(intern): real MERGE/load query
                "useLegacySql": False,
            }
        },
    )

    # Example of a declarative check operator against BigQuery — useful for
    # cheap, DAG-native validation in addition to your agent's own checks.
    t_bq_row_count_check = BigQueryCheckOperator(
        task_id="check_orders_row_count",
        sql="""
            SELECT COUNT(*) BETWEEN 240 AND 360
            FROM `{{ params.project }}.{{ params.dataset }}.fct_orders`
            WHERE DATE(order_ts) = '{{ ds }}'
        """,
        use_legacy_sql=False,
        params={"project": "<YOUR_GCP_PROJECT_ID>", "dataset": "pipeline_intern_<HANDLE>"},
    )

    def run_agent_monitor(**context):
        """
        TODO(intern): this is the "happy path" monitoring task — runs after
        a successful load to do deeper checks the DAG-native operators don't
        cover (e.g. cross-table referential integrity), and to close out any
        open incidents. The failure-path entry point is on_task_failure above.
        """
        raise NotImplementedError

    t_agent_monitor = PythonOperator(
        task_id="agent_monitor",
        python_callable=run_agent_monitor,
    )

    [t_ingest_orders, t_ingest_events] >> t_validate_schema >> t_validate_quality
    t_validate_quality >> t_load_orders_bq >> t_bq_row_count_check >> t_agent_monitor
