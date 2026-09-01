import os
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "intern",
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
}

DATA_DIR = "/opt/airflow/data"

def ingest_orders(**context):
    source_file = os.path.join(DATA_DIR, "orders.csv")
    staging_file = os.path.join(DATA_DIR, "orders_stage.csv")
    
    print(f"Reading source file: {source_file}")
    if not os.path.exists(source_file):
        raise FileNotFoundError(f"Orders file missing: {source_file}")
        
    with open(source_file, "r") as f_src:
        content = f_src.read()
    with open(staging_file, "w") as f_stg:
        f_stg.write(content)
        
    print(f"Successfully staged orders to {staging_file}")


def validate_schema(**context):
    staging_file = os.path.join(DATA_DIR, "orders_stage.csv")
    print(f"Validating schema for: {staging_file}")
    if not os.path.exists(staging_file):
        raise FileNotFoundError(f"Staged orders file missing: {staging_file}")
        
    expected_cols = ["order_id", "customer_id", "amount", "order_date"]
    
    with open(staging_file, "r") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        
    if not header:
        raise ValueError("Staged file is empty")
        
    # Check for missing column or mismatch
    for col in expected_cols:
        if col not in header:
            if col == "customer_id" and "customer_identifier" in header:
                raise ValueError("orders.csv schema mismatch. Expected customer_id but found customer_identifier")
            else:
                actual_col = header[1] if len(header) > 1 else "unknown"
                raise ValueError(f"orders.csv schema mismatch. Expected {col} but found {actual_col}")
            
    print("Schema validation passed.")


def validate_quality(**context):
    staging_file = os.path.join(DATA_DIR, "orders_stage.csv")
    print(f"Validating quality for: {staging_file}")
    if not os.path.exists(staging_file):
        raise FileNotFoundError(f"Staged orders file missing: {staging_file}")
        
    with open(staging_file, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        
    # Check 1: Duplicate order_ids
    seen_ids = set()
    duplicates = []
    for row in rows:
        order_id = row.get("order_id")
        if not order_id:
            continue
        if order_id in seen_ids:
            duplicates.append(order_id)
        else:
            seen_ids.add(order_id)
            
    if duplicates:
        raise ValueError("Duplicate order_ids detected for 2026-06-15")
        
    # Check 2: Null/Empty values in key columns
    for idx, row in enumerate(rows):
        cust_id = row.get("customer_id")
        amount = row.get("amount")
        if not cust_id or cust_id.strip() == "" or not amount or amount.strip() == "":
            raise ValueError("Null value violation: customer_id or amount cannot be null")
            
    print("Data quality checks passed.")


def load_orders(**context):
    staging_file = os.path.join(DATA_DIR, "orders_stage.csv")
    final_file = os.path.join(DATA_DIR, "orders_final.csv")
    print(f"Loading orders from staging {staging_file} to final destination {final_file}")
    if not os.path.exists(staging_file):
        raise FileNotFoundError(f"Staged orders file missing: {staging_file}")
        
    with open(staging_file, "r") as f_stg:
        content = f_stg.read()
    with open(final_file, "w") as f_fin:
        f_fin.write(content)
        
    print("Load task completed successfully.")


with DAG(
    dag_id="self_healing_real_pipeline",
    description="Real data processing and validation self-healing pipeline",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["production", "self-healing"],
) as dag:

    task_ingest = PythonOperator(
        task_id="ingest_orders",
        python_callable=ingest_orders,
        provide_context=True,
    )

    task_validate_schema = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
        provide_context=True,
    )

    task_validate_quality = PythonOperator(
        task_id="validate_quality",
        python_callable=validate_quality,
        provide_context=True,
    )

    task_load = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders,
        provide_context=True,
    )

    task_ingest >> task_validate_schema >> task_validate_quality >> task_load
