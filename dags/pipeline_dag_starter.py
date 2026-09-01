"""
Self-Healing Data Pipeline - self_healing_pipeline

Implemented:
- BigQuery table creation
- Dimension table loading
- Daily orders staging
- Daily orders loading using partition-level WRITE_TRUNCATE
- Orders row-count validation

TODO:
- Events ingestion
- Schema validation
- Quality validation
- Events fact loading
- Agent monitoring
- Self-healing logic
"""

from datetime import datetime, timedelta
import csv
import json
import os
import yaml


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)

from google.cloud import bigquery


PROJECT_ID = "river-pillar-506507-b4"
DATASET_ID = "pipeline_prathiksha"
BIGQUERY_LOCATION = "US"
MAX_QUERY_BYTES_BILLED = 100 * 1024 * 1024


def on_task_failure(context):
    """
    Called whenever a task fails.
    """
    exception = context.get("exception")
    task_instance = context.get("task_instance")

    if exception and task_instance:
        print(f"Task FAILURE detected on task {task_instance.task_id}: {exception}")



default_args = {
    "owner": "intern",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_task_failure,
    "sla": timedelta(hours=26),
}


with DAG(
    dag_id="self_healing_pipeline",
    description=(
        "E2E ingestion -> validation -> BigQuery load -> monitoring"
    ),
    schedule="0 2 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["intern-project", "self-healing"],
) as dag:

    def ingest_orders(**context):
        logical_date = context["logical_date"]
        processing_date = logical_date.strftime("%Y-%m-%d")

        orders_file = (
            f"/opt/airflow/data/orders/"
            f"orders_{processing_date}.csv"
        )

        if not os.path.exists(orders_file):
            raise FileNotFoundError(
                f"Orders file not found: {orders_file}"
            )

        file_size = os.path.getsize(orders_file)
        if file_size == 0:
            raise ValueError(
                f"Orders file is empty: {orders_file}"
            )

        with open(orders_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            row_count = len(rows)

        print(f"Ingested orders for {processing_date}:")
        print(f"  Source file: {orders_file}")
        print(f"  File size: {file_size} bytes")
        print(f"  Row count: {row_count}")

        if not 240 <= row_count <= 360:
            raise ValueError(
                f"Orders row count {row_count} for {processing_date} is "
                f"outside allowed range (240-360)"
            )

        print("ingest_orders completed successfully.")


    def ingest_events(**context):
        logical_date = context["logical_date"]
        processing_date = logical_date.strftime("%Y-%m-%d")
        partition_id = logical_date.strftime("%Y%m%d")

        events_file = (
            f"/opt/airflow/data/events/"
            f"events_{processing_date}.jsonl"
        )

        if not os.path.exists(events_file):
            raise FileNotFoundError(
                f"Events file not found: {events_file}"
            )

        file_size = os.path.getsize(events_file)
        if file_size == 0:
            raise ValueError(
                f"Events file is empty: {events_file}"
            )

        events = []
        with open(events_file, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    events.append(record)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"Malformed JSON in {events_file} at line {line_num}: {e}"
                    )

        source_count = len(events)
        print(
            f"Events source count for {processing_date}: {source_count} "
            f"records (file size: {file_size} bytes)"
        )

        if not 900 <= source_count <= 1500:
            raise ValueError(
                f"Events row count {source_count} for {processing_date} is "
                f"outside allowed range (900-1500)"
            )

        client = bigquery.Client(project=PROJECT_ID)
        destination_partition = (
            f"{PROJECT_ID}.{DATASET_ID}.fct_events${partition_id}"
        )
        destination_table = f"{PROJECT_ID}.{DATASET_ID}.fct_events"

        load_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            schema=[
                bigquery.SchemaField("event_id", "STRING"),
                bigquery.SchemaField("customer_id", "STRING"),
                bigquery.SchemaField("event_type", "STRING"),
                bigquery.SchemaField("session_id", "STRING"),
                bigquery.SchemaField("event_ts", "TIMESTAMP"),
            ],
        )

        with open(events_file, "rb") as f:
            load_job = client.load_table_from_file(
                f,
                destination_partition,
                job_config=load_config,
                location=BIGQUERY_LOCATION,
            )

        load_job.result()
        print(f"Events load job ID: {load_job.job_id}, state: {load_job.state}")

        verification_query = f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT event_id) AS unique_event_ids,
                COUNTIF(event_ts IS NULL) AS null_event_ts,
                MIN(event_ts) AS min_event_ts,
                MAX(event_ts) AS max_event_ts
            FROM `{destination_table}`
            WHERE DATE(event_ts) = @processing_date
        """

        verification_job = client.query(
            verification_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "processing_date", "DATE", processing_date
                    ),
                ],
                use_query_cache=False,
                maximum_bytes_billed=MAX_QUERY_BYTES_BILLED,
            ),
            location=BIGQUERY_LOCATION,
        )

        verification = next(verification_job.result())

        print(f"Verified BigQuery total rows: {verification.total_rows}")
        print(f"Verified unique event IDs: {verification.unique_event_ids}")
        print(f"Verified NULL event_ts: {verification.null_event_ts}")
        print(f"Verified min event_ts: {verification.min_event_ts}")
        print(f"Verified max event_ts: {verification.max_event_ts}")

        if verification.total_rows != source_count:
            raise ValueError(
                f"Events partition row count mismatch for {processing_date}. "
                f"Expected {source_count}, found {verification.total_rows}."
            )

        if verification.unique_event_ids != source_count:
            raise ValueError(
                f"Duplicate event_ids detected in fct_events for {processing_date}. "
                f"Expected {source_count} unique IDs, found {verification.unique_event_ids}."
            )

        if verification.null_event_ts != 0:
            raise ValueError(
                f"NULL event_ts values detected in fct_events for {processing_date}: "
                f"{verification.null_event_ts}"
            )

        print("ingest_events completed successfully.")


    def validate_schema(**context):
        logical_date = context["logical_date"]
        processing_date = logical_date.strftime("%Y-%m-%d")

        config_path = "/opt/airflow/config/pipeline_config.yaml"
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        datasets = config.get("datasets", {})

        # 1. Validate customers.csv
        customers_file = "/opt/airflow/data/customers.csv"
        if not os.path.exists(customers_file):
            raise FileNotFoundError(f"Customers file missing: {customers_file}")
        with open(customers_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            expected_cust_cols = list(datasets["customers"]["schema"].keys())
            if header != expected_cust_cols:
                raise ValueError(
                    f"customers.csv schema mismatch. Expected {expected_cust_cols}, found {header}"
                )

        # 2. Validate products.csv
        products_file = "/opt/airflow/data/products.csv"
        if not os.path.exists(products_file):
            raise FileNotFoundError(f"Products file missing: {products_file}")
        with open(products_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            expected_prod_cols = list(datasets["products"]["schema"].keys())
            if header != expected_prod_cols:
                raise ValueError(
                    f"products.csv schema mismatch. Expected {expected_prod_cols}, found {header}"
                )

        # 3. Validate orders_{processing_date}.csv
        orders_file = f"/opt/airflow/data/orders/orders_{processing_date}.csv"
        if not os.path.exists(orders_file):
            raise FileNotFoundError(f"Orders file missing: {orders_file}")
        with open(orders_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            expected_order_cols = list(datasets["orders"]["schema"].keys())
            if header != expected_order_cols:
                raise ValueError(
                    f"orders_{processing_date}.csv schema mismatch. Expected {expected_order_cols}, found {header}"
                )

        # 4. Validate events_{processing_date}.jsonl
        events_file = f"/opt/airflow/data/events/events_{processing_date}.jsonl"
        if not os.path.exists(events_file):
            raise FileNotFoundError(f"Events file missing: {events_file}")
        expected_event_keys = set(datasets["events"]["schema"].keys())
        with open(events_file, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
            if not first_line:
                raise ValueError(f"Events file is empty: {events_file}")
            first_record = json.loads(first_line)
            record_keys = set(first_record.keys())
            if record_keys != expected_event_keys:
                raise ValueError(
                    f"events_{processing_date}.jsonl schema mismatch. Expected keys {expected_event_keys}, found {record_keys}"
                )

        print(
            "validate_schema completed successfully: All headers and structures match pipeline_config.yaml."
        )


    def validate_quality(**context):
        logical_date = context["logical_date"]
        processing_date = logical_date.strftime("%Y-%m-%d")

        client = bigquery.Client(project=PROJECT_ID)

        # 1. Orders Quality Verification
        orders_table = f"{PROJECT_ID}.{DATASET_ID}.fct_orders"
        orders_query = f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT order_id) AS unique_orders,
                COUNTIF(customer_id IS NULL) AS null_customer_id,
                COUNTIF(order_total IS NULL) AS null_order_total,
                COUNTIF(order_ts IS NULL) AS null_order_ts
            FROM `{orders_table}`
            WHERE DATE(order_ts) = @processing_date
        """
        orders_res = next(
            client.query(
                orders_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "processing_date", "DATE", processing_date
                        ),
                    ],
                    use_query_cache=False,
                    maximum_bytes_billed=MAX_QUERY_BYTES_BILLED,
                ),
                location=BIGQUERY_LOCATION,
            ).result()
        )

        print(f"Orders quality metrics for {processing_date}:")
        print(f"  total_rows: {orders_res.total_rows}")
        print(f"  unique_orders: {orders_res.unique_orders}")
        print(f"  null_customer_id: {orders_res.null_customer_id}")
        print(f"  null_order_total: {orders_res.null_order_total}")
        print(f"  null_order_ts: {orders_res.null_order_ts}")

        if not 240 <= orders_res.total_rows <= 360:
            raise ValueError(
                f"Orders count {orders_res.total_rows} out of bounds (240-360)"
            )

        if orders_res.unique_orders != orders_res.total_rows:
            raise ValueError(
                f"Orders duplicate IDs found: total {orders_res.total_rows}, unique {orders_res.unique_orders}"
            )

        if orders_res.null_customer_id > 0:
            raise ValueError(
                f"NULL customer_id in orders: {orders_res.null_customer_id}"
            )

        null_total_pct = (
            orders_res.null_order_total / float(orders_res.total_rows)
            if orders_res.total_rows > 0
            else 0
        )
        if null_total_pct > 0.02:
            raise ValueError(
                f"NULL order_total exceeds 2% tolerance: {null_total_pct:.2%}"
            )

        # 2. Events Quality Verification
        events_table = f"{PROJECT_ID}.{DATASET_ID}.fct_events"
        events_query = f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT event_id) AS unique_events,
                COUNTIF(customer_id IS NULL) AS null_customer_id,
                COUNTIF(event_ts IS NULL) AS null_event_ts
            FROM `{events_table}`
            WHERE DATE(event_ts) = @processing_date
        """
        events_res = next(
            client.query(
                events_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "processing_date", "DATE", processing_date
                        ),
                    ],
                    use_query_cache=False,
                    maximum_bytes_billed=MAX_QUERY_BYTES_BILLED,
                ),
                location=BIGQUERY_LOCATION,
            ).result()
        )

        print(f"Events quality metrics for {processing_date}:")
        print(f"  total_rows: {events_res.total_rows}")
        print(f"  unique_events: {events_res.unique_events}")
        print(f"  null_customer_id: {events_res.null_customer_id}")
        print(f"  null_event_ts: {events_res.null_event_ts}")

        if not 900 <= events_res.total_rows <= 1500:
            raise ValueError(
                f"Events count {events_res.total_rows} out of bounds (900-1500)"
            )

        if events_res.unique_events != events_res.total_rows:
            raise ValueError(
                f"Events duplicate IDs found: total {events_res.total_rows}, unique {events_res.unique_events}"
            )

        if events_res.null_customer_id > 0:
            raise ValueError(
                f"NULL customer_id in events: {events_res.null_customer_id}"
            )

        # 3. Referential Integrity Check
        ref_orders_cust_query = f"""
            SELECT COUNT(*) AS invalid_count
            FROM `{orders_table}` o
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_customers` c
              ON o.customer_id = c.customer_id
            WHERE DATE(o.order_ts) = @processing_date
              AND c.customer_id IS NULL
        """
        invalid_cust_orders = next(
            client.query(
                ref_orders_cust_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "processing_date", "DATE", processing_date
                        ),
                    ],
                    use_query_cache=False,
                    maximum_bytes_billed=MAX_QUERY_BYTES_BILLED,
                ),
                location=BIGQUERY_LOCATION,
            ).result()
        ).invalid_count

        if invalid_cust_orders > 0:
            raise ValueError(
                f"Referential integrity failure: {invalid_cust_orders} orders have non-existent customer_ids"
            )

        ref_orders_prod_query = f"""
            SELECT COUNT(*) AS invalid_count
            FROM `{orders_table}` o
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.dim_products` p
              ON o.product_id = p.product_id
            WHERE DATE(o.order_ts) = @processing_date
              AND p.product_id IS NULL
        """
        invalid_prod_orders = next(
            client.query(
                ref_orders_prod_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "processing_date", "DATE", processing_date
                        ),
                    ],
                    use_query_cache=False,
                    maximum_bytes_billed=MAX_QUERY_BYTES_BILLED,
                ),
                location=BIGQUERY_LOCATION,
            ).result()
        ).invalid_count

        if invalid_prod_orders > 0:
            raise ValueError(
                f"Referential integrity failure: {invalid_prod_orders} orders have non-existent product_ids"
            )

        print(
            "validate_quality completed successfully: All data quality and referential integrity checks passed."
        )



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

    t_create_tables = BigQueryInsertJobOperator(
        task_id="create_bigquery_tables",
        configuration={
            "query": {
                "query": f"""
                    CREATE TABLE IF NOT EXISTS
                    `{PROJECT_ID}.{DATASET_ID}.dim_customers`
                    (
                        customer_id STRING,
                        name STRING,
                        email STRING,
                        region STRING,
                        signup_date DATE
                    );

                    CREATE TABLE IF NOT EXISTS
                    `{PROJECT_ID}.{DATASET_ID}.dim_products`
                    (
                        product_id STRING,
                        name STRING,
                        category STRING,
                        price FLOAT64
                    );

                    CREATE TABLE IF NOT EXISTS
                    `{PROJECT_ID}.{DATASET_ID}.fct_orders`
                    (
                        order_id STRING,
                        customer_id STRING,
                        product_id STRING,
                        order_ts TIMESTAMP,
                        quantity INT64,
                        order_total FLOAT64,
                        status STRING
                    )
                    PARTITION BY DATE(order_ts)
                    CLUSTER BY customer_id;

                    CREATE TABLE IF NOT EXISTS
                    `{PROJECT_ID}.{DATASET_ID}.fct_events`
                    (
                        event_id STRING,
                        customer_id STRING,
                        event_type STRING,
                        session_id STRING,
                        event_ts TIMESTAMP
                    )
                    PARTITION BY DATE(event_ts)
                    CLUSTER BY customer_id;
                """,
                "useLegacySql": False,
            }
        },
        location=BIGQUERY_LOCATION,
    )

    def load_dimensions(**context):
        client = bigquery.Client(project=PROJECT_ID)

        customers_file = "/opt/airflow/data/customers.csv"
        customers_table = f"{PROJECT_ID}.{DATASET_ID}.dim_customers"

        customers_job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("customer_id", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("region", "STRING"),
                bigquery.SchemaField("signup_date", "DATE"),
            ],
        )

        with open(customers_file, "rb") as file:
            customers_job = client.load_table_from_file(
                file,
                customers_table,
                job_config=customers_job_config,
            )

        customers_job.result()

        print(f"Loaded customers into {customers_table}")

        products_file = "/opt/airflow/data/products.csv"
        products_table = f"{PROJECT_ID}.{DATASET_ID}.dim_products"

        products_job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("product_id", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("category", "STRING"),
                bigquery.SchemaField("price", "FLOAT64"),
            ],
        )

        with open(products_file, "rb") as file:
            products_job = client.load_table_from_file(
                file,
                products_table,
                job_config=products_job_config,
            )

        products_job.result()

        print(f"Loaded products into {products_table}")

        customers = client.get_table(customers_table)
        products = client.get_table(products_table)

        print(f"Customers rows loaded: {customers.num_rows}")
        print(f"Products rows loaded: {products.num_rows}")

        if customers.num_rows != 500:
            raise ValueError(
                f"Expected 500 customers, but found {customers.num_rows}"
            )

        if products.num_rows != 60:
            raise ValueError(
                f"Expected 60 products, but found {products.num_rows}"
            )

        print("Dimension tables loaded successfully.")


    t_load_dimensions = PythonOperator(
        task_id="load_dimensions",
        python_callable=load_dimensions,
    )

    def stage_orders(**context):
        """
        Load the current day's local CSV into a temporary BigQuery
        staging table.
        """

        logical_date = context["logical_date"]
        processing_date = logical_date.strftime("%Y-%m-%d")

        orders_file = (
            f"/opt/airflow/data/orders/"
            f"orders_{processing_date}.csv"
        )

        if not os.path.exists(orders_file):
            raise FileNotFoundError(
                f"Orders file not found: {orders_file}"
            )

        client = bigquery.Client(project=PROJECT_ID)

        staging_table = (
            f"{PROJECT_ID}."
            f"{DATASET_ID}."
            f"_staging_orders_{processing_date.replace('-', '')}"
        )

        load_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("order_id", "STRING"),
                bigquery.SchemaField("customer_id", "STRING"),
                bigquery.SchemaField("product_id", "STRING"),
                bigquery.SchemaField("order_ts", "TIMESTAMP"),
                bigquery.SchemaField("quantity", "INT64"),
                bigquery.SchemaField("order_total", "FLOAT64"),
                bigquery.SchemaField("status", "STRING"),
            ],
        )

        print(f"Staging orders for {processing_date}")
        print(f"Source: {orders_file}")
        print(f"Staging table: {staging_table}")

        with open(orders_file, "rb") as file:
            load_job = client.load_table_from_file(
                file,
                staging_table,
                job_config=load_config,
            )

        load_job.result()

        staging = client.get_table(staging_table)
        staging_count = staging.num_rows

        print(f"Staging rows: {staging_count}")

        if not 240 <= staging_count <= 360:
            raise ValueError(
                f"Order count for {processing_date} is outside "
                f"the allowed range 240-360: {staging_count}"
            )

        print("Orders staging completed successfully.")


    t_stage_orders = PythonOperator(
        task_id="stage_orders",
        python_callable=stage_orders,
    )

    def load_orders_to_bq(**context):
        """
        Replace exactly one daily partition in fct_orders using a query job.

        This avoids DML, keeps retries idempotent, and verifies the actual
        BigQuery partition contents before marking the task successful.
        """

        logical_date = context["logical_date"]
        processing_date = logical_date.strftime("%Y-%m-%d")
        partition_id = logical_date.strftime("%Y%m%d")

        staging_table = (
            f"{PROJECT_ID}."
            f"{DATASET_ID}."
            f"_staging_orders_{partition_id}"
        )

        destination_partition = (
            f"{PROJECT_ID}."
            f"{DATASET_ID}."
            f"fct_orders${partition_id}"
        )

        destination_table = (
            f"{PROJECT_ID}."
            f"{DATASET_ID}."
            f"fct_orders"
        )

        client = bigquery.Client(project=PROJECT_ID)

        source_count_query = f"""
            SELECT COUNT(*) AS row_count
            FROM `{staging_table}`
            WHERE DATE(order_ts) = @processing_date
        """

        source_count_job = client.query(
            source_count_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "processing_date",
                        "DATE",
                        processing_date,
                    ),
                ],
                use_query_cache=False,
                maximum_bytes_billed=MAX_QUERY_BYTES_BILLED,
            ),
            location=BIGQUERY_LOCATION,
        )

        source_count = next(source_count_job.result()).row_count

        print(f"Execution date: {processing_date}")
        print(f"Source staging table: {staging_table}")
        print(f"Expected destination: {destination_table}")
        print(f"Destination partition: fct_orders${partition_id}")
        print("Write disposition: WRITE_TRUNCATE")
        print(f"Source row count: {source_count}")

        if source_count == 0:
            raise ValueError(
                f"No rows found in staging for {processing_date}: "
                f"{staging_table}"
            )

        load_query = f"""
            SELECT
                order_id,
                customer_id,
                product_id,
                order_ts,
                quantity,
                order_total,
                status
            FROM `{staging_table}`
            WHERE DATE(order_ts) = @processing_date
        """

        load_job_config = bigquery.QueryJobConfig(
            destination=destination_partition,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "processing_date",
                    "DATE",
                    processing_date,
                ),
            ],
            use_query_cache=False,
            maximum_bytes_billed=MAX_QUERY_BYTES_BILLED,
        )

        load_job = client.query(
            load_query,
            job_config=load_job_config,
            location=BIGQUERY_LOCATION,
        )
        load_job.result()

        print(f"Load/query job ID: {load_job.job_id}")
        print(f"Job state: {load_job.state}")
        print(f"Job errors: {load_job.errors}")
        print(f"Job destination: {load_job.destination}")
        print(f"Bytes processed: {load_job.total_bytes_processed}")

        verification_query = f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT order_id) AS unique_order_ids,
                COUNTIF(order_ts IS NULL) AS null_order_ts,
                MIN(order_ts) AS min_order_ts,
                MAX(order_ts) AS max_order_ts
            FROM `{destination_table}`
            WHERE DATE(order_ts) = @processing_date
        """

        verification_job = client.query(
            verification_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "processing_date",
                        "DATE",
                        processing_date,
                    ),
                ],
                use_query_cache=False,
                maximum_bytes_billed=MAX_QUERY_BYTES_BILLED,
            ),
            location=BIGQUERY_LOCATION,
        )

        verification = next(verification_job.result())

        partitions_query = f"""
            SELECT
                partition_id,
                total_rows
            FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.PARTITIONS`
            WHERE table_name = 'fct_orders'
              AND partition_id = @partition_id
        """

        partitions_job = client.query(
            partitions_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "partition_id",
                        "STRING",
                        partition_id,
                    ),
                ],
                use_query_cache=False,
                maximum_bytes_billed=MAX_QUERY_BYTES_BILLED,
            ),
            location=BIGQUERY_LOCATION,
        )

        partition_rows = list(partitions_job.result())

        print(f"Verified total rows: {verification.total_rows}")
        print(f"Output rows: {verification.total_rows}")
        print(f"Verified unique order IDs: {verification.unique_order_ids}")
        print(f"Verified NULL order_ts: {verification.null_order_ts}")
        print(f"Verified min order_ts: {verification.min_order_ts}")
        print(f"Verified max order_ts: {verification.max_order_ts}")
        print(f"Partition metadata rows: {partition_rows}")

        if verification.total_rows != source_count:
            raise ValueError(
                f"Partition row count mismatch for {processing_date}. "
                f"Expected {source_count}, found {verification.total_rows}."
            )

        if verification.unique_order_ids != source_count:
            raise ValueError(
                f"Duplicate order_ids detected for {processing_date}. "
                f"Expected {source_count} unique IDs, found "
                f"{verification.unique_order_ids}."
            )

        if verification.null_order_ts != 0:
            raise ValueError(
                f"NULL order_ts values detected in fct_orders partition "
                f"{partition_id}: {verification.null_order_ts}"
            )

        if not partition_rows:
            raise ValueError(
                f"Partition metadata not found for fct_orders${partition_id}"
            )

        if partition_rows[0].total_rows != source_count:
            raise ValueError(
                f"Partition metadata mismatch for fct_orders${partition_id}. "
                f"Expected {source_count}, found "
                f"{partition_rows[0].total_rows}."
            )

        print("Partition load verification completed successfully.")


    t_load_orders_bq = PythonOperator(
        task_id="load_orders_to_bq",
        python_callable=load_orders_to_bq,
    )

    t_cleanup_orders_staging = BigQueryInsertJobOperator(
        task_id="cleanup_orders_staging",
        configuration={
            "query": {
                "query": f"""
                    DROP TABLE IF EXISTS
                    `{PROJECT_ID}.{DATASET_ID}._staging_orders_{{{{ ds_nodash }}}}`
                """,
                "useLegacySql": False,
            }
        },
        location=BIGQUERY_LOCATION,
    )

    t_bq_row_count_check = BigQueryCheckOperator(
        task_id="check_orders_row_count",
        sql=f"""
            SELECT
                COUNT(*) BETWEEN 240 AND 360
            FROM
                `{PROJECT_ID}.{DATASET_ID}.fct_orders`
            WHERE
                DATE(order_ts) = '{{{{ ds }}}}'
        """,
        use_legacy_sql=False,
        location=BIGQUERY_LOCATION,
    )

    def run_agent_monitor(**context):
        logical_date = context["logical_date"]
        processing_date = logical_date.strftime("%Y-%m-%d")

        config_path = "/opt/airflow/config/pipeline_config.yaml"
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        remediation_policy = config.get("agent", {}).get(
            "remediation_policy", {}
        )

        client = bigquery.Client(project=PROJECT_ID)

        print(f"=== AGENT MONITOR RUNNING FOR {processing_date} ===")
        print(f"Active Remediation Policy: {remediation_policy}")

        orders_table = f"{PROJECT_ID}.{DATASET_ID}.fct_orders"
        events_table = f"{PROJECT_ID}.{DATASET_ID}.fct_events"

        orders_check = next(
            client.query(
                f"SELECT COUNT(*) as cnt, COUNT(DISTINCT order_id) as ucnt "
                f"FROM `{orders_table}` WHERE DATE(order_ts) = '{processing_date}'",
                location=BIGQUERY_LOCATION,
            ).result()
        )

        events_check = next(
            client.query(
                f"SELECT COUNT(*) as cnt, COUNT(DISTINCT event_id) as ucnt "
                f"FROM `{events_table}` WHERE DATE(event_ts) = '{processing_date}'",
                location=BIGQUERY_LOCATION,
            ).result()
        )

        anomalies = []

        if orders_check.cnt != orders_check.ucnt:
            anomalies.append(
                (
                    "duplicate_ingestion",
                    "orders duplicates detected",
                    remediation_policy.get("duplicate_ingestion", "escalate"),
                )
            )

        if events_check.cnt != events_check.ucnt:
            anomalies.append(
                (
                    "duplicate_ingestion",
                    "events duplicates detected",
                    remediation_policy.get("duplicate_ingestion", "escalate"),
                )
            )

        if not 240 <= orders_check.cnt <= 360:
            policy = remediation_policy.get(
                "volume_anomaly_spike"
                if orders_check.cnt > 360
                else "volume_anomaly_drop",
                "escalate",
            )
            anomalies.append(
                (
                    "volume_anomaly",
                    f"orders count {orders_check.cnt} out of range",
                    policy,
                )
            )

        if not 900 <= events_check.cnt <= 1500:
            policy = remediation_policy.get(
                "volume_anomaly_spike"
                if events_check.cnt > 1500
                else "volume_anomaly_drop",
                "escalate",
            )
            anomalies.append(
                (
                    "volume_anomaly",
                    f"events count {events_check.cnt} out of range",
                    policy,
                )
            )

        if not anomalies:
            print(
                f"Pipeline Health Summary: HEALTHY (0 anomalies detected for {processing_date})."
            )
            print(
                f"  fct_orders rows: {orders_check.cnt} (100% unique)"
            )
            print(
                f"  fct_events rows: {events_check.cnt} (100% unique)"
            )
            print("  All SLAs and contracts met.")
        else:
            print(f"Anomalies detected: {anomalies}")
            for atype, msg, action in anomalies:
                print(
                    f"Anomaly: {atype} | Message: {msg} | Policy Action: {action}"
                )
                if action == "escalate":
                    raise RuntimeError(f"Pipeline escalated anomaly: {msg}")

        print("Agent monitor check completed.")


    t_agent_monitor = PythonOperator(
        task_id="agent_monitor",
        python_callable=run_agent_monitor,
    )

    t_create_tables >> t_load_dimensions

    t_validate_schema >> [t_ingest_orders, t_ingest_events]

    t_ingest_orders >> t_stage_orders >> t_load_orders_bq >> t_bq_row_count_check

    [t_load_dimensions, t_load_orders_bq, t_ingest_events] >> t_validate_quality

    t_bq_row_count_check >> t_cleanup_orders_staging

    [t_validate_quality, t_cleanup_orders_staging] >> t_agent_monitor
