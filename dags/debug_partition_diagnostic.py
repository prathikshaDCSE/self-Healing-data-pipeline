import json
import sys
from datetime import datetime
from importlib import metadata

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery


PROJECT = "self-healing-data-pipeline-123"
DATASET = "pipeline_intern_prathiksha"
LOCATION = "US"
PROCESSING_DATE = "2026-06-14"
PARTITION_ID = "20260614"
STAGING = f"{PROJECT}.{DATASET}._staging_orders_{PARTITION_ID}"
TARGET = f"{PROJECT}.{DATASET}.fct_orders"
TARGET_PARTITION = f"{TARGET}${PARTITION_ID}"
CONTROL_TABLE = f"{PROJECT}.{DATASET}._test_orders_load"
DEBUG_TABLE = f"{PROJECT}.{DATASET}.debug_partitioned_orders"
DEBUG_PARTITION = f"{DEBUG_TABLE}${PARTITION_ID}"
DEBUG_COPY_TABLE = f"{PROJECT}.{DATASET}.debug_copy_partitioned_orders"
DEBUG_COPY_PARTITION = f"{DEBUG_COPY_TABLE}${PARTITION_ID}"
DEBUG_LOAD_TABLE = f"{PROJECT}.{DATASET}.debug_load_partitioned_orders"
DEBUG_LOAD_PARTITION = f"{DEBUG_LOAD_TABLE}${PARTITION_ID}"
ORDERS_FILE = "/opt/airflow/data/orders/orders_2026-06-14.csv"
MAX_BYTES = "100000000"


def section(title):
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def print_json(label, value):
    print(label)
    print(json.dumps(value, indent=2, default=str))


def row_to_dict(row):
    return {key: row[key] for key in row.keys()}


def safe_run_query(client, sql, job_config=None, label=None):
    if label:
        print(f"Running query: {label}")
    job = client.query(sql, job_config=job_config, location=LOCATION)
    rows = list(job.result())
    return job, rows


def inspect_table(client, table_name, label):
    table = client.get_table(table_name)
    section(label)
    print(f"full_table_id: {table.full_table_id}")
    print(f"table_id: {table.table_id}")
    print(f"dataset_id: {table.dataset_id}")
    print(f"project: {table.project}")
    print(f"location: {table.location}")
    print(f"num_rows: {table.num_rows}")
    print(f"table_type: {table.table_type}")
    print(f"created: {table.created}")
    print(f"modified: {table.modified}")
    print(f"clustering_fields: {table.clustering_fields}")
    print(f"partition_field: {getattr(table.time_partitioning, 'field', None)}")
    print(f"partition_type: {getattr(table.time_partitioning, 'type_', None)}")
    print(
        "partition_expiration_ms: "
        f"{getattr(table.time_partitioning, 'expiration_ms', None)}"
    )
    print(f"require_partition_filter: {table.require_partition_filter}")
    print("schema:")
    for field in table.schema:
        print(f"  {field.name} {field.field_type} {field.mode}")
    return table


def print_partitions(client, table_name, label):
    section(label)
    sql = f"""
    SELECT
        table_name,
        partition_id,
        total_rows
    FROM `{PROJECT}.{DATASET}.INFORMATION_SCHEMA.PARTITIONS`
    WHERE table_name = @table_name
    ORDER BY partition_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
        ],
        use_query_cache=False,
        maximum_bytes_billed=int(MAX_BYTES),
    )
    _, rows = safe_run_query(client, sql, job_config=job_config)
    print(f"number_of_partitions: {len(rows)}")
    if not rows:
        print("NO PARTITIONS FOUND")
    for row in rows:
        print(
            f"table_name={row.table_name} "
            f"partition_id={row.partition_id} total_rows={row.total_rows}"
        )
    return rows


def print_recent_tables(client):
    section("RECENT DATASET TABLES")
    sql = f"""
    SELECT
        table_name,
        table_type,
        creation_time
    FROM `{PROJECT}.{DATASET}.INFORMATION_SCHEMA.TABLES`
    ORDER BY creation_time DESC
    LIMIT 25
    """
    _, rows = safe_run_query(client, sql)
    for row in rows:
        print(
            f"table_name={row.table_name} "
            f"table_type={row.table_type} creation_time={row.creation_time}"
        )


def create_partitioned_table(client, table_name, label):
    section(label)
    sql = f"""
    CREATE OR REPLACE TABLE `{table_name}`
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
    CLUSTER BY customer_id
    """
    safe_run_query(client, sql, label=f"create {table_name}")
    return inspect_table(client, table_name, f"{table_name} METADATA")


def fetch_raw_job(session, job_id):
    job_url = (
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/jobs/"
        f"{job_id}?location={LOCATION}"
    )
    while True:
        poll_response = session.get(job_url)
        poll_response.raise_for_status()
        raw_job = poll_response.json()
        state = raw_job.get("status", {}).get("state")
        print(f"job_state: {state}")
        if state == "DONE":
            return raw_job


def summarize_job(client, session, job_id, label):
    section(label)
    raw_job = fetch_raw_job(session, job_id)
    print_json("final_raw_job_resource:", raw_job)
    status = raw_job.get("status", {})
    stats = raw_job.get("statistics", {})
    query_stats = stats.get("query", {})
    copy_stats = stats.get("copy", {})
    config = raw_job.get("configuration", {})
    client_job = client.get_job(job_id, location=LOCATION)
    print(f"JOB ID: {job_id}")
    print(f"JOB TYPE: {config.get('jobType')}")
    print(f"STATE: {status.get('state')}")
    print(f"ERROR RESULT: {status.get('errorResult')}")
    print(f"ERRORS: {status.get('errors')}")
    print(f"OUTPUT ROWS: {getattr(client_job, 'output_rows', None)}")
    print(f"TOTAL BYTES PROCESSED: {query_stats.get('totalBytesProcessed')}")
    print(f"TOTAL SLOT MS: {query_stats.get('totalSlotMs') or stats.get('totalSlotMs')}")
    print(f"CREATION TIME: {stats.get('creationTime')}")
    print(f"START TIME: {stats.get('startTime')}")
    print(f"END TIME: {stats.get('endTime')}")
    print(f"job.destination: {client_job.destination}")
    if config.get("copy"):
        print_json("configuration.copy:", config.get("copy"))
        print_json("statistics.copy:", copy_stats)
    if config.get("query"):
        print_json("configuration.query.destinationTable:", config.get("query", {}).get("destinationTable"))
        print_json("statistics.query.destinationTable:", query_stats.get("destinationTable"))
    return raw_job, client_job


def verify_partition_rows(client, table_name, label):
    section(label)
    inspect_table(client, table_name, f"{table_name} TABLE STATE")
    partitions = print_partitions(client, table_name.split(".")[-1], f"{table_name} PARTITIONS")
    count_sql = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT order_id) AS unique_ids,
        COUNTIF(order_ts IS NULL) AS null_order_ts,
        MIN(order_ts) AS min_order_ts,
        MAX(order_ts) AS max_order_ts
    FROM `{table_name}`
    WHERE DATE(order_ts) = DATE('{PROCESSING_DATE}')
    """
    _, rows = safe_run_query(client, count_sql)
    row = row_to_dict(rows[0])
    print_json("june14_verification:", row)
    return partitions, row


def source_query_sql(table_name):
    return f"""
    SELECT
        order_id,
        customer_id,
        product_id,
        order_ts,
        quantity,
        order_total,
        status
    FROM `{table_name}`
    WHERE DATE(order_ts) = DATE('{PROCESSING_DATE}')
    """


def main():
    section("PHASE 1 - ENVIRONMENT AND AUTHENTICATION")
    print(f"python_version: {sys.version}")
    print(f"google_cloud_bigquery_version: {metadata.version('google-cloud-bigquery')}")

    credentials, authenticated_project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds_type = type(credentials).__name__
    print(f"authenticated_project: {authenticated_project}")
    print(f"configured_project: {PROJECT}")
    print(f"target_project: {PROJECT}")
    print(f"target_dataset: {DATASET}")
    print(f"current_bigquery_location: {LOCATION}")
    print(f"credentials_type: {creds_type}")

    client = bigquery.Client(project=PROJECT, credentials=credentials, location=LOCATION)
    session = AuthorizedSession(credentials)

    print("simple_select_1:")
    _, select_rows = safe_run_query(client, "SELECT 1 AS ok")
    print(row_to_dict(select_rows[0]))

    print("information_schema_check:")
    _, info_rows = safe_run_query(
        client,
        f"""
        SELECT COUNT(*) AS table_count
        FROM `{PROJECT}.{DATASET}.INFORMATION_SCHEMA.TABLES`
        """,
    )
    print(row_to_dict(info_rows[0]))

    staging_table = inspect_table(client, STAGING, "PHASE 2 - STAGING TABLE METADATA")
    target_table = inspect_table(client, TARGET, "PHASE 3 - TARGET TABLE BEFORE LOAD")

    section("PHASE 2 - STAGING TABLE STATS")
    staging_stats_sql = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNTIF(order_ts IS NULL) AS null_order_ts,
        COUNTIF(DATE(order_ts) = DATE('{PROCESSING_DATE}')) AS june14_rows,
        MIN(order_ts) AS min_order_ts,
        MAX(order_ts) AS max_order_ts,
        COUNT(DISTINCT order_id) AS unique_order_ids
    FROM `{STAGING}`
    """
    _, staging_stats = safe_run_query(client, staging_stats_sql)
    print(row_to_dict(staging_stats[0]))

    print_partitions(client, "fct_orders", "PHASE 3 - TARGET PARTITIONS BEFORE LOAD")

    section("PHASE 4 - SOURCE QUERY INDEPENDENTLY")
    source_sql = source_query_sql(STAGING)
    _, source_rows = safe_run_query(client, source_sql)
    print(f"source_row_count: {len(source_rows)}")
    print("first_3_rows:")
    for row in source_rows[:3]:
        print_json("-", row_to_dict(row))
    print("last_3_rows:")
    for row in source_rows[-3:]:
        print_json("-", row_to_dict(row))

    section("PHASE 5 - CRITICAL PARTITION DECORATOR TEST")
    print(f"repr(TARGET_PARTITION): {repr(TARGET_PARTITION)}")
    print(f"TARGET_PARTITION.endswith('$20260614'): {TARGET_PARTITION.endswith('$20260614')}")
    print("RAW DESTINATION TABLE ID:")
    print(f"project: {PROJECT}")
    print(f"dataset: {DATASET}")
    print(f"tableId: fct_orders${PARTITION_ID}")

    section("PHASE 6 - RAW BIGQUERY JOB REQUEST")
    job_body = {
        "configuration": {
            "query": {
                "query": source_sql,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT,
                    "datasetId": DATASET,
                    "tableId": f"fct_orders${PARTITION_ID}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_NEVER",
                "maximumBytesBilled": MAX_BYTES,
            }
        }
    }
    print_json("raw_job_request:", job_body)
    raw_table_id = job_body["configuration"]["query"]["destinationTable"]["tableId"]
    if raw_table_id != f"fct_orders${PARTITION_ID}":
        raise RuntimeError(f"Decorator lost before submit: {raw_table_id}")

    jobs_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/jobs"
    response = session.post(jobs_url, json=job_body)
    print(f"http_status: {response.status_code}")
    if response.status_code >= 400:
        print(response.text)
        response.raise_for_status()

    initial_job = response.json()
    print_json("initial_job_resource:", initial_job)
    initial_table_id = (
        initial_job.get("configuration", {})
        .get("query", {})
        .get("destinationTable", {})
        .get("tableId")
    )
    print(f"initial_destination_table_id: {initial_table_id}")
    job_id = initial_job["jobReference"]["jobId"]
    print(f"job_id: {job_id}")

    job_url = (
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/jobs/"
        f"{job_id}?location={LOCATION}"
    )

    while True:
        poll_response = session.get(job_url)
        poll_response.raise_for_status()
        raw_job = poll_response.json()
        state = raw_job.get("status", {}).get("state")
        print(f"job_state: {state}")
        if state == "DONE":
            break

    section("PHASE 7 - INSPECT JOB RESULT")
    print_json("final_raw_job_resource:", raw_job)
    status = raw_job.get("status", {})
    stats = raw_job.get("statistics", {})
    query_stats = stats.get("query", {})
    config_query = raw_job.get("configuration", {}).get("query", {})
    print(f"JOB ID: {job_id}")
    print("JOB TYPE: QUERY")
    print(f"STATE: {status.get('state')}")
    print(f"ERROR RESULT: {status.get('errorResult')}")
    print(f"ERRORS: {status.get('errors')}")
    print(f"OUTPUT ROWS: {query_stats.get('numDmlAffectedRows')}")
    print(f"TOTAL BYTES PROCESSED: {query_stats.get('totalBytesProcessed')}")
    print(f"TOTAL SLOT MS: {query_stats.get('totalSlotMs')}")
    print(f"CREATION TIME: {stats.get('creationTime')}")
    print(f"START TIME: {stats.get('startTime')}")
    print(f"END TIME: {stats.get('endTime')}")
    print_json(
        "configuration.query.destinationTable:",
        config_query.get("destinationTable"),
    )
    print_json(
        "statistics.query.destinationTable:",
        query_stats.get("destinationTable"),
    )

    client_job = client.get_job(job_id, location=LOCATION)
    print(f"job.destination: {client_job.destination}")
    print(f"job.state: {client_job.state}")
    print(f"job.errors: {client_job.errors}")
    print(f"job.error_result: {client_job.error_result}")
    print(f"job.output_rows: {getattr(client_job, 'output_rows', None)}")

    inspect_table(client, TARGET, "PHASE 8 - TARGET TABLE AFTER LOAD")

    section("PHASE 8 - TARGET TABLE COUNTS AFTER LOAD")
    overall_sql = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT order_id) AS unique_ids,
        COUNTIF(order_ts IS NULL) AS null_order_ts,
        MIN(order_ts) AS min_order_ts,
        MAX(order_ts) AS max_order_ts
    FROM `{TARGET}`
    """
    _, overall_rows = safe_run_query(client, overall_sql)
    print(row_to_dict(overall_rows[0]))

    june14_sql = f"""
    SELECT COUNT(*) AS june14_rows
    FROM `{TARGET}`
    WHERE DATE(order_ts) = DATE('{PROCESSING_DATE}')
    """
    _, june14_rows = safe_run_query(client, june14_sql)
    print(f"JUNE 14 ROWS: {june14_rows[0].june14_rows}")

    print_partitions(client, "fct_orders", "PHASE 9 - PARTITIONS AFTER LOAD")

    section("PHASE 10 - DIRECT PARTITION VERIFICATION")
    try:
        direct_partition_sql = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT order_id) AS unique_ids,
            MIN(order_ts) AS min_order_ts,
            MAX(order_ts) AS max_order_ts
        FROM `{TARGET_PARTITION}`
        """
        _, direct_rows = safe_run_query(client, direct_partition_sql)
        print(row_to_dict(direct_rows[0]))
    except Exception as exc:
        print(f"direct_partition_query_failed: {exc}")

    section("PHASE 11 - CHECK WHETHER DATA LANDED SOMEWHERE ELSE")
    print_recent_tables(client)

    section("PHASE 12 - NON-PARTITIONED CONTROL")
    control_table = inspect_table(client, CONTROL_TABLE, "CONTROL TABLE METADATA")
    control_sql = f"SELECT COUNT(*) AS total_rows FROM `{CONTROL_TABLE}`"
    _, control_rows = safe_run_query(client, control_sql)
    print(row_to_dict(control_rows[0]))

    section("PHASE 13 - QUERY DESTINATION CONTROL TABLE")
    create_partitioned_table(client, DEBUG_TABLE, "CREATE QUERY CONTROL TABLE")

    debug_job_body = {
        "configuration": {
            "query": {
                "query": source_sql,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT,
                    "datasetId": DATASET,
                    "tableId": f"debug_partitioned_orders${PARTITION_ID}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_NEVER",
                "maximumBytesBilled": MAX_BYTES,
            }
        }
    }
    print_json("debug_control_job_request:", debug_job_body)
    debug_response = session.post(jobs_url, json=debug_job_body)
    print(f"debug_control_http_status: {debug_response.status_code}")
    debug_response.raise_for_status()
    debug_initial = debug_response.json()
    print_json("debug_control_initial_job:", debug_initial)
    debug_job_id = debug_initial["jobReference"]["jobId"]
    summarize_job(client, session, debug_job_id, "QUERY DESTINATION CONTROL JOB RESULT")
    debug_table = inspect_table(client, DEBUG_TABLE, "DEBUG PARTITIONED TABLE AFTER QUERY DESTINATION")
    print_partitions(client, "debug_partitioned_orders", "DEBUG PARTITIONED TABLE PARTITIONS")
    debug_count_sql = f"""
    SELECT COUNT(*) AS june14_rows
    FROM `{DEBUG_TABLE}`
    WHERE DATE(order_ts) = DATE('{PROCESSING_DATE}')
    """
    _, debug_count_rows = safe_run_query(client, debug_count_sql)
    print(f"debug_partitioned_orders june14_rows: {debug_count_rows[0].june14_rows}")
    try:
        debug_direct_sql = f"SELECT COUNT(*) AS total_rows FROM `{DEBUG_PARTITION}`"
        _, debug_direct_rows = safe_run_query(client, debug_direct_sql)
        print(row_to_dict(debug_direct_rows[0]))
    except Exception as exc:
        print(f"debug_direct_partition_query_failed: {exc}")

    section("PHASE 4 - TEST PARTITIONED COPY")
    create_partitioned_table(client, DEBUG_COPY_TABLE, "CREATE COPY CONTROL TABLE")
    copy_job_body = {
        "configuration": {
            "copy": {
                "sourceTables": [
                    {
                        "projectId": PROJECT,
                        "datasetId": DATASET,
                        "tableId": "_test_orders_load",
                    }
                ],
                "destinationTable": {
                    "projectId": PROJECT,
                    "datasetId": DATASET,
                    "tableId": f"debug_copy_partitioned_orders${PARTITION_ID}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_NEVER",
            }
        }
    }
    print_json("copy_job_request:", copy_job_body)
    copy_response = session.post(jobs_url, json=copy_job_body)
    print(f"copy_http_status: {copy_response.status_code}")
    copy_response.raise_for_status()
    copy_initial = copy_response.json()
    print_json("copy_initial_job:", copy_initial)
    copy_job_id = copy_initial["jobReference"]["jobId"]
    summarize_job(client, session, copy_job_id, "COPY JOB RESULT")
    copy_partitions, copy_verification = verify_partition_rows(
        client,
        DEBUG_COPY_TABLE,
        "COPY TABLE VERIFICATION",
    )

    section("PHASE 5 - TEST PARTITIONED LOAD")
    create_partitioned_table(client, DEBUG_LOAD_TABLE, "CREATE LOAD CONTROL TABLE")
    with open(ORDERS_FILE, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file,
            DEBUG_LOAD_PARTITION,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
                schema=[
                    bigquery.SchemaField("order_id", "STRING"),
                    bigquery.SchemaField("customer_id", "STRING"),
                    bigquery.SchemaField("product_id", "STRING"),
                    bigquery.SchemaField("order_ts", "TIMESTAMP"),
                    bigquery.SchemaField("quantity", "INT64"),
                    bigquery.SchemaField("order_total", "FLOAT64"),
                    bigquery.SchemaField("status", "STRING"),
                ],
            ),
            location=LOCATION,
        )
    print(f"load_job_id: {load_job.job_id}")
    load_job.result()
    summarize_job(client, session, load_job.job_id, "LOAD JOB RESULT")
    load_partitions, load_verification = verify_partition_rows(
        client,
        DEBUG_LOAD_TABLE,
        "LOAD TABLE VERIFICATION",
    )

    section("PHASE 15 - TABLE COMPARISON")
    print("Comparing target and debug partitioned table metadata:")
    print(f"target_partition_field: {getattr(target_table.time_partitioning, 'field', None)}")
    print(f"target_partition_type: {getattr(target_table.time_partitioning, 'type_', None)}")
    print(f"target_clustering: {target_table.clustering_fields}")
    print(f"debug_partition_field: {getattr(debug_table.time_partitioning, 'field', None)}")
    print(f"debug_partition_type: {getattr(debug_table.time_partitioning, 'type_', None)}")
    print(f"debug_clustering: {debug_table.clustering_fields}")
    print(f"copy_partitions_found: {len(copy_partitions)}")
    print_json("copy_verification:", copy_verification)
    print(f"load_partitions_found: {len(load_partitions)}")
    print_json("load_verification:", load_verification)


if __name__ == "__main__":
    print(f"diagnostic_started_at: {datetime.utcnow().isoformat()}Z")
    main()
