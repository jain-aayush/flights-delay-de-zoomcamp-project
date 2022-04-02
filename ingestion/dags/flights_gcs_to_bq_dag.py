from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME_DIR = os.environ.get('AIRFLOW_HOME')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'airline_data_temp1')

def gcs_to_bq_transfer(
    dag,
    projectId,
    bucket_name,
    gcs_source_path,
    dataset_name,
    table_name,
    source_format,
    cluster_col
):
    with dag:

        gcs_to_bigquery_task = BigQueryCreateExternalTableOperator(
            task_id="gcs_to_bigquery_task",
            table_resource={
                "tableReference": {
                    "projectId": projectId,
                    "datasetId": dataset_name,
                    "tableId": f"{table_name}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": f"{source_format.upper()}",
                    "sourceUris": [f"gs://{bucket_name}/{gcs_source_path}/*"],
                },
            },
        )

        CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {dataset_name}.{table_name} \
            CLUSTER BY {cluster_col} \
            AS \
            SELECT * FROM {dataset_name}.{table_name}_external_table;"
        )

        bq_create_partitioned_table_task = BigQueryInsertJobOperator(
            task_id=f"bq_create_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        gcs_to_bigquery_task >> bq_create_partitioned_table_task


def gcs_to_bq_transfer_unpartitioned(
    dag,
    projectId,
    bucket_name,
    gcs_source_path,
    dataset_name,
    table_name,
    source_format,
):
    with dag:

        gcs_to_bigquery_task = BigQueryCreateExternalTableOperator(
            task_id="gcs_to_bigquery_task",
            table_resource={
                "tableReference": {
                    "projectId": projectId,
                    "datasetId": dataset_name,
                    "tableId": f"{table_name}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": f"{source_format.upper()}",
                    "sourceUris": [f"gs://{bucket_name}/{gcs_source_path}/*"],
                },
            },
        )

        CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {dataset_name}.{table_name} \
            AS \
            SELECT * FROM {dataset_name}.{table_name}_external_table;"
        )

        bq_create_unpartitioned_table_task = BigQueryInsertJobOperator(
            task_id=f"bq_create_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        gcs_to_bigquery_task >> bq_create_unpartitioned_table_task



bq_flights_dag = DAG(
    dag_id="flights_bq_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
)

gcs_to_bq_transfer(
    bq_flights_dag,
    PROJECT_ID,
    BUCKET,
    'flights_data/parquet',
    BIGQUERY_DATASET,
    'flights',
    'PARQUET',
    'Year'
)

airports_dag = DAG(
    dag_id="airports_bq_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
)

gcs_to_bq_transfer_unpartitioned(
    airports_dag,
    PROJECT_ID,
    BUCKET,
    'supporting_data/airports/parquet',
    BIGQUERY_DATASET,
    'airports',
    'PARQUET'
)

carriers_dag = DAG(
    dag_id="carriers_bq_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
)

gcs_to_bq_transfer_unpartitioned(
    carriers_dag,
    PROJECT_ID,
    BUCKET,
    'supporting_data/carriers/parquet',
    BIGQUERY_DATASET,
    'carriers',
    'PARQUET'
)

plane_data_dag = DAG(
    dag_id="plane_data_bq_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
)

gcs_to_bq_transfer_unpartitioned(
    plane_data_dag,
    PROJECT_ID,
    BUCKET,
    'supporting_data/plane-data/parquet',
    BIGQUERY_DATASET,
    'plane_data',
    'PARQUET',
)
