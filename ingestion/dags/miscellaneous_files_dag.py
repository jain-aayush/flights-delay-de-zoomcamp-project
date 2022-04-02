from datetime import datetime, timedelta
from fileinput import filename
import os
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2008, 12, 31),
    'start_date' : datetime(1987,1,1)
}

def csv_to_parquet_converter(filename):
    table = pd.read_csv(filename)
    table.to_parquet(filename.replace('csv', 'parquet'))

def get_download_url(file_name, metadata_directory, ti):
    with open(metadata_directory, 'r') as metadata_file:
        metadata = json.loads(metadata_file.read())
    for file in metadata['datasetVersion']['files']:
        if(file['label'] == file_name):
            persistent_id = file['dataFile']['persistentId']
            download_url = f'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId={persistent_id}'
            ti.xcom_push(key=f'download_url', value=download_url)
            break

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME_DIR = os.environ.get('AIRFLOW_HOME')

METADATA_DIRECTORY = f"{AIRFLOW_HOME_DIR}/metadata.json"


def download_and_upload_data(
    dag, 
    filename,
    output_file_name
):
    with dag:

        get_download_url_task = PythonOperator(
            task_id="get_download_url_task",
            python_callable=get_download_url,
            op_kwargs={
                "file_name": filename,
                "metadata_directory" : METADATA_DIRECTORY
            }
        )

        download_data_task = BashOperator(
            task_id='download_data_task',
            bash_command=f"wget {{{{ ti.xcom_pull(key = 'download_url') }}}} -O {AIRFLOW_HOME_DIR}/{output_file_name}.csv"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=csv_to_parquet_converter,
            op_kwargs={
                "filename": f"{AIRFLOW_HOME_DIR}/{output_file_name}.csv",
            },
        )

        upload_csv_to_gcs_task = LocalFilesystemToGCSOperator(
            task_id="upload_file_csv",
            src=f'{AIRFLOW_HOME_DIR}/{output_file_name}.csv',
            dst=f'supporting_data/{output_file_name}/csv/',
            bucket=BUCKET,
        )

        upload_parquet_to_gcs_task = LocalFilesystemToGCSOperator(
            task_id="upload_file_parquet",
            src=f'{AIRFLOW_HOME_DIR}/{output_file_name}.parquet',
            dst=f'supporting_data/{output_file_name}/parquet/',
            bucket=BUCKET,
        )

        clean_up_task = BashOperator(
            task_id = 'clean_up_task',
            bash_command=f'rm {AIRFLOW_HOME_DIR}/{output_file_name}.csv {AIRFLOW_HOME_DIR}/{output_file_name}.parquet'
        )

        get_download_url_task >> download_data_task >> format_to_parquet_task >> [upload_csv_to_gcs_task, upload_parquet_to_gcs_task] >> clean_up_task


airport_data_download_dag = DAG(
    dag_id="airport_data_download_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
)

download_and_upload_data(
    airport_data_download_dag, 
    'airports.csv',
    'airports'
)

carrier_data_download_dag = DAG(
    dag_id="carrier_data_download_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
)
download_and_upload_data(
    carrier_data_download_dag, 
    'carriers.csv',
    'carriers'
)

plane_data_download_dag = DAG(
    dag_id="plane_data_download_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
)
download_and_upload_data(
    plane_data_download_dag, 
    'plane-data.csv',
    'plane-data'
)
