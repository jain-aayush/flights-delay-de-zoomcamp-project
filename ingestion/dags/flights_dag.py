from datetime import datetime, timedelta
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
}

def csv_to_parquet_converter(filename):
    table = pd.read_csv(filename,encoding = 'latin-1', dtype = {
            'DepTime': 'float', 'ArrTime': 'float', 'CRSElapsedTime': 'float', 
            'AirTime': 'float', 'ArrDelay': 'float', 'DepDelay': 'float',
            'Distance': 'Int64', 'TaxiIn': 'float', 'TaxiOut': 'float',
            'CarrierDelay': 'float', 'WeatherDelay': 'float', 'NASDelay': 'float',
            'SecurityDelay': 'float', 'LateAircraftDelay': 'float','TailNum' : 'object', 
            'CancellationCode' : 'object'
        }
    )
    table.to_parquet(filename.replace('csv.bz2', 'parquet'))

def get_download_url(execution_year_file_label, metadata_directory, ti):

    with open(metadata_directory, 'r') as metadata_file:
        metadata = json.loads(metadata_file.read())
        for file in metadata['datasetVersion']['files']:
            if(file['label']==execution_year_file_label):
                persistent_id = file['dataFile']['persistentId']
                download_url = f'https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId={persistent_id}'
                ti.xcom_push(key=f'download_url', value=download_url)
                break

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME_DIR = os.environ.get('AIRFLOW_HOME')

OUTPUT_FILE_NAME = 'flight_data_{{ execution_date.strftime(\'%Y\') }}'
EXECUTION_YEAR_FILE_LABEL = "{{ execution_date.strftime('%Y') }}.csv.bz2"
METADATA_DIRECTORY = f"{AIRFLOW_HOME_DIR}/metadata.json"

with DAG(
    'flights_dag',
    default_args=default_args,
    description='DAG that helps fetch the flights data and upload to GCP',
    schedule_interval="0 0 5 1 *",
    start_date=datetime(1987,1,1),
    catchup = True, 
    max_active_runs = 1
) as dag:

    get_download_url_task = PythonOperator(
        task_id="get_download_url_task",
        python_callable=get_download_url,
        op_kwargs={
            "execution_year_file_label": EXECUTION_YEAR_FILE_LABEL,
            "metadata_directory" : METADATA_DIRECTORY
        }
    )

    download_data_task = BashOperator(
        task_id='download_data_task',
        bash_command="wget {{{{ ti.xcom_pull(key = 'download_url') }}}} -O {}/{}.csv.bz2".format(AIRFLOW_HOME_DIR, OUTPUT_FILE_NAME)
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=csv_to_parquet_converter,
        op_kwargs={
            "filename": f"{AIRFLOW_HOME_DIR}/{OUTPUT_FILE_NAME}.csv.bz2"
        }
    )

    upload_csv_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_file_csv",
        src=f'{AIRFLOW_HOME_DIR}/{OUTPUT_FILE_NAME}.csv.bz2',
        dst='flights_data/csv/',
        bucket=BUCKET,
    )

    upload_parquet_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_file_parquet",
        src=f'{AIRFLOW_HOME_DIR}/{OUTPUT_FILE_NAME}.parquet',
        dst='flights_data/parquet/',
        bucket=BUCKET
    )

    clean_up_task = BashOperator(
        task_id = 'clean_up_task',
        bash_command=f'rm {AIRFLOW_HOME_DIR}/{OUTPUT_FILE_NAME}.csv.bz2 {AIRFLOW_HOME_DIR}/{OUTPUT_FILE_NAME}.parquet'
    )

    get_download_url_task >> download_data_task >> format_to_parquet_task >> [upload_csv_to_gcs_task, upload_parquet_to_gcs_task] >> clean_up_task

