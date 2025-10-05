import os
import logging
import requests
import json
import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.json as pj
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')

path_to_local_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'trips_data_all')

def get_data():
    kanye_url = "https://api.kanye.rest"
    cat_fact_url = "https://meowfacts.herokuapp.com/"
    cat_img_url = "https://cataas.com/cat/small?json=true"
    kanyeOTD = requests.get(kanye_url).json()['quote']
    catFactOTD = requests.get(cat_fact_url).json()['data'][0]
    catImgOTD = requests.get(cat_img_url).json()['url']

    today = datetime.datetime.today().strftime('%d-%m-%Y')
    content = {
        "date": today,
        "kanye": kanyeOTD,
        "catFact": catFactOTD,
        "catImg": catImgOTD}
    return {'content': content, 'filename' : f'data_{today}.json'}

def format_to_parquet(ti, **kwargs):
    data = ti.xcom_pull(task_ids='get_data_task')
    content = data['content']
    filename = data['filename']
    src_file = f"/tmp/{filename}"
    with open(src_file, "w") as f:
        json.dump(content, f)
    table = pj.read_json(src_file)
    pq.write_table(table, src_file.replace('.json', '.parquet'))

def upload_to_gcs(bucket, ti, **kwargs):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    data = ti.xcom_pull(task_ids='get_data_task')
    filename = data['filename']
    local_file = f"/tmp/{filename.replace('.json', '.parquet')}"
    object_name = f"raw/{filename.replace('.json', '.parquet')}"
    client = storage.Client()
    bucket_obj = client.bucket(bucket)

    blob = bucket_obj.blob(object_name)
    blob.upload_from_filename(local_file)
    ti.xcom_push(key="gcs_uri", value=f'gs://{bucket}/{object_name}')

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    get_data_task = PythonOperator(
        task_id="get_data_task",
        python_callable=get_data
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": ["{{ ti.xcom_pull(key='gcs_uri', task_ids='local_to_gcs_task') }}"],
            },
        },
    )

    get_data_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task