from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
from datetime import timedelta

from src.tasks import soundcloud
from src.tasks import spotify

import os

# Connection
MONGO_CONN_ID = 'mongo_conn'


CUR_DIR = os.path.abspath(os.path.dirname(__file__))

default_args = {
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}



with DAG(
    dag_id='music_chart',
    schedule_interval='@once',
    start_date=pendulum.datetime(2022, 9, 12, tz='Asia/Ho_Chi_Minh'),
    max_active_runs=5,
    catchup=False,
    default_args=default_args
):

    sc_get_client_id = PythonOperator(
        task_id='soundcloud_GetClientId',
        python_callable=soundcloud.get_client_id
    )

    sc_fetch_data = PythonOperator(
        task_id='soundcloud_FetchMongo',
        python_callable=soundcloud.fetch_to_mongo,
        op_kwargs={
            'url': 'https://api-v2.soundcloud.com/charts',
            'url_params': {
                'offset': 20,
                'genre': 'soundcloud%3Agenres%3Aall-music',
                'kind': 'top',
                'limit': 100,
                'client_id': '{{ ti.xcom_pull(task_ids="soundcloud_GetClientId") }}'
            },
            'mongo_conn_id': MONGO_CONN_ID
        }
    )

    sp_get_access_token = PythonOperator(
        task_id='spotify_getAccessToken',
        python_callable=spotify.get_access_token,
        op_kwargs={
            'client_id': '{{ var.json.spotify_cred.client_id }}',
            'client_secret': '{{ var.json.spotify_cred.client_secret }}'
        }
    )

    sp_fetch_data = PythonOperator(
        task_id='spotify_FetchMongo',
        python_callable=spotify.fetch_to_mongo,
        op_kwargs={
            'url': 'https://api.spotify.com/v1/browse/categories',
            'access_token': '{{ ti.xcom_pull(task_ids="spotify_getAccessToken") }}',
            'url_params':{
                'limit': 50
            },
            'mongo_conn_id': MONGO_CONN_ID
        }
    )

    sc_get_client_id >> sc_fetch_data
    sp_get_access_token >> sp_fetch_data