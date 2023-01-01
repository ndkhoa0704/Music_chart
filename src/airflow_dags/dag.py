from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
import pendulum
from datetime import timedelta

from airflow_dags.tasks import soundcloud
from airflow_dags.tasks import spotify

import os

# Connection
MONGO_CONN_ID = 'mongo_conn'
SPARK_CONN_ID = 'spark_conn'



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

    sc_fetch_data = PythonOperator(
        task_id='soundcloud_FetchMongo',
        python_callable=soundcloud.fetch_to_mongo,
        op_kwargs={
            'url': 'https://api-v2.soundcloud.com/charts',
            'url_params': {
                'offset': 20,
                'genre': 'soundcloud%3Agenres%3Aall-music',
                'kind': 'top',
                'limit': 100
            },
            'mongo_conn_id': MONGO_CONN_ID,
            'collection': 'soundcloud',
            'schema': 'music_chart'
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
            'mongo_conn_id': MONGO_CONN_ID,
            'collection': 'spotify',
            'schema': 'music_chart',
            'client_payload': Variable.get('spotify_cred', deserialize_json=True)
        }
    )

    transform = SparkSubmitOperator(
        task_id='transform_data',
        conn_id=SPARK_CONN_ID,
        conf={
            'spark.mongodb.input.uri': (
                'mongodb://{{{{ conn.{T}.login  }}}}@'
                '{{{{ conn.{T}.host  }}}}:'
                '{{{{ conn.{T}.port  }}}}/'
                '{{{{ conn.{T}.schema }}}}'
            ).format(T=MONGO_CONN_ID)
        },
        application=f'{CUR_DIR}/../spark_job/transform.py',
        application_args=[]
    )

    transform.set_upstream([sc_fetch_data, sp_fetch_data])