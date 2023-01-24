from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import pendulum
from datetime import timedelta
from music_chart.common.utils import get_uri, cleanup_xcom
from music_chart.dags.tasks import soundcloud
from music_chart.dags.tasks import spotify
import os


# Connection
MONGO_CONN_ID = 'mongo_conn'
SPARK_CONN_ID = 'spark_conn'

SPARK_JOBS_DIR = f'{os.path.abspath(os.path.dirname(__file__))}/../spark_job'



default_args = {
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}


with DAG(
    dag_id='music_chart',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 9, 12, tz='Asia/Ho_Chi_Minh'),
    max_active_runs=5,
    catchup=False,
    default_args=default_args,
    on_success_callback=cleanup_xcom
):
    # clean_lake_old_data = PythonOperator(
    #     task_id='clean_lake_old_data',
    #     python_callable=
    # )

    with TaskGroup('soundcloud') as soundcloud_group:
        fetch_creds = PythonOperator(
            task_id='get_client_id',
            python_callable=soundcloud.get_client_id,
        )

        fetch_top_tracks = PythonOperator(
            task_id='fetch_top_tracks',
            python_callable=soundcloud.fetch_top_tracks,
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
                'schema': 'music_chart',
            }
        )

        fetch_artists = PythonOperator(
            task_id='fetch_artists',
            python_callable=lambda: None
        )

        fetch_creds >> [fetch_top_tracks, fetch_artists]

    with TaskGroup('spotify') as spotify_group:
        fetch_creds = PythonOperator(
            task_id='get_access_token',
            python_callable=spotify.get_access_token, 
            op_kwargs={
                'client_payload': Variable.get('spotify_cred', deserialize_json=True)
            }
        )

        fetch_top_tracks = PythonOperator(
            task_id='fetch_top_tracks',
            python_callable=spotify.fetch_top_tracks,
            op_kwargs={
                'url': 'https://api.spotify.com/v1/playlists/37i9dQZEVXbNG2KDcFcKOF/tracks',
                'access_token': '{{ ti.xcom_pull(task_ids="spotify_getAccessToken") }}',
                'url_params':{
                    'limit': 50
                },
                'mongo_conn_id': MONGO_CONN_ID,
                'collection': 'spotify',
                'schema': 'music_chart',
                'access_token': 'ti.xcom_pull(task_ids="get_access_token")'
            }
        )

        fetch_artists = PythonOperator(
            task_id='fetch_artists',
            python_callable=lambda: None
        )
    
        fetch_creds >> [fetch_top_tracks, fetch_artists]

    transform = SparkSubmitOperator(
        task_id='transform_data',
        conn_id=SPARK_CONN_ID,
        application=SPARK_JOBS_DIR + '/transform.py',
        packages=(
            'org.mongodb.spark:mongo-spark-connector:10.0.5,'
            'org.mongodb:mongodb-driver-sync:4.8.1'
        ),
        application_args=[
            '--uri', get_uri(MONGO_CONN_ID, conn_type='mongo'),
            '--runtime', '{{ ts }}'
        ],
        retries=0
    )

    transform.set_upstream([spotify_group, soundcloud_group])