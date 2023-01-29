from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
MYSQL_CONN_ID = 'mysql_conn'

# Dir
SPARK_JOBS_DIR = f'{os.path.abspath(os.path.dirname(__file__))}/../spark_job/'

# Database
MONGO_DB_NAME = 'music_chart'


default_args = {
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}


with DAG(
    dag_id=MONGO_DB_NAME,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 9, 12, tz='Asia/Ho_Chi_Minh'),
    max_active_runs=5,
    catchup=False,
    default_args=default_args
):
    clear_warehouse_task = MySqlOperator(
        task_id='clean_warehouse',
        doc_md='''
        Clean old data daily by timestamp
        ''',
        mysql_conn_id=MYSQL_CONN_ID,
        sql='''
        DELETE FROM music_chart.tracks WHERE data_time = '{{ ts }}'
        '''
    )

    with TaskGroup('soundcloud') as soundcloud_group:
        fetch_top_tracks_task = PythonOperator(
            task_id='fetch_top_tracks',
            python_callable=soundcloud.fetch_top_tracks,
            op_kwargs={
                'mongo_conn_id': MONGO_CONN_ID,
                'collname': 'soundcloud_top_tracks',
                'dbname': MONGO_DB_NAME
            }
        )

        fetch_artists_task = PythonOperator(
            task_id='fetch_artists',
            python_callable=soundcloud.fetch_artists,
            op_kwargs={
                'mongo_conn_id': MONGO_CONN_ID,
                'dbname': MONGO_DB_NAME,
                'tracks_collname': 'soundcloud_top_tracks',
                'artists_collname': 'soundcloud_artists'
            }
        )

        fetch_top_tracks_task >> fetch_artists_task

    with TaskGroup('spotify') as spotify_group:
        fetch_creds_task = PythonOperator(
            task_id='get_access_token',
            python_callable=spotify.get_access_token,
            op_kwargs={
                'client_payload': '{{ conn.spotify_creds.password }}'
            }
        )

        fetch_top_tracks_task = PythonOperator(
            task_id='fetch_top_tracks',
            python_callable=spotify.fetch_top_tracks,
            op_kwargs={
                'mongo_conn_id': MONGO_CONN_ID,
                'collname': 'spotify_top_tracks',
                'dbname': MONGO_DB_NAME
            }
        )

        fetch_artists_task = PythonOperator(
            task_id='fetch_artists',
            python_callable=spotify.fetch_artists,
            op_kwargs={
                'mongo_conn_id': MONGO_CONN_ID,
                'dbname': MONGO_DB_NAME,
                'tracks_collname': 'spotify_top_tracks',
                'artists_collname': 'spotify_artists'
            }
        )
    
        fetch_creds_task >> fetch_top_tracks_task >> fetch_artists_task
    
    with TaskGroup('cleanup') as cleanup_group:
        clean_xcom_task = PythonOperator(
            task_id='clean_xcom',
            python_callable=cleanup_xcom
        )
        
        clean_xcom_task

    transform_tracks_task = SparkSubmitOperator(
        task_id='transform_tracks',
        conn_id=SPARK_CONN_ID,
        application=SPARK_JOBS_DIR + 'transform_tracks.py',
        packages=(
            'org.mongodb.spark:mongo-spark-connector:10.0.5,'
            'org.mongodb:mongodb-driver-sync:4.8.1,'
            'mysql:mysql-connector-java:8.0.32'
        ),
        application_args=[
            '--mongo_uri', get_uri(MONGO_CONN_ID, conn_type='mongo'),
            '--mysql_uri', get_uri(MYSQL_CONN_ID, jdbc=True, include_user_pwd=False),
            '--mysql_login', '{{ conn.mysql_conn.login }}',
            '--mysql_password', '{{ conn.mysql_conn.password }}',
            '--runtime', '{{ ts }}'
        ]
    )

    transform_artists_task = SparkSubmitOperator(
        task_id='transform_artists',
        conn_id=SPARK_CONN_ID,
        application=SPARK_JOBS_DIR + 'transform_artists.py',
        packages=(
            'org.mongodb.spark:mongo-spark-connector:10.0.5,'
            'org.mongodb:mongodb-driver-sync:4.8.1,'
            'mysql:mysql-connector-java:8.0.32'
        ),
        application_args=[
            '--mongo_uri', get_uri(MONGO_CONN_ID, conn_type='mongo'),
            '--mysql_uri', get_uri(MYSQL_CONN_ID, include_user_pwd=False, jdbc=True),
            '--mysql_login', '{{ conn.mysql_conn.login }}',
            '--mysql_password', '{{ conn.mysql_conn.password }}',
            '--runtime', '{{ ts }}'
        ]
    )


    [spotify_group, soundcloud_group] >> clear_warehouse_task >> \
    [transform_tracks_task, transform_artists_task] >> cleanup_group