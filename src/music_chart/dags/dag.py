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



dag =  DAG(
    dag_id=MONGO_DB_NAME,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 9, 12, tz='Asia/Ho_Chi_Minh'),
    max_active_runs=5,
    catchup=False,
    default_args=default_args
)

def create_clean_group(group_name: str, source: str, dag):
    'Create clean warehouse task group'
    with TaskGroup(group_name, dag=dag) as group:
        clean_tracks_task = MySqlOperator(
            task_id='clean_tracks',
            doc_md='''
            Clean old data daily by timestamp
            ''',
            mysql_conn_id=MYSQL_CONN_ID,
            sql='''
            DELETE t1, t2
            FROM music_chart.tracks AS t1 JOIN music_chart.data_time_metadata AS t2
            ON t1.id = t2.record_id
            WHERE t2.data_time = '{{ ds }}' AND t1.source = '%s'
            ''' % source,
            dag=dag
        )

        clean_artists_task = MySqlOperator(
            task_id='clean_artists',
            doc_md='''
            Clean old data daily by timestamp
            ''',
            mysql_conn_id=MYSQL_CONN_ID,
            sql='''
            DELETE t1, t2
            FROM music_chart.artists AS t1 JOIN music_chart.data_time_metadata AS t2
            ON t1.id = t2.record_id
            WHERE t2.data_time = '{{ ds }}' AND t1.source = '%s'
            ''' % source,
            dag=dag
        )

        clean_genres_task = MySqlOperator(
            task_id='clean_track_genres',
            doc_md='''
            Clean old data daily by timestamp
            ''',
            mysql_conn_id=MYSQL_CONN_ID,
            sql='''
            DELETE t1, t2
            FROM music_chart.track_genres AS t1 JOIN music_chart.data_time_metadata AS t2
            JOIN music_chart.tracks AS t3
            ON t1.id = t2.record_id AND t1.track_id = t3.track_id
            WHERE t2.data_time = '{{ ds }}' AND t3.source = '%s'
            ''' % source,
            dag=dag
        )
        clean_artists_task, clean_genres_task, clean_tracks_task
    return group


cleandwh_soundcloud_group = create_clean_group('cleandwh_soundcloud', 'soundcloud', dag)        
cleandwh_spotify_group = create_clean_group('cleandwh_spotify', 'spotify', dag)        

with TaskGroup('fetch_soundcloud', dag=dag) as fetch_soundcloud_group:
    fetch_top_tracks_task = PythonOperator(
        task_id='fetch_top_tracks',
        python_callable=soundcloud.fetch_top_tracks,
        op_kwargs={
            'mongo_conn_id': MONGO_CONN_ID,
            'collname': 'soundcloud_top_tracks',
            'dbname': MONGO_DB_NAME
        },
        dag=dag
    )

    fetch_artists_task = PythonOperator(
        task_id='fetch_artists',
        python_callable=soundcloud.fetch_artists,
        op_kwargs={
            'mongo_conn_id': MONGO_CONN_ID,
            'dbname': MONGO_DB_NAME,
            'tracks_collname': 'soundcloud_top_tracks',
            'artists_collname': 'soundcloud_artists'
        },
        dag=dag
    )

    fetch_top_tracks_task >> fetch_artists_task

with TaskGroup('fetch_spotify', dag=dag) as fetch_spotify_group:
    fetch_top_tracks_task = PythonOperator(
        task_id='fetch_top_tracks',
        python_callable=spotify.fetch_top_tracks,
        op_kwargs={
            'mongo_conn_id': MONGO_CONN_ID,
            'collname': 'spotify_top_tracks',
            'dbname': MONGO_DB_NAME,
            'client_payload': '{{ conn.spotify_creds.password }}'
        },
        dag=dag
    )

    fetch_artists_task = PythonOperator(
        task_id='fetch_artists',
        python_callable=spotify.fetch_artists,
        op_kwargs={
            'mongo_conn_id': MONGO_CONN_ID,
            'dbname': MONGO_DB_NAME,
            'tracks_collname': 'spotify_top_tracks',
            'artists_collname': 'spotify_artists',
            'client_payload': '{{ conn.spotify_creds.password }}'
        },
        dag=dag
    )

    fetch_top_tracks_task >> fetch_artists_task
    
with TaskGroup('cleanup', dag=dag) as cleanup_group:
    clean_xcom_task = PythonOperator(
        task_id='clean_xcom',
        python_callable=cleanup_xcom,
        dag=dag
    )
    
    clean_xcom_task

transform_soundcloud_task = SparkSubmitOperator(
    task_id='transform_soundcloud',
    conn_id=SPARK_CONN_ID,
    application=SPARK_JOBS_DIR + 'transform_soundcloud.py',
    packages=(
        'org.mongodb.spark:mongo-spark-connector:10.0.5,'
        'org.mongodb:mongodb-driver-sync:4.8.1'
    ),
    application_args=[
        '--mongo_uri', get_uri(MONGO_CONN_ID, conn_type='mongo'),
        '--mysql_uri', get_uri(MYSQL_CONN_ID, jdbc=True, include_user_pwd=False),
        '--mysql_login', '{{ conn.mysql_conn.login }}',
        '--mysql_password', '{{ conn.mysql_conn.password }}',
        '--runtime', '{{ ts }}'
    ],
    py_files=SPARK_JOBS_DIR + 'utils.py',
    dag=dag
)

transform_spotify_task = SparkSubmitOperator(
    task_id='transform_spotify',
    conn_id=SPARK_CONN_ID,
    application=SPARK_JOBS_DIR + 'transform_spotify.py',
    packages=(
        'org.mongodb.spark:mongo-spark-connector:10.0.5,'
        'org.mongodb:mongodb-driver-sync:4.8.1'
    ),
    application_args=[
        '--mongo_uri', get_uri(MONGO_CONN_ID, conn_type='mongo'),
        '--mysql_uri', get_uri(MYSQL_CONN_ID, jdbc=True, include_user_pwd=False),
        '--mysql_login', '{{ conn.mysql_conn.login }}',
        '--mysql_password', '{{ conn.mysql_conn.password }}',
        '--runtime', '{{ ts }}'
    ],
    py_files=SPARK_JOBS_DIR + 'utils.py',
    dag=dag
)


fetch_spotify_group >>  cleandwh_spotify_group
fetch_soundcloud_group >> cleandwh_soundcloud_group
[cleandwh_spotify_group, cleandwh_soundcloud_group] >> transform_soundcloud_task >> transform_spotify_task >> clean_xcom_task