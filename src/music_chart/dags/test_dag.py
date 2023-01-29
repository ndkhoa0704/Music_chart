from airflow import DAG
from airflow.operators.python import PythonOperator
from music_chart.common.utils import get_uri, cleanup_xcom
import pendulum


MYSQL_CONN_ID = 'mysql_conn'

dag = DAG(
    dag_id='test',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 9, 12, tz='Asia/Ho_Chi_Minh'),
    catchup=False,
)

test_uri = PythonOperator(
    task_id="test_uri",
    python_callable=get_uri,
    op_kwargs={
        'conn_id':MYSQL_CONN_ID, 'include_user_pwd':False, 'jdbc':True
    },
    dag=dag
)

test_uri