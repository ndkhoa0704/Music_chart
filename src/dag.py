from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from tasks.test_conn import test_mongo, test_spark
import pendulum
from datetime import timedelta


default_args = {
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}


with DAG(
    dag_id='hashtag',
    schedule_interval='@hourly',
    start_date=pendulum.datetime(2022, 9, 12, tz='Asia/Ho_Chi_Minh'),
    max_active_runs=5,
    catchup=False,
    default_args=default_args
):

    test_mongo_task = PythonOperator(
        task_id='test_mongo',
        python_callable=test_mongo,
        op_kwargs={
            'conn_id': 'mongo_conn'
        }
    )

    test_spark_task = PythonOperator(
        task_id='test_spark',
        python_callable=test_spark,
        op_kwargs={
            'conn_id': 'spark_conn'
        }
    )

    test_mysql_task = EmptyOperator(
        task_id='test_mysql'
    )

    [
        test_mongo_task,
        test_spark_task,
        test_mysql_task
    ]