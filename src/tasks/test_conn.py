from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook


def test_mongo(conn_id):
    hook = MongoHook(conn_id=conn_id)

def test_spark(conn_id):
    hook = SparkSubmitHook(conn_id=conn_id)
    hook.submit()