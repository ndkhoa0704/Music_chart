from airflow.providers.mongo.hooks.mongo import MongoHook


def test_mongo(conn_id):
    hook = MongoHook(conn_id=conn_id)
