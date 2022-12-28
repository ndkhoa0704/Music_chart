import requests
import re
import json
from src.common.utils import json_path, add_url_params
import logging
from airflow.providers.mongo.hooks.mongo import MongoHook        
import backoff

def _make_requests(url: str, method: str, *args, **kwargs):
    logging.info(f"Fetching '{url}'")
    response = requests.request(method=method, url=url, *args, **kwargs)
    if response.status_code != 200:
        error_response = json.loads(response.content)
        if response.status_code == 429: # Reach rate limits
            reset_time = json_path('errors.[*].meta.reset_times', error_response)
            return reset_time
        else: 
            raise Exception(f'Error code: {response.status_code} \n {response.content}')
    return response.content


def get_client_id() -> str:
    '''
    Get client_id required by soundcloud
    '''
    response = _make_requests('https://soundcloud.com', 'GET').decode('utf-8')
    # Get js url to. Last js seems to contain the client_id
    js_urls = re.findall(r'<script crossorigin src="(.+)"></script>', response)
    client_id = None
    for url in reversed(js_urls):
        response = _make_requests(url, 'GET').decode('utf-8')
        # Get client_id 
        js_file = response
        client_id_idx = js_file.find(',client_id:"')
        if client_id_idx == -1:
            continue
        else: 
            client_id = js_file[client_id_idx + 12: client_id_idx + 44]
            break
    return client_id


def fetch_to_mongo(url: str, mongo_conn_id: str, url_params: dict=None):
    '''
    Fetch json data from url and store them to mongodb
    '''
    params = url_params
    with MongoHook(conn_id=mongo_conn_id).get_conn() as client:
        db = client['soundcloud']
        collection = db['chart']
        complete_url = add_url_params(url, params)
        response = _make_requests(complete_url, 'GET')
        json_data = json.loads(response)
        collection.insert_one(json_data)