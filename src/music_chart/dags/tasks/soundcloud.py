import requests
import re
import json
from music_chart.common.utils import json_path, add_url_params
import logging
from airflow.providers.mongo.hooks.mongo import MongoHook        
from airflow.exceptions import AirflowSkipException
from time import sleep


def _make_requests(url: str, method: str, *args, **kwargs):
    '''
    Raise unknown exceptions
    '''
    logging.info(f"Fetching '{url}'")
    response = requests.request(method=method, url=url, *args, **kwargs)
    if response.status_code != 200:
        error_response = json.loads(response.content)
        if response.status_code == 429: # Reach rate limits
            # reset_time = json_path('errors.[*].meta.reset_times', error_response)
            raise AirflowSkipException
        elif response.status_code == 403:
            return 403
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
    logging.info('CLIENT ID: {}'.format(client_id)) # TEST
    return client_id


def fetch_top_tracks(
        url: str, 
        mongo_conn_id: str, 
        collection: str, 
        schema: str, 
        ts,
        client_id,
        url_params: dict=None
):
    '''
    Fetch top tracks to mongodb
    '''
    params = url_params
    with MongoHook(conn_id=mongo_conn_id).get_conn() as client:
        db = client[schema]
        coll = db[collection]
        while True:
            params['client_id'] = client_id
            complete_url = add_url_params(url, params)
            response = _make_requests(complete_url, 'GET')
            if response == 403: # CLient error
                sleep(0.05)
                continue
            break
        json_data = json.loads(response)
        json_data['data_time'] = ts # Assign dag run time
        coll.insert_one(json_data)