import requests
import re
import json
from music_chart.common.utils import json_path, add_url_params, get_uri
import logging
from airflow.providers.mongo.hooks.mongo import MongoHook        
from airflow.exceptions import AirflowSkipException
from time import sleep
from pymongo import MongoClient


def _make_requests(url: str, method: str, *args, **kwargs):
    '''
    A make_requests function for soundcloud
    to handle some specific exceptions
    '''
    logging.info(f"Fetching '{url}'")
    response = requests.request(method=method, url=url, *args, **kwargs)
    if response.status_code != 200:
        # error_response = json.loads(response.content)
        if response.status_code == 429: # Reach rate limits
            # reset_time = json_path('errors.[*].meta.reset_times', error_response)
            raise AirflowSkipException
        elif response.status_code == 403:
            return 403
        else: 
            raise Exception(f'Error code: {response.status_code} \n {response.content}')
        
    return response.content


def _get_client_id() -> str:
    '''
    Get client_id required by soundcloud
    '''

    client_id = None
    response = _make_requests('https://soundcloud.com', 'GET').decode('utf-8')
    # Get js url to. Last js seems to contain the client_id
    js_urls = re.findall(r'<script crossorigin src="(.+)"></script>', response)
    for url in reversed(js_urls):
        response = _make_requests(url, 'GET').decode('utf-8')
        # Get client_id 
        js_file = response
        client_id_idx = js_file.find(',client_id:"')
        if client_id_idx == -1:
            sleep(0.05)
            continue
        else: 
            client_id = js_file[client_id_idx + 12: client_id_idx + 44]
            break
    if client_id is None:
        raise Exception('Cannot get client_id')

    return client_id


def fetch_top_tracks(
        mongo_conn_id: str, 
        collname: str, 
        dbname: str, 
        ts
):
    '''
    Fetch top tracks to mongodb
    '''
    url_params = {
        'offset': 20,
        'genre': 'soundcloud%3Agenres%3Aall-music',
        'kind': 'top',
        'limit': 100
    }
    url = 'https://api-v2.soundcloud.com/charts'
    with MongoHook(conn_id=mongo_conn_id).get_conn() as client:
        db = client[dbname]
        coll = db[collname]
        for _ in range(10):
            url_params['client_id'] = _get_client_id()
            complete_url = add_url_params(url, url_params)
            response = _make_requests(complete_url, 'GET')
            if response == 403: # Client error
                sleep(0.05)
                continue
            else: break
        json_data = json.loads(response)
        json_data['data_time'] = ts # Assign dag run time
        coll.insert_one(json_data)


def fetch_artists(
    mongo_conn_id: str, 
    dbname: str, 
    tracks_collname: str, 
    artists_collname: str, 
    ts
):
    '''
    Fetch artists' data to mongodb
    '''
    client = MongoClient(get_uri(mongo_conn_id, conn_type='mongo'))
    db = client[dbname]
    collection = db[tracks_collname]

    pipeline = [
        { '$match': { 'data_time': ts } },
        { '$project': {"collection.track.user_id": 1, "_id": 0 }}
    ]
    
    for _ in range(10):
        data = next(collection.aggregate(pipeline), None)
        if data is not None:
            break
        sleep(0.05)
    if data is None:
        raise Exception('Cannot fetch tracks data')
    
    ids = set(json_path('collection.[*].track.user_id', data))
    artists_data = [] 
    collection = db[artists_collname]
    client_id = _get_client_id()
    for i, id in enumerate(ids):
        url = f'https://api-v2.soundcloud.com/users/{id}?client_id={client_id}'
        for _ in range(10):
            logging.info('Fetching: {}'.format(url))
            response = _make_requests(url, 'GET')
            if response == 403:
                sleep(0.05)
                client_id = _get_client_id()
            else: break

        json_data = json.loads(response)
        json_data['data_time'] = ts # Assign dag run time
        artists_data.append(json_data)
        if i != 0 and i % 500 == 0:
            logging.info('Inserting')
            collection.insert_many(artists_data)
            artists_data.clear()
    if artists_data:
        collection.insert_many(artists_data)