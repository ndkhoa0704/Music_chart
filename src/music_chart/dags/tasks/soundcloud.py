import requests
import re
import json
from music_chart.common.utils import (
    json_path, add_url_params, get_uri, retry, make_request
)
import logging
from airflow.providers.mongo.hooks.mongo import MongoHook        
from time import sleep
from pymongo import MongoClient
from ratelimit import limits


MAX_REQUEST_RETRIES = 5

@retry(retries=MAX_REQUEST_RETRIES, sleep_time=0.05)
@limits(calls=15000, period=24*3600)
def _make_request(*args, **kwargs):
    return make_request(*args, **kwargs)


def get_client_id() -> str:
    '''
    Get client_id required by soundcloud
    '''
    client_id = None
    response = _make_request('https://soundcloud.com', 'GET', 'utf-8')
    # Get js url to. Last js seems to contain the client_id
    js_urls = re.findall(r'<script crossorigin src="(.+)"></script>', response)
    for url in reversed(js_urls):
        response = _make_request(url, 'GET', 'utf-8')
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
    url_params['client_id'] = get_client_id()
    with MongoHook(conn_id=mongo_conn_id).get_conn() as client:
        db = client[dbname]
        coll = db[collname]
        complete_url = add_url_params(url, url_params)
        response = _make_request(complete_url, 'GET')
        json_data = json.loads(response)
        json_data['data_time'] = ts # Assign dag run time
        coll.insert_one(json_data)


def fetch_artists(
    mongo_conn_id: str, 
    dbname: str, 
    tracks_collname: str, 
    artists_collname: str, 
    ts,
    ti
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
    client_id = get_client_id()
    missing_artists = []
    for i, id in enumerate(ids):
        url = f'https://api-v2.soundcloud.com/users/{id}?client_id={client_id}'
        response = _make_request(url, 'GET')
        json_data = json.loads(response)
        json_data['data_time'] = ts # Assign dag run time
        artists_data.append(json_data)
        if i != 0 and i % 500 == 0:
            logging.info('Inserting')
            collection.insert_many(artists_data)
            artists_data.clear()
    if artists_data:
        collection.insert_many(artists_data)

    ti.xcom_push(key='missing_artists', value=missing_artists)