from music_chart.common.utils import (
    json_path, add_url_params, get_uri, flatten
)
import requests
import logging
from airflow.providers.mongo.hooks.mongo import MongoHook
import json
import base64
from pymongo import MongoClient
from time import sleep


def _make_requests(url: str, method: str,  *args, **kwargs):
    logging.info(f"Fetch '{url}'")
    response = requests.request(method=method, url=url, *args, **kwargs)
    if response.status_code != 200:
        raise Exception(f'Error code {response.status_code}\n{response.content}')
    return response.content


def get_access_token(client_payload: str) -> str:
    '''
    Get spotify access token
    '''
    payload = json.loads(client_payload)
    headers = {
        'Authorization': f'Basic ' + \
            base64.b64encode(f'{payload["client_id"]}:{payload["client_secret"]}'.encode('ascii')).decode('ascii')
    }
    data = {
        'grant_type': 'client_credentials'
    }
    response = _make_requests('https://accounts.spotify.com/api/token', 'POST', headers=headers, data=data)
    access_token = json_path('access_token', json.loads(response))

    return access_token


def fetch_top_tracks(
    mongo_conn_id: str, 
    collname: str, 
    dbname: str,
    client_payload: str,
    ts,
    ti
):
    '''
    Fetch top tracks to mongodb
    '''
    url = 'https://api.spotify.com/v1/playlists/37i9dQZEVXbNG2KDcFcKOF/tracks'
    url_params = {
        'limit': 50
    }
    access_token = get_access_token(client_payload)
    with MongoHook(conn_id=mongo_conn_id).get_conn() as client:
        db = client[dbname]
        coll = db[collname]
        headers = {
            'Authorization': f'Bearer {access_token}' 
        }
        complete_url = add_url_params(url, url_params)
        response = _make_requests(complete_url, 'GET', headers=headers)
        json_data = json.loads(response)
        json_data['data_time'] = ts # Assign dag run time
        coll.insert_one(json_data)


def fetch_artists(
    mongo_conn_id: str, 
    dbname: str, 
    tracks_collname: str, 
    artists_collname: str, 
    client_payload: str,
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
        { '$project': {"items.track.artists.href": 1, "_id": 0 }}
    ]

    for _ in range(10):
        data = next(collection.aggregate(pipeline), None)
        if data is not None:
            break
        sleep(0.05)
    if data is None:
        raise Exception('Cannot fetch tracks data')
    hrefs = set(flatten(json_path('items.[*].track.artists.[*].href', data)))
    
    artists_data = []
    collection = db[artists_collname]
    access_token = get_access_token(client_payload)
    headers = {
        'Authorization': f'Bearer {access_token}' 
    }
    for i, url in enumerate(hrefs):
        logging.info('Fetching: {}'.format(url))
        response = _make_requests(url, 'GET', headers=headers)
        json_data = json.loads(response)
        json_data['data_time'] = ts # Assign dag run time
        artists_data.append(json_data)
        if i != 0 and i % 500 == 0: 
            logging.info('Inserting')
            collection.insert_many(artists_data)
            artists_data.clear()
    if artists_data:
        collection.insert_many(artists_data)