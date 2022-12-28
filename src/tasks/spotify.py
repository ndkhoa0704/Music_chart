from src.common.utils import json_path, add_url_params
import requests
import logging
from airflow.providers.mongo.hooks.mongo import MongoHook
import json
import base64


def _make_requests(url: str, method: str,  *args, **kwargs):
    logging.info(f"Fetch '{url}'")
    response = requests.request(method=method, url=url, *args, **kwargs)
    if response.status_code != 200:
        raise Exception(f'Error code {response.status_code}\n{response.content}')
    return response.content


def get_access_token(client_id: str, client_secret: str) -> str:
    '''
    Get spotify access token
    '''
    print(client_id)
    print(client_secret)
    headers = {
        'Authorization': f'Basic ' + \
            base64.b64encode(f'{client_id}:{client_secret}'.encode('ascii')).decode('ascii')
    }
    data = {
        'grant_type': 'client_credentials'
    }
    response = _make_requests('https://accounts.spotify.com/api/token', 'POST', headers=headers, data=data)
    access_token = json_path('access_token', json.loads(response))
    return access_token


def fetch_to_mongo(url: str, mongo_conn_id: str, access_token: str, url_params: dict=None):
    with MongoHook(conn_id=mongo_conn_id).get_conn() as client:
        db = client['spotify']
        collection = db['chart']
        headers = {
            'Authorization': f'Bearer {access_token}' 
        }
        complete_url = add_url_params(url, url_params)
        response = _make_requests(complete_url, 'GET', headers=headers)
        json_data = json.loads(response)
        collection.insert_one(json_data)