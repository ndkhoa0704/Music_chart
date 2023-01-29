import re
from airflow.hooks.base import BaseHook
from airflow.utils.db import provide_session
from airflow.models.xcom import XCom
import itertools


@provide_session
def cleanup_xcom(ti, session=None):
    session.query(XCom).filter(XCom.dag_id == ti.dag_id).delete()


def add_url_params(url: str, params):
    '''
    Construct url with parameters
    '''
    if not '?' in url:
        url += '?'
    for key in params.keys():
        url += f'{key}={params[key]}&'
    return url[:-1]


def json_path(path: str, json_data: dict):
    '''
    Get item from json data based on user defined path
    '''
    if path is None: # End recursion
        return json_data
    
    dot_idx = path.find('.')
    if dot_idx == -1:
        next_path = None
        item = path
    else:
        next_path = path[dot_idx + 1:]
        item = path[:dot_idx]
    del dot_idx

    if isinstance(json_data, list):
        in_brackets = next(re.finditer(r'\[(.+)\]', item), None)
        if in_brackets is None:
            raise Exception('Not a valid path')
        in_brackets = in_brackets.group(1)
        if in_brackets == '*':
            return [json_path(next_path, i) for i in json_data]
        elif re.match(r'\d*:\d*', in_brackets):
            return [json_path(next_path, i) for i in json_data]
        elif not in_brackets.isdigit():
            raise Exception('Not a valid path')
        else:
            in_brackets = int(in_brackets)
        json_data = json_data[in_brackets]
    else:
        json_data = json_data.get(item)
        if json_data is None:
            return None
    return json_path(next_path, json_data)


def get_uri(conn_id: str, conn_type: str=None, include_user_pwd: bool=True, jdbc: bool=False):
    '''
    Get uri of a connection
    '''
    conn = BaseHook().get_connection(conn_id)
    if conn_type == 'mongo':
        conn.conn_type = 'mongodb'
    
    uri = conn.get_uri()
    if jdbc:
        uri = 'jdbc:' + uri

    if not include_user_pwd and conn.login is not None and conn.password is not None:
        uri = uri.replace(uri[uri.rfind('/') + 1:uri.rfind('@') + 1],'')

    return uri
    
    
def flatten(data: list):
    '''
    Flatten 2d list to 1d list
    '''
    return list(itertools.chain.from_iterable(data))