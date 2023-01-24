import re
import logging
from airflow.hooks.base import BaseHook
from airflow.utils.db import provide_session
from airflow.models import XCom


@provide_session
def cleanup_xcom(context, session=None):
    dag_id = context["ti"]["dag_id"]
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


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
    # if not isinstance(json_data, dict) or isinstance(json_data, list):
    #     raise Exception('Not a valid json')
    logging.info('JSON path: %s' % path)
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


def get_uri(conn_id: str, conn_type: str):
    '''
    Get uri of a mongodb connection
    '''
    conn = BaseHook().get_connection(conn_id)
    if conn_type == 'mongo':
        conn.conn_type = 'mongodb'
        return conn.get_uri()
    else:
        raise NotImplementedError