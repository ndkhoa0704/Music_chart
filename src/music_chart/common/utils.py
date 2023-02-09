import re
from airflow.hooks.base import BaseHook
from airflow.utils.db import provide_session
from functools import wraps
from airflow.models.xcom import XCom
import itertools
import logging
import requests
from time import sleep
from ratelimit import limits
from datetime import datetime


@provide_session
def cleanup_xcom(ti, ts, session=None):
    session.query(XCom).filter(
        XCom.dag_id == ti.dag_id,
        XCom.timestamp == datetime.fromisoformat(ts)
        ).delete()


def retry(_func=None ,retries: int=0, sleep_time: int=0, stop_retry: bool=False, stop_condition=None):
    '''
    Decorate a function with retry capability.
    Skip all exceptions raise by input function

    :param int retries: number of retries
    :param int sleep_time (optional): sleep time between retries (second)
    :param any stop_condition (optional): stop retrying condition (only if `stop_retry` == True)
    :param bool stop_retry (optional): stop retrying with condition
    :return: decorated function
    :rtype: function

    Exceptions: raise when the function cannot meet the stop_condition
    after reaching the number of retries
    '''
    def decorator_retry(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            counter = 0
            while True:
                try:
                    result = func(*args, **kwargs)
                    if stop_retry: 
                        if result == stop_condition:
                            return result  
                    else: return result
                except:
                    pass
                if sleep_time > 0:
                    sleep(sleep_time)
                counter += 1
                if counter == retries:
                    raise ValueError('Reached retry limit with no expected output')
        return wrapper
    if _func:
        return decorator_retry(_func)
    else:
        return decorator_retry
            

def make_request(url: str, method: str, decode: str=None, *args, **kwargs):
    '''
    Make a request and return response only if reponse status is 200

    :param str url: url
    :param str method: http method
    :param str decode: response decode string
    :return: (decoded) response content
    :rtype str
    '''
    logging.info(f"Fetch '{url}'")
    response = requests.request(method=method, url=url, *args, **kwargs)
    if response.status_code != 200:
        raise requests.ConnectionError(f'Error code {response.status_code}')
    logging.debug(response.raw)
    if not decode:
        return response.content
    return response.content.decode(decode)


def extended_make_request(repeat: int=0, calls: int=0, period: float=0, sleep_time: float=0, *args, **kwargs):
    '''
    Extened version of make_request with repeating capability
    and ratelimit

    :param int repeat: number of repetition
    :param int sleep: sleep time between repetition
    :param int calls: number of requests in a period
    :param float pediod: period interval (seconds)
    :return: (decoded) response content
    :rtype str
    '''
    return retry(limits(calls=calls, period=period)(make_request), retries=repeat, sleep_time=sleep_time)(*args, **kwargs)


def add_url_params(url: str, params: dict):
    '''
    Construct url with parameters

    :param str url: url
    :param dict params: pairs of key value for url params
    :return constructed url
    :rtype str
    '''
    if not '?' in url:
        url += '?'
    for key in params.keys():
        url += f'{key}={params[key]}&'
    return url[:-1]


def json_path(path: str, json_data: dict):
    '''
    Get item from json data based on user defined path
    :param str path: user defined json path
    :param dict json_data: deserialize json data
    :return data field
    :rtype any
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

    # Remove authorization part
    if not include_user_pwd and conn.login is not None and conn.password is not None:
        uri = uri.replace(uri[uri.rfind('/') + 1:uri.rfind('@') + 1],'')

    return uri
    
    
def flatten(data: list):
    '''
    Flatten 2d list to 1d list
    '''
    return list(itertools.chain.from_iterable(data))