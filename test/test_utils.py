import pytest
from music_chart.common.utils import (
    json_path, retry
) 
from time import time
from random import randint


@pytest.fixture
def json_path_data1():
    return {
        'person': [
        {
            'name': 'homie',
            'address': '46 dict'
        },
        {
            'name': 'bro',
            'address': '64 tcid'
        }
        ]
    }
@pytest.fixture
def json_path_data2():
    return [
        {
            'object': 'customer',
            'color': 'red'
        },
        {
            'object': 'bat',
            'color': 'black'
        }
    ]

@pytest.fixture
def json_path_data3():
    return [
        [
            {'name':'a'},
            {'name':'b'}
        ],
        [
            {'name':'b'},
            {'name':'a'}
        ],
        [
            {'name':'c'},
            {'name':'c'}
        ]
    ]

def test_json_path(json_path_data1, json_path_data2, json_path_data3):
    assert json_path('person.[0].name', json_path_data1) == 'homie'
    assert json_path('person.[*].name', json_path_data1) == ['homie', 'bro']
    assert json_path('[*].object', json_path_data2) == ['customer','bat']
    assert json_path('[*].[*].name', json_path_data3) == [['a','b'],['b','a'],['c','c']]


def test_retry_no_args():
    @retry
    def add(a, b):
        return a + b
    assert add(1, 2) == 3

def test_retry_with_args():
    @retry(sleep_time=3)
    def add(a, b):
        return a + b
    assert add(1, 2) == 3

def test_retry_sleep():
    @retry(sleep_time=0.5, retries=2)
    def func():
        raise Exception('exception')
    start = time()
    with pytest.raises(ValueError): 
        func()
    end = time()
    assert round(end - start) == 1

def test_retry_condition_fail():
    @retry(retries=5, stop_retry=True, stop_condition=1)
    def func():
        return 0
    
    with pytest.raises(ValueError):
        func()

def test_retry_condition_success():
    @retry(retries=5, stop_retry=True, stop_condition=1)
    def func():
        return 1
    
    assert func() == 1