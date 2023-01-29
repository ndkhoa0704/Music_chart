import pytest
from music_chart.common.utils import json_path

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