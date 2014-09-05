import json
from server_support import server, H
from dict_differ import assert_same_jsons


def test_strip():
    """'strip' removes whitespace recursively"""
    request_data = {
        'a': ['  aaa ', '\n  aaaa \t'],
        'b': ' bb\r',
        'c': {'x': 1, 'y': {}}
    }
    expected_result = {
        'a': ['aaa', 'aaaa'],
        'b': 'bb',
        'c': {'x': 1, 'y': {}}
    }
    url = server() + 'strip'
    resp_meta, resp_body = H.request(url, 'POST',
                                     body=json.dumps(request_data))
    assert resp_meta.status == 200
    assert_same_jsons(expected_result, resp_body)
