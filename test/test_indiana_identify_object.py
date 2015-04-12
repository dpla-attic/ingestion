import json
from server_support import server, H
from dict_differ import assert_same_jsons


def test_indiana_identify_object_from_source():
    """Indiana object is assigned from source"""
    request_data = {
        'originalRecord': {'source': 'http://thumbnail/url'}
    }
    expected_result = {
        'originalRecord': {'source': 'http://thumbnail/url'},
        'object': 'http://thumbnail/url'
    }
    url = server() + 'indiana_identify_object'
    resp_meta, resp_body = H.request(url, 'POST',
                                     body=json.dumps(request_data))
    assert resp_meta.status == 200
    assert_same_jsons(expected_result, resp_body)
