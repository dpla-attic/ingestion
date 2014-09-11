import json
from server_support import server, H
from dict_differ import assert_same_jsons


def test_strip_html():
    """'strip_html' strips HTML tags and entities recursively"""
    request_data = {
        'a': {
            'b': [' <i>string</i> <b>one</b> \n \t', 'string &lt; two  ']
        },
        'c': '  \n <p>string three</p>',
        'd': {},
        'e': 1
    }
    expected_result = {
        'a': {
            'b': [u'string one', u'string < two']
        },
        'c': u'string three',
        'd': {},  # unaltered
        'e': 1    # unaltered
    }
    url = server() + 'strip_html'
    resp_meta, resp_body = H.request(url, 'POST',
                                     body=json.dumps(request_data))
    assert resp_meta.status == 200
    assert_same_jsons(expected_result, resp_body)
