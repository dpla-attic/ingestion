"""
 Akara module that cleans empty lives of given json;
 to test locally, save TEST_EXAMPLE to local file (i.e. artstor_doc.js) and then run:

 url -X POST -d @test/artstor_doc.js -H "Content-Type: application/json" http://localhost:8879/filter_empty_values
"""

__author__ = 'Alexey R.'

import sys
import copy

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'


def filter_empty_leaves(d, ignore_keys=tuple()):
    """
    Removes empty leaves from dictionary tree; (Empty leaf = key without value;)

    Ignores keys listed in ignore_keys sequence;
    Returns: cleaned dictionary;
    """
    def sub_cleaner(el):
        if isinstance(el, dict):
            return filter_empty_leaves(copy.deepcopy(el), ignore_keys)
        else:
            return el

    for k, v in d.items():
        if k in ignore_keys:
            continue
        elif isinstance(v, dict) and v:
            d[k] = filter_empty_leaves(v, ignore_keys)
        elif isinstance(v, list) and v:
            d[k] = [sub_cleaner(e) for e in v if e]
        else:
            if not v and k not in ignore_keys:
                del d[k]
    return d

def filter_empty_values(d, ignore_keys=tuple()):
    """
    Removes keys with empty values from dictionaries,
    repeatedly runs leaves cleaner until all empty leaves are removed.
    """
    hash_before = hash(str(d))
    hash_current = None
    while hash_before != hash_current:
        hash_before = hash_current
        hash_current = hash(str(filter_empty_leaves(d, ignore_keys)))
    return d

def test_filtering():
    source = {"v1": "", "v2": "value2", "v3": {"vv1": "", "vv2": "v_value2"}, "v4": {}, "v5": {"0": {"name": ""}, "1": {"name": "name_value_1"}}, "v6": ["", "vvalue6", {}, {"v_sub": ""}], "v7": [""]}
    expected = {"v2": "value2", "v3": {"vv2": "v_value2"}, "v5": {"1": {"name": "name_value_1"}}, "v6": ["vvalue6"]}
    filtered = filter_empty_values(copy.deepcopy(source))
    assert expected == filtered, "Expected dictionary does not equal to filtered"
    #d2 = json.loads(TEST_EXAMPLE)
    #print d2, hash(str(d2))
    #print filter_empty_values(d2, ("dplaSourceRecord",)), hash(str(d2))

@simple_service('POST', 'http://purl.org/la/dp/filter_empty_values', 'filter_empty_values', HTTP_TYPE_JSON)
def filter_empty_values_endpoint(body, ctype, ignore_key="dplaSourceRecord"):
    try:
        assert ctype == HTTP_TYPE_JSON, "%s is not %s" % (HTTP_HEADER_TYPE, HTTP_TYPE_JSON)
        data = json.loads(body)
    except Exception as e:
        error_text = "Bad JSON: %s: %s" % (e.__class__.__name__, str(e))
        logger.exception(error_text)
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return error_text

    ignore_keys = [k.strip() for k in ignore_key.split(",") if k]
    data = filter_empty_values(data, ignore_keys)
    return json.dumps(data)


#def main(args):
#    test_filtering()
#
#if __name__ == "__main__":
#    main(sys.argv)

