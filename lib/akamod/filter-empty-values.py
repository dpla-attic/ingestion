__author__ = 'Alexey R.'

import sys

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'content-type'


def filter_empty_lives(d, ignore_keys=[]):
    """
    Removes keys with empty value from dictionary;
    Returns: cleaned dictionary;
    """
    for k, v in d.items():
        if isinstance(v, dict) and v:
            d[k] = filter_empty_lives(v)
        else:
            if not v and k not in ignore_keys:
                del d[k]
    return d

def filter_empty_values(d, ignore_keys=[]):
    hash_before = hash(str(d))
    hash_current = None
    while hash_before != hash_current:
        hash_before = hash_current
        hash_current = hash(str(filter_empty_lives(d, ignore_keys)))
    return d

def test_filtering():
    d = {"v1": "", "v2": "value2", "v3": {"vv1": "", "vv2": "v_value2"}, "v4": {}, "v5": {"0": {"name": ""}, "1": {"name": "name_value_1"}}}
    print d, hash(str(d))
    print filter_empty_values(d), hash(str(d))

@simple_service('POST', 'http://purl.org/la/dp/filter_empty_values', 'filter_empty_values', HTTP_TYPE_JSON)
def filter_empty_values_endpoint(body, ctype, ignore_key="dplaSourceRecord"):
    try:
        assert ctype == HTTP_TYPE_JSON
        data = json.loads(body)
    except Exception as e:
        error_text = "%s: %s" % (e.__class__.__name__, str(e))
        logger.exception(error_text)
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return error_text

    ignore_keys = [k.strip() for k in ignore_key.split(",") if k]
    filter_empty_values(data, ignore_keys)
    return json.dumps(data)


def main(args):
    test_filtering()

if __name__ == "__main__":
    main(sys.argv)

