import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {
    "Content-Type": "application/json", 
    "Connection": "close"
}

H = httplib2.Http()
url = server() + "bhl_contributor_to_collection"

def _get_server_response(body):
    return H.request(url,"POST",body=body,headers=CT_JSON)

def test_bhl_contributor_to_collection1():
    """Should do nothing"""

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2"
        },
        "collection": {
            "@id": "http://dp.la/api/collections/bhl--item",
            "name": "Item Collection"
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_bhl_contributor_to_collection2():
    """Should copy contributor to collection name and contributor
       acronym to collection @id
    """

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "contributor": "Missouri Botanical Garden, Peter H. Raven Library"
        },
        "collection": {
            "@id": "http://dp.la/api/collections/bhl--item",
            "name": "Item Collection"
        }
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "contributor": "Missouri Botanical Garden, Peter H. Raven Library"
        },
        "collection": {
            "@id": "http://dp.la/api/collections/bhl--MBGPHRL",
            "name": "Missouri Botanical Garden, Peter H. Raven Library"
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
