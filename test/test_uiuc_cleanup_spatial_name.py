from server_support import server, print_error_log, H
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo

def _get_server_response(body):
    url = server() + "uiuc_cleanup_spatial_name"
    return H.request(url, "POST", body=body)

def test_uiuc_cleanup_spatial_name1():
    INPUT = {
        "sourceResource": {
            "spatial": [
                {
                    "state": "US-CA",
                    "name": "California"
                },
                {
                    "state": "US-CA",
                    "name": "\n\n\n    \nCalifornia"
                }
            ]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "spatial": [
                {
                    "state": "US-CA",
                    "name": "California"
                },
                {
                    "state": "US-CA",
                    "name": "California"
                }
            ]
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp["status"] == "200"
    assert_same_jsons(EXPECTED, json.loads(content)) 
