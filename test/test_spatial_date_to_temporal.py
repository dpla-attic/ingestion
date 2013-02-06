import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()
url = server() + "spatial_dates_to_temporal"

def _get_server_response(body):
    return H.request(url,"POST",body=body,headers=CT_JSON)

def test_spatial_dates_to_temporal1():
    """
    Should remove dates from the spatial field and place them in the
    temporal field.
    """
    INPUT = {
        "spatial" : [
            {"name": "1901-1999"},
            {"name": " 1901 - 1999 "},
            {"name": "1901 - 01 - 01"},
            {"name": " 1901 / 01 / 01"},
            {"name": "1905-04-12"},
            {"name": "01/01/1901"},
            {"name": "01 - 01 - 1901"},
            {"name": "1901"},
            {"name": "North Carolina"}
        ]
    }
    EXPECTED = {
        "temporal": [
            {"name": "1901-1999"},
            {"name": " 1901 - 1999 "},
            {"name": "1901 - 01 - 01"},
            {"name": " 1901 / 01 / 01"},
            {"name": "1905-04-12"},
            {"name": "01/01/1901"},
            {"name": "01 - 01 - 1901"},
            {"name": "1901"}
        ],
        "spatial" : [{"name": "North Carolina"}]
    }
 
    resp,content = _get_server_response(json.dumps(INPUT)) 
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_spatial_dates_to_temporal2():
    """
    Should not change spatial nor add temporal.
    """
    INPUT = {
        "spatial" : [
            {"name": "Asheville"},
            {"name": "North Carolina"},
            {"name": "12"},
            {"name": "12-"},
            {"name": "12-1"},
            {"name": "12-12"},
            {"name": "12-12-"}
        ]
    }
 
    resp,content = _get_server_response(json.dumps(INPUT)) 
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_spatial_dates_to_temporal3():
    """
    Should remove spatial field if only element is a date.
    """
    INPUT = {
        "spatial" : [
            {"name": " 1901 - 1999 "}
        ]
    }
    EXPECTED = {
        "temporal": [
            {"name": " 1901 - 1999 "}
        ]
    }
 
    resp,content = _get_server_response(json.dumps(INPUT)) 
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
