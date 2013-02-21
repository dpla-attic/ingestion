import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()

def _get_server_response(body, prop=None):
    url = server() + "move_dates_to_temporal"
    if prop:
        url = "%s?prop=%s" % (url, prop)
    return H.request(url,"POST",body=body,headers=CT_JSON)

def test_move_dates_to_temporal_no_prop():
    """
    Should do nothing
    """
    INPUT = {
        "aggregatedCHO": {
            "spatial": [{"name": "1901 - 1999"}],
            "subject": [{"name": "1901 - 1999"}]
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_move_dates_to_temporal_spatial1():
    """
    Should remove dates from the spatial field and place them in the
    temporal field.
    """
    prop = "aggregatedCHO/spatial"
    INPUT = {
        "aggregatedCHO": {
            "spatial" : [
                {"name": "1901-1999"},
                {"name": " 1901 - 1999 "},
                {"name": "1901 - 01 - 01"},
                {"name": " 1901 / 01 / 01 "},
                {"name": "1905-04-12"},
                {"name": "01/01/1901"},
                {"name": "01 - 01 - 1901"},
                {"name": "1901"},
                {"name": "North Carolina"}
            ]
        }
    }
    EXPECTED = {
        "aggregatedCHO": {
            "temporal": [
                {"name": "1901-1999"},
                {"name": "1901 - 1999"},
                {"name": "1901 - 01 - 01"},
                {"name": "1901 / 01 / 01"},
                {"name": "1905-04-12"},
                {"name": "01/01/1901"},
                {"name": "01 - 01 - 1901"},
                {"name": "1901"}
            ],
            "spatial" : [{"name": "North Carolina"}]
        }
    }
 
    resp,content = _get_server_response(json.dumps(INPUT),prop) 
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_move_dates_to_temporal_spatial2():
    """
    Should not change spatial nor add temporal.
    """
    prop = "aggregatedCHO/spatial"
    INPUT = {
        "aggregatedCHO": {
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
    }
 
    resp,content = _get_server_response(json.dumps(INPUT),prop) 
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_move_dates_to_temporal_spatial3():
    """
    Should remove spatial field if only element is a date.
    """
    prop = "aggregatedCHO/spatial"
    INPUT = {
        "aggregatedCHO": {
            "spatial" : [
                {"name": " 1901 - 1999 "}
            ]
        }
    }
    EXPECTED = {
        "aggregatedCHO": {
            "temporal": [
                {"name": "1901 - 1999"}
            ]
        }
    } 
 
    resp,content = _get_server_response(json.dumps(INPUT),prop) 
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_move_dates_to_temporal_subject1():
    """
    Should remove dates from the subject field and place them in the
    temporal field.
    """
    prop = "aggregatedCHO/subject"
    INPUT = {
        "aggregatedCHO": {
            "subject" : [
                {"name": "(1901-1999)"},
                {"name": "1901-1999"},
                {"name": "1901"},
                {"name": " (1902) "},
                {"name": "United States--History--Civil War, 1861-1865--Soldiers--Pictorial works."},
                {"name": "subject1"}
            ]
        }
    }
    EXPECTED = {
        "aggregatedCHO": {
            "temporal": [
                {"name": "1901-1999"},
                {"name": "1901-1999"},
                {"name": "1901"},
                {"name": "1902"},
                {"name": "1861-1865"}
            ],
            "subject" : [
                {"name": "United States--History--Civil War, --Soldiers--Pictorial works."},
                {"name": "subject1"}
            ]
        }
    }
 
    resp,content = _get_server_response(json.dumps(INPUT),prop) 
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_move_dates_to_temporal_subject2():
    """
    Should not change subject nor add temporal.
    """
    prop = "aggregatedCHO/subject"
    INPUT = {
        "aggregatedCHO": {
            "subject" : [
                {"name": "subject1"},
                {"name": " (subject 2) "},
                {"name": "(12)"},
                {"name": "12-"},
                {"name": "12-1"},
                {"name": "12-12"},
                {"name": "12-12-"}
            ]
        }   
    }
 
    resp,content = _get_server_response(json.dumps(INPUT),prop) 
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_move_dates_to_temporal_subject3():
    """
    Should remove subject field if only element is a date.
    """
    prop = "aggregatedCHO/subject"
    INPUT = {
        "aggregatedCHO": {
            "subject" : [
                {"name": " ( 1901 - 1999 ) "}
            ]
        }
    }
    EXPECTED = {
        "aggregatedCHO": {
            "temporal": [
                {"name": "1901 - 1999"}
            ]
        }
    } 
 
    resp,content = _get_server_response(json.dumps(INPUT),prop) 
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
