import sys
from server_support import server, print_error_log, H
from amara.thirdparty import json

def _get_server_response(body, prop=None, to_prop=None):
    url = server() + "move_date_values"
    if prop:
        url = "%s?prop=%s" % (url, prop)
    if to_prop:
        url = "%s&to_prop=%s" % (url, to_prop)
    return H.request(url,"POST",body=body)

def test_move_date_values_no_prop():
    """
    Should do nothing
    """
    INPUT = {
        "aggregatedCHO": {
            "spatial": ["1901 - 1999"],
            "subject": ["1901 - 1999"]
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_move_date_values_spatial1():
    """
    Should remove dates from the spatial field and place them in the
    temporal field.
    """
    prop = "aggregatedCHO/spatial"
    INPUT = {
        "aggregatedCHO": {
            "spatial" : [
                "1901-1999",
                " 1901 - 1999 ",
                "1901 - 01 - 01",
                " 1901 / 01 / 01 ",
                "1905-04-12",
                "01/01/1901",
                "01 - 01 - 1901",
                "1901",
                "North Carolina"
            ]
        }
    }
    EXPECTED = {
        "aggregatedCHO": {
            "temporal": [
                "1901-1999",
                "1901 - 1999",
                "1901 - 01 - 01",
                "1901 / 01 / 01",
                "1905-04-12",
                "01/01/1901",
                "01 - 01 - 1901",
                "1901"
            ],
            "spatial" : ["North Carolina"]
        }
    }
 
    resp,content = _get_server_response(json.dumps(INPUT),prop) 
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_move_date_values_spatial2():
    """
    Should not change spatial nor add temporal.
    """
    prop = "aggregatedCHO/spatial"
    INPUT = {
        "aggregatedCHO": {
            "spatial" : [
                "Asheville",
                "North Carolina",
                "12",
                "12-",
                "12-1",
                "12-12",
                "12-12-"
            ]
        }   
    }
 
    resp,content = _get_server_response(json.dumps(INPUT),prop) 
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_move_date_values_spatial3():
    """
    Should remove spatial field if only element is a date.
    """
    prop = "aggregatedCHO/spatial"
    INPUT = {
        "aggregatedCHO": {
            "spatial" : [
                " 1901 - 1999 "
            ]
        }
    }
    EXPECTED = {
        "aggregatedCHO": {
            "temporal": [
                "1901 - 1999"
            ]
        }
    } 
 
    resp,content = _get_server_response(json.dumps(INPUT),prop) 
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_move_date_values_subject1():
    """
    Should remove dates from the subject field and place them in the
    temporal field.
    """
    prop = "aggregatedCHO/subject"
    INPUT = {
        "aggregatedCHO": {
            "subject" : [
                "(1901-1999)",
                "1902-1999",
                "1903",
                " (1904) ",
                ".1905?",
                "United States--History--Civil War, 1861-1865--Soldiers--Pictorial works.",
                "subject1"
            ]
        }
    }
    EXPECTED = {
        "aggregatedCHO": {
            "temporal": [
                "1901-1999",
                "1902-1999",
                "1903",
                "1904",
                "1905"
            ],
            "subject" : [
                "United States--History--Civil War, 1861-1865--Soldiers--Pictorial works.",
                "subject1"
            ]
        }
    }
 
    resp,content = _get_server_response(json.dumps(INPUT),prop) 
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_move_date_values_subject2():
    """
    Should not change subject nor add temporal.
    """
    prop = "aggregatedCHO/subject"
    INPUT = {
        "aggregatedCHO": {
            "subject" : [
                "subject1",
                " (subject 2) ",
                "(12)",
                "12-",
                "12-1",
                "12-12",
                "12-12-",
                "12345",
                "02-02-02-",
                "1984-1999-2011-2013",
                "1234 1234",
                "01/01/13 01/01/13"
            ]
        }   
    }
 
    resp,content = _get_server_response(json.dumps(INPUT),prop) 
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_move_date_values_subject3():
    """
    Should remove subject field if only element is a date.
    """
    prop = "aggregatedCHO/subject"
    INPUT = {
        "aggregatedCHO": {
            "subject" : [
                " ( 1901 - 1999 ) "
            ]
        }
    }
    EXPECTED = {
        "aggregatedCHO": {
            "temporal": [
                "1901 - 1999"
            ]
        }
    } 
 
    resp,content = _get_server_response(json.dumps(INPUT),prop) 
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_move_date_values_to_date():
    """
    Should remove subject field if only element is a date.
    """
    prop = "aggregatedCHO/spatial"
    to_prop = "aggregatedCHO/date"
    INPUT = {
        "aggregatedCHO": {
            "spatial" : [
                "1861-12-30/1862-07-13",
                "(1862/12/30 - 1863/07/13)"
            ]
        }
    }
    EXPECTED = {
        "aggregatedCHO": {
            "date": [
                "1861-12-30/1862-07-13",
                "1862/12/30 - 1863/07/13"
            ]
        }
    } 
 
    resp,content = _get_server_response(json.dumps(INPUT),prop,to_prop) 
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
