import sys
from server_support import server
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()
url = server() + "enrich-subject"

def _get_server_response(body):
    return H.request(url,"POST",body=body,headers=CT_JSON)

def test_enrich_subject_capitalize_firs_letter():
    """Should capitalize first letter of each subject"""

    INPUT = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            {"name": "subject"},
            {"name": "hi there"},
            {"name": "hello"}
        ]
    }
    EXPECTED = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            {"name": "Subject"},
            {"name": "Hi there"},
            {"name": "Hello"}
        ]
    }

def test_enrich_subject_one_char_string1():
    """Should not add one or two char strings to DPLA schema"""

    INPUT = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            {"name": "subject"},
            {"name": "a"},
            {"name": "ab"},
            {"name": "hello"}
        ]
    }
    EXPECTED = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            {"name": "Subject"},
            {"name": "Hello"}
        ]
    }
 
    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    print str(json.loads(content))
    assert json.loads(content) == EXPECTED

def test_enrich_subject_one_char_string2():
    """Should not include subject"""

    INPUT = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            {"name": "h"},
            {"name": "hi"}
        ]
    }
    EXPECTED = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ]
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    print str(json.loads(content))
    assert json.loads(content) == EXPECTED

def test_enrich_subject_remove_period_space():
    """Should not include subject"""

    INPUT = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            {"name": "hello there"},
            {"name": "123"},
            {"name": ". hi "}
        ]
    }
    EXPECTED = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            {"name": "Hello there"},
            {"name": "123"},
            {"name": "Hi"}
        ]
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    print str(json.loads(content))
    assert json.loads(content) == EXPECTED

   
