import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()

def _get_server_response(body, prop=None, to_prop=None, skip_if_exists=None):
    url = server() + "copy_prop?prop=%s&to_prop=%s" % (prop, to_prop)
    if skip_if_exists:
        url = "%s&skip_if_exists=%s" % (url, skip_if_exists)
    return H.request(url, "POST", body=body, headers=CT_JSON)

def test_copy_prop1():
    """Should do nothing since from_prop does not exists"""
    prop = "sourceResource/rights"
    to_prop = "hasView/rights"

    INPUT = {
        "sourceResource": {
            "key1" : "value1",
            "key2": "value2"
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
                                        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_copy_prop2():
    """Should do nothing since to_prop parent property does not exist"""
    prop = "sourceResource/rights"
    to_prop = "hasView/rights"

    INPUT = {
        "sourceResource": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights"
        }
    }
    EXPECTED = {
        "sourceResource": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights"
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT),prop=prop,
                                        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_copy_prop_create_to_prop():
    """Should create to_prop and set its value to that of from_prop"""
    prop = "sourceResource/rights"
    to_prop = "hasView/rights"

    INPUT = {
        "sourceResource": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "hasView": {
            "@id": "id"
        }
    }
    EXPECTED = {
        "sourceResource": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "hasView": {
            "@id": "id",
            "rights": "These are the rights"
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT),prop=prop,
                                        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_str_to_str():
    """Should extend to_prop"""
    prop = "note"
    to_prop = "sourceResource/description"

    INPUT = {
        "note": "This is a note",
        "sourceResource": {
            "description": "This is a description"
        }
    }
    EXPECTED = {
        "note": "This is a note",
        "sourceResource": {
            "description": ["This is a description", "This is a note"]
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT),prop=prop,
                                        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_str_to_list():
    """Should extend to_prop"""
    prop = "note"
    to_prop = "sourceResource/description"

    INPUT = {
        "note": "This is a note",
        "sourceResource": {
            "description": [
                "This is a description",
                "This is another description"
            ]
        }
    }
    EXPECTED = {
        "note": "This is a note",
        "sourceResource": {
            "description": [
                "This is a description",
                "This is another description",
                "This is a note"
            ]
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT),prop=prop,
                                        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_list_to_str():
    """Should extend to_prop"""
    prop = "note"
    to_prop = "sourceResource/description"

    INPUT = {
        "note": ["This is a note", "This is another note"],
        "sourceResource": {
            "description": "This is a description"
        }
    }
    EXPECTED = {
        "note": ["This is a note", "This is another note"],
        "sourceResource": {
            "description": [
                "This is a description",
                "This is a note",
                "This is another note"
            ]
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT),prop=prop,
                                        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_list_to_list():
    """Should extend to_prop"""
    prop = "note"
    to_prop = "sourceResource/description"

    INPUT = {
        "note": ["This is a note", "This is another note"],
        "sourceResource": {
            "description": [
                "This is a description",
                "This is another description"
            ]
        }
    }
    EXPECTED = {
        "note": ["This is a note", "This is another note"],
        "sourceResource": {
            "description": [
                "This is a description",
                "This is another description",
                "This is a note",
                "This is another note"
            ]
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT),prop=prop,
                                        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_skip():
    """Should do nothing since to_prop exists and skip_if_exists is True"""
    prop = "note"
    to_prop = "sourceResource/description"

    INPUT = {
        "note": "This is a note",
        "sourceResource": {
            "description": "This is a description"
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT),prop=prop,
                                        to_prop=to_prop, skip_if_exists=True)
    assert resp.status == 200
    assert json.loads(content) == INPUT

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
