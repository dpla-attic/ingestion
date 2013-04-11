import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json
from dict_differ import assert_same_jsons

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()

def _get_server_response(body):
    url = server() + "set_type_from_physical_format"
    return H.request(url, "POST", body=body, headers=CT_JSON)

def test_set_type_from_physical_format1():
    """Should not set type"""
    INPUT = {
        "sourceResource": {
            "subject": "a"
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(INPUT, content)

def test_set_type_from_physical_format2():
    """Should not set type"""
    INPUT = {
        "sourceResource": {
            "format": ["Textile", "frame", "Photographs", "still image",
                       "prints", "magazines", "letters", "Sound Recording",
                       "Audio", "online Collections", "finding aid",
                       "Online Exhibit"],
            "type": ["sound"]
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(INPUT, content)

def test_set_type_from_physical_format3():
    """Should not set type"""
    INPUT = {
        "sourceResource": {
            "format": ["format"]
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(INPUT, content)

def test_set_type_from_physical_format4():
    """Should set type"""
    INPUT = {
        "sourceResource": {
            "format": ["Textile", "frame", "Photographs", "still image",
                       "prints", "magazines", "letters", "Sound Recording",
                       "Audio", "online Collections", "finding aid",
                       "Online Exhibit"]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "format": ["Textile", "frame", "Photographs", "still image",
                       "prints", "magazines", "letters", "Sound Recording",
                       "Audio", "online Collections", "finding aid",
                       "Online Exhibit"],
            "type": ["physical object", "image", "text", "sound",
                     "collection", "interactive resource"]
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, content)

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
