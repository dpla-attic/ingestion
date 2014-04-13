from server_support import server, H, print_error_log 
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo

def _get_server_response(body, default=None):
    url = server() + "enrich-type"
    if default is not None:
        url += "?default=" + default
    return H.request(url, "POST", body=body)

def test_remove_type():
    """Should remove type"""
    INPUT = {
        "id": "123",
        "sourceResource": {
            "type": "bananas"
        }
    }
    EXPECTED = {
        "id": "123",
        "sourceResource": {}
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

def test_default_type():
    """Should set type to default value"""
    INPUT = {
        "id": "123",
        "sourceResource": {
            "type": "bananas"
        }
    }
    EXPECTED = {
        "id": "123",
        "sourceResource": {
            "type": "image"
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT), default="image")
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

def test_type_for_phys_keyword():
    INPUT = {
        "id": "123",
        "sourceResource": {
            "type": "bananas",
            "format": "Holiday Card"
        }
    }
    EXPECTED = {
        "id": "123",
        "sourceResource": {
            "type": "image",
            "format": "Holiday Card"
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

def test_type_for_type_keyword():
    INPUT = {
        "id": "123",
        "sourceResource": {
            "type": "Photograph"
        }
    }
    EXPECTED = {
        "id": "123",
        "sourceResource": {
            "type": "image"
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
