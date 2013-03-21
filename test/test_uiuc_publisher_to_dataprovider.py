import sys
from server_support import server, print_error_log, H
from amara.thirdparty import json

url = server() + "uiuc_publisher_to_dataprovider"

def _get_server_response(body):
    return H.request(url,"POST",body=body)

def test_uiuc_publisher_to_dataprovider1():
    """Should do nothing"""

    INPUT = {
        "sourceResource": {
            "key": "value"
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_uiuc_publisher_to_dataprovider2():
    """Should do nothing"""

    INPUT = {
        "dataProvider": "UIUC Source",
        "sourceResource": {
            "publisher": "UIUC Provider"
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_uiuc_publisher_to_dataprovider1():
    """Should copy publisher to dataProvider and remove publisher"""

    INPUT = {
        "sourceResource": {
            "publisher": "UIUC Provider"
        }
    }
    EXPECTED = {
        "dataProvider": "UIUC Provider",
        "sourceResource": {}
    }

    resp,content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
