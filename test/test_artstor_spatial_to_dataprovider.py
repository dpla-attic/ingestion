import sys
from server_support import server, print_error_log, H
from amara.thirdparty import json

url = server() + "artstor_spatial_to_dataprovider"

def _get_server_response(body):
    return H.request(url,"POST",body=body)

def test_artstor_spatial_to_dataprovider():
    """Should split spatial on semicolon and copy first value"""

    INPUT = {
        "sourceResource": {"spatial": "a;b;c;d;e"}
    }
    EXPECTED = {
        "sourceResource": {},
        "dataProvider": "a"
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
