import sys
from server_support import server, print_error_log, H
from amara.thirdparty import json

def _get_server_response(body):
    url = server() + "artstor_spatial_to_dataprovider"
    
    return H.request(url,"POST",body=body)

def test_artstor_spatial_to_dataprovider1():
    """Should split spatial on semicolon, override dataProvider with first
       spatial value, then delete sourceResource/spatial
    """

    INPUT = {
        "originalRecord": {"setSpec": "DPLADallas"},
        "sourceResource": {"spatial": "Repository: a;b;c;d;e"},
        "dataProvider": "blah"
    }
    EXPECTED = {
        "originalRecord": {"setSpec": "DPLADallas"},
        "sourceResource": {},
        "dataProvider": "a"
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_artstor_spatial_to_dataprovider2():
    """Should override dataProvider with "Repository: " spatial value, then
       keep values in spatial field that do not contain "Repository:" or
       "Accession"
    """

    INPUT = {
        "originalRecord": {"setSpec": "SSDPLAWashington"},
        "sourceResource": {"spatial": ["New York", "Repository: ArtStor",
                           "Accession number: 12345"]},
        "dataProvider": "blah"
    }
    EXPECTED = {
        "originalRecord": {"setSpec": "SSDPLAWashington"},
        "sourceResource": {"spatial": ["New York"]},
        "dataProvider": "ArtStor"
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED


if __name__ == "__main__":
    raise SystemExit("Use nosetest")
