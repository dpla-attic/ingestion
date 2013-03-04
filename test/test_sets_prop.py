import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()

def _get_server_response(body, prop=None, value=None):
    url = server() + "sets_prop?prop=%s" % prop
    if value:
        url = "%s&value=%s" % (url, value)
    return H.request(url, "POST", body=body, headers=CT_JSON)

def test_sets_prop_set():
    """Should set prop to value"""
    prop = "aggregatedCHO/rights"
    value = "rights"

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "rights": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "rights": "rights"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        value=value)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_sets_prop_unset():
    """Should unset prop"""
    prop = "aggregatedCHO/rights"

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "rights": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop)
    assert resp.status == 200
    print >> sys.stderr, str(json.loads(content))
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
