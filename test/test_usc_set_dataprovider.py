from server_support import server, print_error_log, H
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo

def _get_server_response(body):
    url = server() + "usc_set_dataprovider"

    return H.request(url, "POST", body=body)

def test_usc_set_dataprovider1():
    """Should set dataProvider to the string
       "University of Southern California. Libraries" for
       non chs collection records
    """
    INPUT = {
        "originalRecord": {
            "setSpec": "p15799coll104"
        },
        "dataProvider": ["a", "b", "c"]
    }
    EXPECTED = {
        "originalRecord": {
            "setSpec": "p15799coll104"
        },
        "dataProvider": "University of Southern California. Libraries"
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    print_error_log()
    assert resp["status"] == "200"
    assert_same_jsons(EXPECTED, json.loads(content))

def test_usc_set_dataprovider2():
    """Should set dataProvider to the first value in dataProvider for
       chs collection records
    """
    INPUT = {
        "originalRecord": {
            "setSpec": "p15799coll65"
        },
        "dataProvider": ["a", "b", "c"]
    }
    EXPECTED = {
        "originalRecord": {
            "setSpec": "p15799coll65"
        },
        "dataProvider": "a"
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    print_error_log()
    assert resp["status"] == "200"
    assert_same_jsons(EXPECTED, json.loads(content))

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
