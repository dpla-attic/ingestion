from server_support import server, print_error_log, H
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo

def _get_server_response(body, prop, values=None):
    url = server() + "remove_list_values?prop=%s" % prop
    if values:
        url += "&values=" + values

    return H.request(url, "POST", body=body)

def test_remove_list_values1():
    """Should do nothing since prop is not list"""
    prop = "sourceResource/type"
    values = "d,e,f"
    INPUT = {
        "sourceResource": {
            "type": "text"
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT), prop, values)
    assert resp["status"] == "200"
    assert_same_jsons(INPUT, json.loads(content))

def test_remove_list_values2():
    """Should do nothing since values not in prop"""
    prop = "sourceResource/type"
    values = "d,e,f"
    INPUT = {
        "sourceResource": {
            "type": ["a", "b", "c"]
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT), prop, values)
    assert resp["status"] == "200"
    assert_same_jsons(INPUT, json.loads(content))

def test_remove_list_values3():
    """Should do nothing since values not supplied"""
    prop = "sourceResource/type"
    INPUT = {
        "sourceResource": {
            "type": "text"
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT), prop)
    assert resp["status"] == "200"
    assert_same_jsons(INPUT, json.loads(content))

def test_remove_list_values4():
    """Should remove values from prop"""
    prop = "sourceResource/type"
    values = "d,e,f"
    INPUT = {
        "sourceResource": {
            "type": ["a", "b", "d", "e", "e", "c"]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "type": ["a", "b", "c"]
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT), prop, values)
    assert resp["status"] == "200"
    assert_same_jsons(EXPECTED, json.loads(content))

def test_remove_list_values5():
    """Should prop if prop is empty after removal of values"""
    prop = "sourceResource/type"
    values = "d,e,f"
    INPUT = {
        "sourceResource": {
            "type": ["d", "e", "f", "d"]
        }
    }
    EXPECTED = {
        "sourceResource": {
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT), prop, values)
    assert resp["status"] == "200"
    assert_same_jsons(EXPECTED, json.loads(content))

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
