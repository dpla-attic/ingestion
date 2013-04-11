import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()

def _get_server_response(body, action="set", prop=None, value=None,
                         condition_prop=None, condition=None):
    url = server() + "%s_prop?prop=%s" % (action, prop)
    if value:
        url = "%s&value=%s" % (url, value)
    if condition_prop:
        url = "%s&condition_prop=%s" % (url, condition_prop)
    if condition:
        url = "%s&condition=%s" % (url, condition)
    return H.request(url, "POST", body=body, headers=CT_JSON)

def test_set_prop1():
    """Should set prop to value"""
    prop = "sourceResource/rights"
    value = "rights"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
            "rights": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
            "rights": "rights"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        value=value)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_set_prop2():
    """Should create the prop and set its value"""
    prop = "sourceResource/rights"
    value = "rights"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
            "rights": "rights"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        value=value)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_set_prop3():
    """Should do nothing, since no value was supplid"""
    prop = "sourceResource/rights"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
            "rights": "value2"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop)
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_set_prop4():
    """Should do nothing, since condition_prop does not exist"""
    prop = "sourceResource/rights"
    value = "rights"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        value=value, condition_prop=prop)
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_set_prop5():
    """Should set prop to value, since condition_prop exists"""
    prop = "sourceResource/rights"
    value = "rights"
    condition_prop = "sourceResource"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
            "rights": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
            "rights": "rights"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        value=value, condition_prop="sourceResource")
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_unset_prop1():
    """Should unset prop"""
    action = "unset"
    prop = "sourceResource/rights"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
            "rights": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), action=action,
        prop=prop)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_unset_prop2():
    """Should unset prop since condition is met"""
    action = "unset"
    prop = "sourceResource/rights"
    condition = "is_digit"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
            "rights": "20010983784"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), action=action,
        prop=prop, condition=condition)
    assert resp.status == 200
    print_error_log()
    assert json.loads(content) == EXPECTED

def test_unset_prop3():
    """Should not unset prop since condition is not met"""
    action = "unset"
    prop = "sourceResource/rights"
    condition = "is_digit"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
            "rights": "value2"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), action=action,
        prop=prop, condition=condition)
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_unset_prop4():
    """Should do nothing to INPUT but catch keyError since condition is not
       in CONDITIONS
    """
    action = "unset"
    prop = "sourceResource/rights"
    condition = "is_digits"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
            "rights": "value2"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), action=action,
        prop=prop, condition=condition)
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_unset_prop5():
    """Should do nothing since prop does not exist"""
    action = "unset"
    prop = "sourceResource/rights"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), action=action,
        prop=prop)
    assert resp.status == 200
    assert json.loads(content) == INPUT

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
