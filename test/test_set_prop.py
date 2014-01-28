import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json
from urllib import urlencode

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()

def _get_server_response(body, action="set", **kwargs):
    url = server() + "%s_prop?%s" % (action, urlencode(kwargs))

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

def test_set_prop6():
    """Should parse value as JSON before setting the prop"""
    prop = "provider"
    value = '{"@id": "http://dp.la/api/contributor/scdl-clemson",' + \
            '"name": "South Carolina Digital Library"}'
    INPUT = {
        "key1": "value1"
    }
    EXPECTED = {
        "key1": "value1",
        "provider": {
            "@id": "http://dp.la/api/contributor/scdl-clemson",
            "name": "South Carolina Digital Library"
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
                                        value=value, _dict=True)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_set_prop7():
    """Should not set prop since condition_prop's value != condition_value"""
    prop = "provider"
    value = "provider"
    condition_prop = "ingestType"
    condition_value = "item"
    INPUT = {
        "key1": "value1",
        "ingestType": "collection"
    }
    EXPECTED = {
        "key1": "value1",
        "ingestType": "collection"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
                                        value=value,
                                        condition_prop=condition_prop,
                                        condition_value=condition_value)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_set_prop8():
    """Should set prop since condition_prop's value == condition_value"""
    prop = "provider"
    value = "provider"
    condition_prop = "ingestType"
    condition_value = "item"
    INPUT = {
        "key1": "value1",
        "ingestType": "item"
    }
    EXPECTED = {
        "key1": "value1",
        "ingestType": "item",
        "provider": "provider"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
                                        value=value,
                                        condition_prop=condition_prop,
                                        condition_value=condition_value)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_unset_prop1():
    """Should unset prop"""
    action = "unset"
    prop = "sourceResource/rights"

    INPUT = {
        "_id": "12345",
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
            "rights": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "_id": "12345",
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
        "_id": "12345",
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1",
            "rights": "20010983784"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "_id": "12345",
        "key1": "value1",
        "sourceResource": {
            "key1" : "value1"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), action=action,
        prop=prop, condition=condition)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_unset_prop3():
    """Should not unset prop since condition is not met"""
    action = "unset"
    prop = "sourceResource/rights"
    condition = "is_digit"

    INPUT = {
        "_id": "12345",
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
        "_id": "12345",
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
        "_id": "12345",
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

def test_unset_prop6():
    """Should unset prop since conditions are met for hathi_exclude"""
    action = "unset"
    prop = "_id"
    condition = "hathi_exclude"
    condition_prop = "dataProvider"

    INPUT = {
        "_id": "12345",
        "dataProvider": ["Hathitrust", "Minnesota Digital Library"]
    }
    EXPECTED = {
        "dataProvider": ["Hathitrust", "Minnesota Digital Library"]
    }

    resp, content = _get_server_response(json.dumps(INPUT), action=action,
        prop=prop, condition=condition, condition_prop=condition_prop)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED
   
def test_unset_prop7():
    """Should not unset prop since condition is not met for hathi_exclude"""
    action = "unset"
    prop = "_id"
    condition = "hathi_exclude"
    condition_prop = "dataProvider"

    INPUT = {
        "_id": "12345",
        "dataProvider": ["Hathitrust"]
    }

    resp, content = _get_server_response(json.dumps(INPUT), action=action,
        prop=prop, condition=condition, condition_prop=condition_prop)
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_unset_prop8():
    """Should not unset prop since condition is not met with dataProvider"""
    action = "unset"
    prop = "_id"
    condition = "hathi_exclude"
    condition_prop = "dataProvider,sourceResource/type"

    INPUT = {
        "_id": "12345",
        "dataProvider": "Hathitrust",
        "sourceResource": {
            "type": "image"
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT), action=action,
        prop=prop, condition=condition, condition_prop=condition_prop)
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_unset_prop_finding_aid():
    """Should unset _id since title starts with 'Finding Aid'"""
    action = "unset"
    prop = "_id"
    condition = "finding_aid_title"
    condition_prop = "sourceResource/title"

    INPUT = {
        "_id": "12345",
        "sourceResource": {
            "title": "Finding Aid: George Williams Papers"
        }
    }
    EXPECTED = {
        "sourceResource": {
            "title": "Finding Aid: George Williams Papers"
        }
    }


    resp, content = _get_server_response(json.dumps(INPUT), action=action,
        prop=prop, condition=condition, condition_prop=condition_prop)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
