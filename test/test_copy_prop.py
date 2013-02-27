import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()

def _get_server_response(body, prop=None, to_prop=None, create=None, key=None,
    remove=None):
    url = server() + "copy_prop?prop=%s&to_prop=%s" % (prop, to_prop)
    if create:
        url = "%s&create=%s" % (url, create)
    if key:
        url = "%s&key=%s" % (url, key)
    if remove:
        url = "%s&remove=%s" % (url, remove)
    return H.request(url, "POST", body=body, headers=CT_JSON)

def test_copy_prop_rights1():
    """Should do nothing"""
    prop = "aggregatedCHO/rights"
    to_prop = "isShownAt"
    key = "rights"

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop, key=key)
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_copy_prop_rights2():
    """Should do nothing"""
    prop = "aggregatedCHO/rights"
    to_prop = "isShownAt"
    key = "rights"

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT),prop=prop,
        to_prop=to_prop, key=key)
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_copy_prop_rights3():
    """Should copy aggregatedCHO/rights to isShownAt"""
    prop = "aggregatedCHO/rights"
    to_prop = "isShownAt"
    key = "rights"

    INPUT = {
        "key1": "value1",
        "isShownAt": {
            "key1": "value1",
            "key2": "value2",
            "rights": ""
        },
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "isShownAt": {
            "key1": "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop, key=key)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_rights4():
    """Should copy aggregatedCHO/rights to isShownAt then remove
       aggregatedCHO/rights
    """
    prop = "aggregatedCHO/rights"
    to_prop = "isShownAt"
    key = "rights"
    remove = True

    INPUT = {
        "key1": "value1",
        "isShownAt": {
            "key1": "value1",
            "key2": "value2",
            "rights": ""
        },
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "isShownAt": {
            "key1": "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop, key=key, remove=remove)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_rights5():
    """Should copy aggregatedCHO/rights to aggregatedCHO/hasView items"""
    prop = "aggregatedCHO/rights"
    to_prop = "aggregatedCHO/hasView"
    key = "rights"

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights",
            "hasView": [
                {
                    "key1": "value1",
                    "rights": ""
                },
                {
                    "key1": "value1",
                    "rights": ""
                }
            ]
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights",
            "hasView": [
                {
                    "key1": "value1",
                    "rights": "These are the rights"
                },
                {
                    "key1": "value1",
                    "rights": "These are the rights"
                }
            ]
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop, key=key)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_rights6():
    """Should copy aggregatedCHO/rights to aggregatedCHO/hasView items
       then remove aggregatedCHO/rights
    """
    prop = "aggregatedCHO/rights"
    to_prop = "aggregatedCHO/hasView"
    key = "rights"
    remove = True

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights",
            "hasView": [
                {
                    "key1": "value1",
                    "rights": ""
                },
                {
                    "key1": "value1",
                    "rights": ""
                }
            ]
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "hasView": [
                {
                    "key1": "value1",
                    "rights": "These are the rights"
                },
                {
                    "key1": "value1",
                    "rights": "These are the rights"
                }
            ]
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop, key=key, remove=remove)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_contributor1():
    """Should copy aggregatedCHO/contributor to dataProvider"""
    prop = "aggregatedCHO/contributor"
    to_prop = "dataProvider"
    create = True

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        }
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        },
        "dataProvider": "Natural History Museum Library, London"
    }
       
    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop, create=create)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_contributor2():
    """Should copy aggregatedCHO/contributor to dataProvider then remove
        aggregatedCHO/contributor
    """
    prop = "aggregatedCHO/contributor"
    to_prop = "dataProvider"
    create = True
    remove = True

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        }
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1"
        },
        "dataProvider": "Natural History Museum Library, London"
    }
       
    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop, create=create, remove=remove)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_contributor3():
    """Should overwrite dataProvider (create = False)"""
    prop = "aggregatedCHO/contributor"
    to_prop = "dataProvider"

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        },
        "dataProvider" : "Taken from source"
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        },
        "dataProvider": "Natural History Museum Library, London"
    }
       
    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_contributor4():
    """Should overwrite dataProvider (create = False) then remove
       aggregatedCHO/contributor
    """
    prop = "aggregatedCHO/contributor"
    to_prop = "dataProvider"
    remove = True

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        },
        "dataProvider" : "Taken from source"
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1"
        },
        "dataProvider": "Natural History Museum Library, London"
    }
       
    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop, remove=remove)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED
  
def test_copy_prop_contributor5():
    """Should overwrite dataProvider (create = True)"""
    prop = "aggregatedCHO/contributor"
    to_prop = "dataProvider"
    create = True

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        },
        "dataProvider" : "Taken from source"
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        },
        "dataProvider": "Natural History Museum Library, London"
    }
       
    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop, create=create)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_contributor6():
    """Should overwrite dataProvider (create = True) then remove
       aggregatedCHO/contributor
    """
    prop = "aggregatedCHO/contributor"
    to_prop = "dataProvider"
    create = True
    remove = True

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        },
        "dataProvider" : "Taken from source"
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1"
        },
        "dataProvider": "Natural History Museum Library, London"
    }
       
    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop, create=create, remove=remove)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_list_to_list():
    """Should join prop into to_prop"""
    prop = "aggregatedCHO/from_list"
    to_prop = "aggregatedCHO/to_list"

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "from_list": ["1", "2", "3"],
            "to_list" : ["a", "b", "c"],
            "key2": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "from_list": ["1", "2", "3"],
            "to_list" : ["a", "b", "c", "1", "2", "3"],
            "key2": "value2"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) ==  EXPECTED

def test_copy_prop_string_to_list():
    """Should append to to_prop"""
    prop = "aggregatedCHO/from_string"
    to_prop = "aggregatedCHO/to_list"

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "from_string": "stringy",
            "to_list" : ["a", "b", "c"],
            "key2": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "from_string": "stringy",
            "to_list" : ["a", "b", "c", "stringy"],
            "key2": "value2"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) ==  EXPECTED

def test_copy_prop_dict_to_list():
    """Should append to to_prop"""
    prop = "aggregatedCHO/from_dict"
    to_prop = "aggregatedCHO/to_list"

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "from_dict": {"key1": "value1"},
            "to_list" : ["a", "b", "c"],
            "key2": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "from_dict": {"key1": "value1"},
            "to_list" : ["a", "b", "c", {"key1": "value1"}],
            "key2": "value2"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) ==  EXPECTED

def test_copy_prop_to_prop_dict_no_key():
    """Should overwrite to_prop with prop"""
    prop = "aggregatedCHO/from_dict"
    to_prop = "aggregatedCHO/to_dict"

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "from_dict": {"key1": "value1"},
            "to_dict" : {"key2": "value2"},
            "key2": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1": "value1",
            "from_dict": {"key1": "value1"},
            "to_dict" : {"key1": "value1"},
            "key2": "value2"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) ==  EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
