import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()

def _get_server_response(body, prop=None, to_prop=None, create=None, key=None,
    remove=None, no_replace=None):
    url = server() + "copy_prop?prop=%s&to_prop=%s" % (prop, to_prop)
    if create:
        url = "%s&create=%s" % (url, create)
    if key:
        url = "%s&key=%s" % (url, key)
    if remove:
        url = "%s&remove=%s" % (url, remove)
    if no_replace:
        url = "%s&no_replace=%s" % (url, no_replace)
    return H.request(url, "POST", body=body, headers=CT_JSON)

def test_copy_prop_rights1():
    """Should do nothing"""
    prop = "sourceResource/rights"
    to_prop = "isShownAt"
    key = "rights"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
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
    prop = "sourceResource/rights"
    to_prop = "isShownAt"
    key = "rights"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
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
    """Should copy sourceResource/rights to isShownAt"""
    prop = "sourceResource/rights"
    to_prop = "isShownAt"
    key = "rights"

    INPUT = {
        "key1": "value1",
        "isShownAt": {
            "key1": "value1",
            "key2": "value2",
            "rights": ""
        },
        "sourceResource": {
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
        "sourceResource": {
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
    """Should copy sourceResource/rights to isShownAt then remove
       sourceResource/rights
    """
    prop = "sourceResource/rights"
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
        "sourceResource": {
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
        "sourceResource": {
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
    """Should copy sourceResource/rights to sourceResource/hasView items"""
    prop = "sourceResource/rights"
    to_prop = "sourceResource/hasView"
    key = "rights"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
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
        "sourceResource": {
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
    """Should copy sourceResource/rights to sourceResource/hasView items
       then remove sourceResource/rights
    """
    prop = "sourceResource/rights"
    to_prop = "sourceResource/hasView"
    key = "rights"
    remove = True

    INPUT = {
        "key1": "value1",
        "sourceResource": {
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
        "sourceResource": {
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
    """Should copy sourceResource/contributor to dataProvider"""
    prop = "sourceResource/contributor"
    to_prop = "dataProvider"
    create = True

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        }
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
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
    """Should copy sourceResource/contributor to dataProvider then remove
        sourceResource/contributor
    """
    prop = "sourceResource/contributor"
    to_prop = "dataProvider"
    create = True
    remove = True

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        }
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
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
    prop = "sourceResource/contributor"
    to_prop = "dataProvider"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        },
        "dataProvider" : "Taken from source"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
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
       sourceResource/contributor
    """
    prop = "sourceResource/contributor"
    to_prop = "dataProvider"
    remove = True

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        },
        "dataProvider" : "Taken from source"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
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
    prop = "sourceResource/contributor"
    to_prop = "dataProvider"
    create = True

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        },
        "dataProvider" : "Taken from source"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
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
       sourceResource/contributor
    """
    prop = "sourceResource/contributor"
    to_prop = "dataProvider"
    create = True
    remove = True

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1": "value1",
            "contributor": "Natural History Museum Library, London"
        },
        "dataProvider" : "Taken from source"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
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
    prop = "sourceResource/from_list"
    to_prop = "sourceResource/to_list"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1": "value1",
            "from_list": ["1", "2", "3"],
            "to_list" : ["a", "b", "c"],
            "key2": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
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
    prop = "sourceResource/from_string"
    to_prop = "sourceResource/to_list"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1": "value1",
            "from_string": "stringy",
            "to_list" : ["a", "b", "c"],
            "key2": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
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
    prop = "sourceResource/from_dict"
    to_prop = "sourceResource/to_list"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1": "value1",
            "from_dict": {"key1": "value1"},
            "to_list" : ["a", "b", "c"],
            "key2": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
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
    prop = "sourceResource/from_dict"
    to_prop = "sourceResource/to_dict"

    INPUT = {
        "key1": "value1",
        "sourceResource": {
            "key1": "value1",
            "from_dict": {"key1": "value1"},
            "to_dict" : {"key2": "value2"},
            "key2": "value2"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "sourceResource": {
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

def test_copy_prop_no_replace1():
    """Should create list of prop string and append to_prop"""
    prop = "aggregatedCHO/source"
    to_prop = "aggregatedCHO/description"
    no_replace = True

    INPUT = {
        "aggregatedCHO": {
            "description" : "Description string.",
            "source": "Source string."
        }
    }
    EXPECTED = {
        "aggregatedCHO": {
            "description": [
                "Description string.",
                "Source string."
            ],
            "source": "Source string."
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop, no_replace=no_replace)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_no_replace2():
    """Should create list of prop string and append to_prop"""
    prop = "aggregatedCHO/source"
    to_prop = "aggregatedCHO/description"
    no_replace = True

    INPUT = {
        "aggregatedCHO": {
            "description" : "Description string.",
            "source": ["Source string1.", "Source string2."]
        }
    }
    EXPECTED = {
        "aggregatedCHO": {
            "description": [
                "Description string.",
                "Source string1.",
                "Source string2."
            ],
            "source": ["Source string1.", "Source string2."]
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
        to_prop=to_prop, no_replace=no_replace)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_no_replace3():
    """Should create list of prop string and append to_prop"""
    prop1 = "aggregatedCHO/source1"
    prop2 = "aggregatedCHO/source2"
    to_prop = "aggregatedCHO/description"
    no_replace = True

    INPUT = {
        "aggregatedCHO": {
            "description" : "Description string.",
            "source1": "Source1 string1.",
            "source2": ["Source2 string1.", "Source2 string2."]
        }
    }
    EXPECTED1 = {
        "aggregatedCHO": {
            "description": [
                "Description string.",
                "Source1 string1."
            ],
            "source1": "Source1 string1.",
            "source2": ["Source2 string1.", "Source2 string2."]
        }
    }
    EXPECTED2 = {
        "aggregatedCHO": {
            "description": [
                "Description string.",
                "Source1 string1.",
                "Source2 string1.",
                "Source2 string2."
            ],
            "source1": "Source1 string1.",
            "source2": ["Source2 string1.", "Source2 string2."]
        }
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop1,
        to_prop=to_prop, no_replace=no_replace)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED1

    resp,content = _get_server_response(json.dumps(EXPECTED1), prop=prop2,
        to_prop=to_prop, no_replace=no_replace)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED2

def test_copy_prop_to_prop_create_dict_key1():
    """Should copy to_prop into new dict with key"""
    prop1 = "key1"
    prop2 = "aggregatedCHO/key2"
    to_prop = "aggregatedCHO/to_dict"
    key1 = "key1"
    key2 = "key2" 
    create = True

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key2": "value2",
            "key3": "value3"
        },
        "key4": "value4"
    }
    EXPECTED1 = {
        "key1": "value1",
        "aggregatedCHO": {
            "key2": "value2",
            "key3": "value3",
            "to_dict" : {"key1": "value1"}
        },
        "key4": "value4"
    }
    EXPECTED2 = {
        "key1": "value1",
        "aggregatedCHO": {
            "key2": "value2",
            "key3": "value3",
            "to_dict" : {
                "key1": "value1",
                "key2": "value2"
            }
        },
        "key4": "value4"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop1,
        to_prop=to_prop, key=key1, create=create)
    assert resp.status == 200
    assert json.loads(content) ==  EXPECTED1

    resp,content = _get_server_response(json.dumps(EXPECTED1), prop=prop2,
        to_prop=to_prop, key=key2, create=create)
    assert resp.status == 200
    assert json.loads(content) ==  EXPECTED2

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
