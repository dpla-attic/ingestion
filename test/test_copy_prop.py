import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()

def _get_server_response(body,prop=None,to_prop=None,create=None,key=None):
    url = server() + "copy_prop?prop=%s&to_prop=%s" % (prop, to_prop)
    if create:
        url = "%s&create=%s" % (url,create)
    if key:
        url = "%s&key=%s" % (url,key)
    return H.request(url,"POST",body=body,headers=CT_JSON)

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

    resp,content = _get_server_response(json.dumps(INPUT),prop=prop, \
        to_prop=to_prop,key=key)
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

    resp,content = _get_server_response(json.dumps(INPUT),prop=prop, \
        to_prop=to_prop,key=key)
    print_error_log()
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_copy_prop_rights3():
    """Should copy rights to isShownAt"""
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

    resp,content = _get_server_response(json.dumps(INPUT),prop=prop, \
        to_prop=to_prop,key=key)
    print_error_log()
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_rights4():
    """Should copy rights to aggregatedCHO/hasView items"""
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

    resp,content = _get_server_response(json.dumps(INPUT),prop=prop, \
        to_prop=to_prop,key=key)
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
       
    resp,content = _get_server_response(json.dumps(INPUT),prop=prop, \
        to_prop=to_prop,create=create)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_prop_contributor2():
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
       
    resp,content = _get_server_response(json.dumps(INPUT),prop=prop, \
        to_prop=to_prop)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED
   
def test_copy_prop_contributor3():
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
       
    resp,content = _get_server_response(json.dumps(INPUT),prop=prop, \
        to_prop=to_prop,create=create)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
