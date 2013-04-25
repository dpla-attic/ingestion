import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()

def _get_server_response(body, prop=None, old=None, new=None):
    url = server() + "replace_substring"
    if prop:
        url = "%s?prop=%s" % (url, prop)
    if old:
        url = "%s&old=%s" % (url, old)
    if new:
        url = "%s&new=%s" % (url, new)
    return H.request(url, "POST", body=body, headers=CT_JSON)

def test_replace_string1():
    """Should do nothing since old/new is not set"""
    prop = "isShownAt"

    INPUT = {
        "isShownAt": "http://74.126.224.122/luna/servlet/detail/RUMSEY~8~1~107~10001"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop)
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_replace_string2():
    """Should do nothing since old not in prop"""
    prop = "isShownAt"
    old = "bananas"
    new = "apples"

    INPUT = {
        "isShownAt": "http://74.126.224.122/luna/servlet/detail/RUMSEY~8~1~107~10001"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop, old=old,
                                        new=new)
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_replace_string3():
    """Should replace old with new"""
    prop = "isShownAt"
    old = "74%2E126%2E224%2E122"
    new = "www%2Edavidrumsey%2Ecom"

    INPUT = {
        "isShownAt": "http://74.126.224.122/luna/servlet/detail/RUMSEY~8~1~107~10001"
    }
    EXPECTED = {
        "isShownAt": "http://www.davidrumsey.com/luna/servlet/detail/RUMSEY~8~1~107~10001"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop, old=old,
                                        new=new)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
