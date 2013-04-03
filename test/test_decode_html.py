import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()

def _get_server_response(body, prop=None):
    url = server() + "decode_html?prop=%s" % prop
    return H.request(url, "POST", body=body, headers=CT_JSON)

def test_decode_html():
    """Should decode ", &, <, and >"""
    INPUT = {
        "subject": ['a', 'b', '&quot;&amp;', 'c; &lt;', 'd', '&gt;;e']
    }
    EXPECTED = {
        "subject": ['a', 'b', '\"&', 'c; <', 'd', '>;e']
    }

    resp, content = _get_server_response(json.dumps(INPUT), prop="subject")
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
