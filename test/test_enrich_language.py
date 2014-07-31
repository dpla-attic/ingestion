import sys
from server_support import server, H
from amara.thirdparty import json


def _get_server_response(body, prop=None):
    """
    Returns response from server using provided url.
    """
    url = server() + "enrich_language"
    return H.request(url, "POST", body=body)

def test_language_name_regex():
    INPUT = {
        "sourceResource": {
            "language": "mUlTiple languAges"
        }
    }
    EXPECTED = {
        "sourceResource": {
            "language": [
                {
                    "iso639_3": "mul",
                    "name": "Multiple languages"
                }
            ]
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    print >> sys.stderr, EXPECTED
    print >> sys.stderr, json.loads(content)
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
