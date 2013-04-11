from server_support import server, H
from amara.thirdparty import json
from dict_differ import DictDiffer

url = server() + "capitalize_value"


def _get_server_response(body, prop=None):
    u = url + "?prop=" + prop if prop else url
    return H.request(u, "POST", body=body)


def test_capitalize_value():
    """Should capitalize first letter of each property"""

    INPUT = {
        "id": "123",
        "spatial": {
            "key1": "asheville",
            "key2": "north Carolina"
        },
        "subject": [
            "subject",
            "hi there",
            "hello"
        ]
    }
    EXPECTED = {
        "id": "123",
        "spatial": {
            "key1": "Asheville",
            "key2": "North Carolina"
        },
        "subject": [
            "Subject",
            "Hi there",
            "Hello"
        ]
    }
    resp, content = _get_server_response(json.dumps(INPUT), prop="spatial/key1,spatial/key2,subject")
    assert resp.status == 200
    FETCHED = json.loads(content)
    assert FETCHED == EXPECTED, DictDiffer(EXPECTED, FETCHED).diff()


if __name__ == "__main__":
    raise SystemExit("Use nosetest")
