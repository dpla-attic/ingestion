from server_support import server, print_error_log, H
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo

def _get_server_response(body):
    url = server() + "georgia_set_spec_type"

    return H.request(url, "POST", body=body)

def test_georgia_set_spec_type():
    """Should set the specType to include Book, Government Document,
       and Serial
    """
    INPUT = {
        "sourceResource": {
            "type": [
                "something Books",
                " GOVERNMENT something",
                " Something periodicals",
                " Another periodicals"
            ]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "type": [
                "something Books",
                " GOVERNMENT something",
                " Something periodicals",
                " Another periodicals"
            ],
            "specType": [
                "Book",
                "Government Document",
                "Serial"
            ]
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    print_error_log()
    assert resp["status"] == "200"
    assert_same_jsons(EXPECTED, json.loads(content))

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
