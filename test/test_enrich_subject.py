import sys
from server_support import server, H
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo

url = server() + "enrich-subject?prop=subject"

def _get_server_response(body):
    return H.request(url,"POST",body=body)

def test_enrich_subject_capitalize_firs_letter():
    """Should capitalize first letter of each subject"""

    INPUT = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            "subject",
            "hi there",
            "hello"
        ]
    }
    EXPECTED = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            {"name": "Subject"},
            {"name": "Hi there"},
            {"name": "Hello"}
        ]
    }

def test_enrich_subject_one_char_string1():
    """Should not add one or two char strings to DPLA schema"""

    INPUT = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            "subject",
            "a",
            "ab",
            "hello"
        ]
    }
    EXPECTED = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            {"name": "Subject"},
            {"name": "Hello"}
        ]
    }
 
    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    print str(json.loads(content))
    assert json.loads(content) == EXPECTED

def test_enrich_subject_one_char_string2():
    """Should not include subject"""

    INPUT = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            "h",
            "hi"
        ]
    }
    EXPECTED = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject" : []
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    print str(json.loads(content))
    assert json.loads(content) == EXPECTED

def test_enrich_subject_remove_period_space():
    """Should not include subject"""

    INPUT = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            "hello there",
            "123",
            ". hi ",
            ".  hi",
            "             . hi there    ",
            "a banana"
        ]
    }
    EXPECTED = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            {"name": "Hello there"},
            {"name": "123"},
            {"name": "Hi there"},
            {"name": "A banana"}
        ]
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    print str(json.loads(content))
    assert json.loads(content) == EXPECTED

def test_remove_spaces_around_dashes():
    """Should remove spaces around dashes."""
    INPUT = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            "hello there",
            "aaa--bbb",
            "aaa --bbb",
            "aaa-- bbb",
            "aaa --  bbb",
            "aaa  --  bbb    -- ccc - - ddd -- "
        ]
    }
    EXPECTED = {
        "id": "123",
        "spatial": [
            {"name": "Asheville"},
            {"name": "North Carolina"}
        ],
        "subject": [
            {"name": "Hello there"},
            {"name": "Aaa--bbb"},
            {"name": "Aaa--bbb"},
            {"name": "Aaa--bbb"},
            {"name": "Aaa--bbb"},
            {"name": "Aaa--bbb--ccc - - ddd--"},
        ]
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    assert_same_jsons(json.dumps(EXPECTED), content)
    assert resp.status == 200


if __name__ == "__main__":
    raise SystemExit("Use nosetest")
