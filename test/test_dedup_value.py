import os
import sys
from server_support import server, print_error_log, H
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo

def _get_server_response(body, prop=None):
    url = server() + "dedup_value?prop=" + prop
    return H.request(url, "POST", body=body)


def test_dedup_value1():
    """Should remove duplicate values"""

    props = "subject,spatial,description"
    INPUT = {
        "subject": [
            "This is a subject",
            "This is a subject.",
            " this is a SuBject . ",
            "This is another subject (1780).",
            "This is another subject 1780",
            "   thiS IS anOther subject (1780)"
        ],
        "spatial": ["North Carolina", "New York"],
        "description": "A description"
    }
    EXPECTED = {
        "subject": [
            "This is a subject",
            "This is another subject (1780)."
        ],
        "spatial": ["North Carolina", "New York"],
        "description": "A description"
    }

    resp, content = _get_server_response(json.dumps(INPUT), props)
    assert resp.status == 200
    assert_same_jsons(EXPECTED, content)

def test_dedup_value2():
    """Should do nothing if prop does not exists"""

    props = "dataProvider,language"
    INPUT = {
        "subject": [
            "This is a subject",
            "This is a subject.",
            " this is a SuBject . ",
            "This is another subject (1780).",
            "This is another subject 1780",
            "   thiS IS anOther subject (1780)"
        ],
        "spatial": ["North Carolina", "New York"],
        "description": "A description"
    }

    resp, content = _get_server_response(json.dumps(INPUT), props)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)
