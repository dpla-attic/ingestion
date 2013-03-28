import os
import sys
import re
import hashlib
from server_support import server, print_error_log, get_thumbs_root, H
from dplaingestion.akamod.download_test_image import image_png, image_jpg
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo
from nose.tools import nottest

BASIC_URL = server() + "mwdl_cleanup_field"


def _get_server_response(body, prop=None):
    """
    Returns response from server using provided url.
    """
    import urllib
    url = BASIC_URL

    if prop:
        url = url + "?prop=" + urllib.quote(prop)

    print "Calling URL = [%s]" % url
    return H.request(url, "POST", body=body)


def test_cleanup_value_for_string():
    INPUT = {
        "aaa": {
            "bbb":
                "Fisker, Kay--Architect--Danish--Male",
        }
    }
    EXPECTED_OUTPUT = {
        "aaa": {
            "bbb":
                "Fisker, Kay",
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT), "aaa/bbb")
    print_error_log()
    assert resp["status"].startswith("2")
    assert_same_jsons(EXPECTED_OUTPUT, content)


def test_cleanup_value_for_list():
    INPUT = {
        "aaa": {
            "bbb": [
                "Fisker, Kay--Architect--Danish--Male",
                "Fisker, Kay--Architect--Danish--Male",
                "bbb",
                "ccc"
            ]
        }
    }
    EXPECTED_OUTPUT = {
        "aaa": {
            "bbb": [
                "Fisker, Kay",
                "Fisker, Kay",
                "bbb",
                "ccc"
            ]
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT), "aaa/bbb")
    print_error_log()
    assert resp["status"].startswith("2")
    assert_same_jsons(EXPECTED_OUTPUT, content)


def test_default_prop_value():
    """Should convert default field only."""
    INPUT = {
        "sourceResource": {
            "creator": [
                "Fisker, Kay--Architect--Danish--Male",
                "Fisker, Kay--Architect--Danish--Male",
                "bbb",
                "ccc"
            ],
            "aaa": [
                "Fisker, Kay--Architect--Danish--Male",
                "Fisker, Kay--Architect--Danish--Male",
                "bbb",
                "ccc"
            ]
        }
    }
    EXPECTED_OUTPUT = {
        "sourceResource": {
            "creator": [
                "Fisker, Kay",
                "Fisker, Kay",
                "bbb",
                "ccc"
            ],
            "aaa": [
                "Fisker, Kay--Architect--Danish--Male",
                "Fisker, Kay--Architect--Danish--Male",
                "bbb",
                "ccc"
            ]
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    print_error_log()
    assert resp["status"].startswith("2")
    assert_same_jsons(EXPECTED_OUTPUT, content)


def test_suppress_creator_value_for_string():
    """Should remove creator value if the is 'creator'."""
    INPUT = {
        "sourceResource": {
            "creator": "creator",
            "aaa": [
                "Fisker, Kay--Architect--Danish--Male",
                "Fisker, Kay--Architect--Danish--Male",
                "bbb",
                "ccc"
            ]
        }
    }
    EXPECTED_OUTPUT = {
        "sourceResource": {
            "aaa": [
                "Fisker, Kay--Architect--Danish--Male",
                "Fisker, Kay--Architect--Danish--Male",
                "bbb",
                "ccc"
            ]
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    print_error_log()
    assert resp["status"].startswith("2")
    assert_same_jsons(EXPECTED_OUTPUT, content)


def test_suppress_creator_value_for_list():
    """Should remove creator value if the is 'creator'."""
    INPUT = {
        "sourceResource": {
            "creator": [
                "creator",
                "Fisker, Kay--Architect--Danish--Male",
                "Fisker, Kay--Architect--Danish--Male",
                "bbb",
                "ccc"
            ],
            "aaa": [
                "Fisker, Kay--Architect--Danish--Male",
                "Fisker, Kay--Architect--Danish--Male",
                "bbb",
                "ccc"
            ]
        }
    }
    EXPECTED_OUTPUT = {
        "sourceResource": {
            "creator": [
                "Fisker, Kay",
                "Fisker, Kay",
                "bbb",
                "ccc"
            ],
            "aaa": [
                "Fisker, Kay--Architect--Danish--Male",
                "Fisker, Kay--Architect--Danish--Male",
                "bbb",
                "ccc"
            ]
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    print_error_log()
    assert resp["status"].startswith("2")
    assert_same_jsons(EXPECTED_OUTPUT, content)
