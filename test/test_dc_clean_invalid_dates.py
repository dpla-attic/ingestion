import os
import sys
import re
import hashlib
from server_support import server, print_error_log, get_thumbs_root, H
from dplaingestion.akamod.download_test_image import image_png, image_jpg
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo
from nose.tools import nottest
from urllib import quote


BASIC_URL = server() + "dc_clean_invalid_dates"


def _get_server_response(body, param=None):

    import urllib
    url = BASIC_URL

    if param:
        url = url + "?prop=" + urllib.quote(param)

    INPUT = body

    if isinstance(INPUT, dict):
        INPUT = json.dumps(body)

    return H.request(url, "POST", body=INPUT)


def test_removing_date_string():
    INPUT = {
        "sourceResource": {
            "date": "aaa",
        }
    }
    EXPECTED_OUTPUT = {
        "sourceResource": {
        }
    }
    resp, content = _get_server_response(INPUT, "sourceResource/date")
    print_error_log()
    assert resp["status"].startswith("2")
    assert_same_jsons(EXPECTED_OUTPUT, content)


def test_removing_multiple_invalid_date_strings():
    INPUT = {
        "sourceResource": {
            "date": [
                "aaa",
                "bbb",
                "ccc",
            ]
        }
    }
    EXPECTED_OUTPUT = {
        "sourceResource": {
        }
    }
    resp, content = _get_server_response(INPUT, "sourceResource/date")
    print_error_log()
    assert resp["status"].startswith("2")
    assert_same_jsons(EXPECTED_OUTPUT, content)


def test_removing_multiple_invalid_date_dicts():
    INPUT = {
            "sourceResource": {
                "date": [
                    {
                        "begin": "1959-01-01",
                        "end": "1959-01-01",
                        "displayDate": "1959-01-01"
                        },
                    {
                        "begin": "2008-11-18",
                        "end": "2008-11-18",
                        "displayDate": "2008-11-18"
                        },
                    {
                        "begin": "1959-01-01",
                        "end": None,
                        "displayDate": "1959-01-01"
                        },
                    {
                        "begin": "2008-11-18",
                        "end": "",
                        "displayDate": "2008-11-18"
                        },
                    {
                        "begin": None,
                        "end": "1959-01-01",
                        "displayDate": "1959-01-01"
                        },
                    {
                        "begin": "",
                        "end": "2008-11-18",
                        "displayDate": "2008-11-18"
                        },
                    "aaa",
                    "bbb",
                    "ccc",
                    ]
                }
            }
    EXPECTED_OUTPUT = {
            "sourceResource": {
                "date": [
                    {
                        "begin": "1959-01-01",
                        "end": "1959-01-01",
                        "displayDate": "1959-01-01",
                        },
                    {
                        "begin": "2008-11-18",
                        "end": "2008-11-18",
                        "displayDate": "2008-11-18"
                        },
                    ]
                }
            }
    resp, content = _get_server_response(INPUT, "sourceResource/date")
    print_error_log()
    assert resp["status"].startswith("2")
    assert_same_jsons(EXPECTED_OUTPUT, content)

def test_not_remove_valid_date_dict():
    INPUT = {
            "sourceResource": {
                "date":
                    {
                        "begin": "1959-01-01",
                        "end": "1959-01-01",
                        "displayDate": "1959-01-01"
                        },
                    }
            }
    EXPECTED_OUTPUT = {
            "sourceResource": {
                "date":
                {
                    "begin": "1959-01-01",
                    "end": "1959-01-01",
                    "displayDate": "1959-01-01"
                    },
                }
            }
    resp, content = _get_server_response(INPUT)
    print_error_log()
    assert resp["status"].startswith("2")
    assert_same_jsons(EXPECTED_OUTPUT, content)

def test_removing_single_invalid_dict():
    data = (None, None), (None, ""), ("", ""), ("", None), ("", "")
    INPUT = {
        "sourceResource": {
            "date":
                {
                    "begin": "",
                    "end": "",
                    "displayDate": "2008-11-18"
                },
            }
    }
    EXPECTED_OUTPUT = {
        "sourceResource": {
        }
    }

    for d in data:
        INPUT["sourceResource"]["date"]["begin"] = d[0]
        INPUT["sourceResource"]["date"]["end"] = d[1]
        resp, content = _get_server_response(INPUT, "sourceResource/date")
        print_error_log()
        assert resp["status"].startswith("2")
        assert_same_jsons(EXPECTED_OUTPUT, content)


