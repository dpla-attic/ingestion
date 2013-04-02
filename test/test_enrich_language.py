import os
import sys
import re
import hashlib
from server_support import server, print_error_log, get_thumbs_root, H
from dplaingestion.akamod.download_test_image import image_png, image_jpg
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo
from nose.tools import nottest

BASIC_URL = server() + "enrich_language"

def _get_server_response(body, prop=None):
    """
Returns response from server using provided url.
"""
    url = BASIC_URL + "?"

    if prop is not None:
        url += "prop=%s&" % prop

    return H.request(url, "POST", body=body)


def test_bad_INPUT_json():
    """
Should return 500 when getting bad JSON.
"""
    INPUT = '{"aaabbb: eeee}'
    resp, content = _get_server_response(INPUT, "a")
    assert resp.status == 500


def test_no_params():
    """
Should return the same JSON for no param and no default key.
"""
    INPUT = '{"aaa":"bbb"}'
    resp, content = _get_server_response(INPUT)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)


def test_no_params_with_languages():
    """
Should return converted JSON for no param.
"""
    INPUT = {
        "sourceResource": {
            "language": "aaa"
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    INPUT["sourceResource"]["language"] = {"name": "aaa"}
    print_error_log()
    assert resp.status == 200
    assert_same_jsons(INPUT, content)


def test_no_params_with_many_languages():
    """
Should return converted JSON for no param.
"""
    INPUT = {
        "sourceResource": {
            "language": ["aaa", "bbb"]
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    INPUT["sourceResource"]["language"] = [{"name": "aaa"}, {"name": "bbb"}]
    assert resp.status == 200
    assert_same_jsons(INPUT, content)

def test_cleanup_enrich_then_lookup1():
    """Should produce both name and iso639_3 language fields"""
    INPUT = [
        "en", "English", ["eng"], ["English"], ["en", "English"]
    ]
    EXPECTED = {
        "sourceResource": {
            "language": [{"name": "English", "iso639_3": "eng"}]
        }
    }

    for i in range(len(INPUT)):
        input = {"sourceResource": {"language": INPUT[i]}}
        url = server() + "cleanup_language"
        resp, content = H.request(url, "POST", json.dumps(input))
        assert resp.status == 200
        url = server() + "enrich_language"
        resp, content = H.request(url, "POST", content)
        assert resp.status == 200
        url = server() + "lookup?prop=sourceResource%2Flanguage%2Fname" + \
              "&target=sourceResource%2Flanguage%2Fname&substitution=iso639_3"
        resp, content = H.request(url, "POST", content)
        assert resp.status == 200
        url = server() + "lookup?prop=sourceResource%2Flanguage%2Fname" + \
                         "&target=sourceResource%2Flanguage%2Fiso639_3" + \
                         "&substitution=iso639_3&inverse=True"
        resp, content = H.request(url, "POST", content)
        assert resp.status == 200
        assert_same_jsons(content, EXPECTED)

def test_cleanup_enrich_then_lookup2():
    """Should produce both name and iso639_3 language fields"""
    INPUT = {
        "sourceResource": {
            "language": [
                "en", "French and arabic", "spanish Spanish", "Ze Germans"
            ]
        }
    }

    EXPECTED = {
        "sourceResource": {
            "language": [
                {"name": "English", "iso639_3": "eng"},
                {"name": "Arabic", "iso639_3": "ara"},
                {"name": "French", "iso639_3": "fre"},
                {"name": "Spanish", "iso639_3": "spa"}
            ]
        }
    }

    url = server() + "cleanup_language"
    resp, content = H.request(url, "POST", json.dumps(INPUT))
    assert resp.status == 200
    url = server() + "enrich_language"
    resp, content = H.request(url, "POST", content)
    assert resp.status == 200
    url = server() + "lookup?prop=sourceResource%2Flanguage%2Fname" + \
          "&target=sourceResource%2Flanguage%2Fname&substitution=iso639_3"
    resp, content = H.request(url, "POST", content)
    assert resp.status == 200
    url = server() + "lookup?prop=sourceResource%2Flanguage%2Fname" + \
                     "&target=sourceResource%2Flanguage%2Fiso639_3" + \
                     "&substitution=iso639_3&inverse=True"
    resp, content = H.request(url, "POST", content)
    assert resp.status == 200
    assert_same_jsons(content, EXPECTED)

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
