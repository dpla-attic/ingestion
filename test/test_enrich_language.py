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
        "aggregatedCHO": {
            "language": "aaa"
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    INPUT["aggregatedCHO"]["language"] = {"name": "aaa"}
    print_error_log()
    assert resp.status == 200
    assert_same_jsons(INPUT, content)


def test_no_params_with_many_languages():
    """
    Should return converted JSON for no param.
    """
    INPUT = {
        "aggregatedCHO": {
            "language": ["aaa", "bbb"]
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    INPUT["aggregatedCHO"]["language"] = [{"name": "aaa"}, {"name": "bbb"}]
    assert resp.status == 200
    assert_same_jsons(INPUT, content)


if __name__ == "__main__":
    raise SystemExit("Use nosetest")
