import os
import sys
import re
import hashlib
from server_support import server, print_error_log, get_thumbs_root, H
from dplaingestion.akamod.download_test_image import image_png, image_jpg
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo
from nose.tools import nottest

BASIC_URL = server() + "cleanup_language"

def _get_server_response(body, prop=None):
    """
    Returns response from server using provided url.
    """
    url = BASIC_URL + "?"

    if prop is not None:
        url += "prop=%s&" % prop

    return H.request(url, "POST", body=body)

def test_name_match():
    """
    Should return lower case ISO 639-3 reference names
    """
    INPUT = {
        "language": ["This is in English and Spanish.",
                     "German and german, with more German",
                     "English again and Japanese"] 
    }
    EXPECTED = {
        "language": ["English", "Spanish", "German", "Japanese"]
    }

    resp, content = _get_server_response(json.dumps(INPUT), prop="language")
    print_error_log()
    assert resp.status == 200
    assert_same_jsons(content, EXPECTED)

def test_iso_match():
    """
    Should return ISO 639-3 codes
    """
    INPUT = {
        "language": ["ger", "jpn", "en-US", "fr/FR", "af", "afr"]
    }
    EXPECTED = {
        "language": ["ger", "jpn", "eng", "fra", "afr"]
    }

    resp, content = _get_server_response(json.dumps(INPUT), prop="language")
    print_error_log()
    assert resp.status == 200
    assert_same_jsons(content, EXPECTED)

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
