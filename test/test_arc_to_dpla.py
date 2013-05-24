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


BASIC_URL = server() + "arc-to-dpla"


def _get_server_response(body):

    INPUT = body

    if isinstance(body, dict):
        INPUT = json.dumps(body)

    return H.request(BASIC_URL, "POST", body=INPUT)


def test_populating_thumbnail_url_with_multiple_objects():
    INPUT = {
        "objects": {
            "object": [
                {
                    "thumbnail-url": "http://media.nara.gov/rg-241/A1-9-E/HF-107033943-twain-suspenders/hf1-107033943-2011-015-pr_t.jpg",
                    "object-sequence-number": "1",
                    "file-size": "1000713",
                    "mime-type": "image/jpeg",
                    "num": "1",
                    "file-url": "http://media.nara.gov/rg-241/A1-9-E/HF-107033943-twain-suspenders/hf1-107033943-2011-015-pr.jpg"
                    },
                {
                    "thumbnail-url": "http://media.nara.gov/rg-241/A1-9-E/HF-107033943-twain-suspenders/hf1-107033943-2011-016-pr_t.jpg",
                    "object-sequence-number": "2",
                    "file-size": "828924",
                    "mime-type": "image/jpeg",
                    "num": "2",
                    "file-url": "http://media.nara.gov/rg-241/A1-9-E/HF-107033943-twain-suspenders/hf1-107033943-2011-016-pr.jpg"
                    },
                {
                    "thumbnail-url": "http://media.nara.gov/rg-241/A1-9-E/HF-107033943-twain-suspenders/hf1-107033943-2011-017-pr_t.jpg",
                    "object-sequence-number": "3",
                    "file-size": "772116",
                    "mime-type": "image/jpeg",
                    "num": "3",
                    "file-url": "http://media.nara.gov/rg-241/A1-9-E/HF-107033943-twain-suspenders/hf1-107033943-2011-017-pr.jpg"
                    }
                ]
            },

        }

    EXPECTED_OBJECT = "http://media.nara.gov/rg-241/A1-9-E/HF-107033943-twain-suspenders/hf1-107033943-2011-015-pr_t.jpg"
    resp, content = _get_server_response(INPUT)
    assert resp["status"].startswith("2")
    CONTENT = json.loads(content)
    
    assert EXPECTED_OBJECT == CONTENT["object"]


def test_populating_thumbnail_url_with_one_object():
    INPUT = {
        "objects": {
            "object":
                {
                    "thumbnail-url": "http://media.nara.gov/rg-241/A1-9-E/HF-107033943-twain-suspenders/hf1-107033943-2011-015-pr_t.jpg",
                    "object-sequence-number": "1",
                    "file-size": "1000713",
                    "mime-type": "image/jpeg",
                    "num": "1",
                    "file-url": "http://media.nara.gov/rg-241/A1-9-E/HF-107033943-twain-suspenders/hf1-107033943-2011-015-pr.jpg"
                    },
            },

        }

    EXPECTED_OBJECT = "http://media.nara.gov/rg-241/A1-9-E/HF-107033943-twain-suspenders/hf1-107033943-2011-015-pr_t.jpg"
    resp, content = _get_server_response(INPUT)
    assert resp["status"].startswith("2")
    CONTENT = json.loads(content)
    
    assert EXPECTED_OBJECT == CONTENT["object"]
