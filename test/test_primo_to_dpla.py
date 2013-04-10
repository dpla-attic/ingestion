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


BASIC_URL = server() + "primo-to-dpla"


def _get_server_response(body):
    return H.request(BASIC_URL, "POST", body=body)


SPATIAL_INPUT = {
        "PrimoNMBib": {
            "record": {
                "display": {
                    "lds08": "Salt Lake City, Salt Lake county, Utah, United States",
                    },
                "search": {
                    "lsr14": "Salt Lake City, Salt Lake county, Utah, United States",
                    }
                }
            }
        }


def test_converting_single_spatial_field():
    resp, content = _get_server_response(json.dumps(SPATIAL_INPUT))
    spatial_res = json.loads(content)["sourceResource"]["spatial"]
    expected_res = ["Salt Lake City, Salt Lake county, Utah, United States"]
    assert expected_res == spatial_res
