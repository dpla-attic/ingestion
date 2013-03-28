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


BASIC_URL = server() + "edan_to_dpla"


def _get_server_response(body):

    INPUT = body

    if isinstance(body, dict):
        INPUT = json.dumps(body)

    return H.request(BASIC_URL, "POST", body=INPUT)


def test_populating_collection_name():
    INPUT = {
            "collection": {
                "@id": "http://dp.la/api/collections/smithsonian--nmafa_files_1964-2008_[national_museum_of_african_art_u.s._office_of_the_director]",
                "title": "nmafa_files_1964-2008_[national_museum_of_african_art_u.s._office_of_the_director]"
                },
            "freetext": {
                "physicalDescription": {
                    "#text": "4 cu. ft. (4 record storage boxes)",
                    "@label": "Physical description"
                    },
                "name": [
                    {
                        "#text": "National Museum of African Art (U.S.) Office of the Director",
                        "@label": "Creator"
                        },
                    {
                        "#text": "Fiske, Patricia L",
                        "@label": "Subject"
                        },
                    {
                        "#text": "Patton, Sharon F",
                        "@label": "Subject"
                        },
                    {
                        "#text": "Robbins, Warren M",
                        "@label": "Subject"
                        },
                    {
                        "#text": "Walker, Roslyn A",
                        "@label": "Subject"
                        },
                    {
                        "#text": "Williams, Sylvia H",
                        "@label": "Subject"
                        },
                    {
                        "#text": "Reinhardt, John E",
                        "@label": "Subject"
                        },
                    {
                        "#text": "Museum of African Art (U.S.)",
                        "@label": "Subject"
                        },
                    {
                        "#text": "Quadrangle Complex Quadrangle Museum Project",
                        "@label": "Subject"
                        }
                    ],
                "setName": {
                    "#text": "NMAfA Files 1964-2008 [National Museum of African Art (U.S.) Office of the Director]",
                    "@label": "See more items in"
                    },
                }
            }
    EXPECTED_COLLECTION = {
            "@id": "http://dp.la/api/collections/smithsonian--nmafa_files_1964-2008_[national_museum_of_african_art_u.s._office_of_the_director]",
            "title": "NMAfA Files 1964-2008 [National Museum of African Art (U.S.) Office of the Director]",
            }
    resp, content = _get_server_response(INPUT)
    assert resp["status"].startswith("2")
    CONTENT = json.loads(content)
    pinfo(CONTENT["collection"])

    assert_same_jsons(EXPECTED_COLLECTION, CONTENT["collection"])
