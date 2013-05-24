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


# This may no longer be a valid test - JL 5/24
@nottest
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
    pinfo(content,CONTENT["sourceResource"]["collection"])
    assert_same_jsons(EXPECTED_COLLECTION, CONTENT["sourceResource"]["collection"])


def test_populating_publisher_field():
    INPUT = {
            "freetext": {
                "publisher": [
                    {
                        "#text": "Glossary of Coins and Currency Terms",
                        "@label": "Publication title"
                        },
                    {
                        "#text": "http://americanhistory.si.edu/coins/glossary.cfm",
                        "@label": "Publication URL"
                        },
                    {
                        "#text": "xx",
                        "@label": "Publisher"
                        }
                    ],
                }
            }
    EXPECTED_PUBLISHER = {
            "publisher": ["xx"]
    }
    resp, content = _get_server_response(INPUT)
    print_error_log()
    assert resp["status"].startswith("2")
    CONTENT = json.loads(content)
    pinfo(content)

    assert_same_jsons(EXPECTED_PUBLISHER, CONTENT["sourceResource"])


def test_populating_data_provider_field():
    INPUT = {
            "descriptiveNonRepeating": {
                "data_source": "Smithsonian Institution Archives",
                }
            }
    resp, content = _get_server_response(INPUT)
    pinfo(content)
    print_error_log()
    assert resp["status"].startswith("2")
    CONTENT = json.loads(content)
    assert CONTENT["dataProvider"] == "Smithsonian Institution Archives"


def test_populating_title():
    LABELS = (True, "Title"), (True, "title"), (False, "aaa"), (False, "bbb")
    INPUT = {
            "descriptiveNonRepeating": {
                "title": {
                    "@label": "",
                    "#text": "tt",
                }
            }
    }

    for d in LABELS:
        INPUT["descriptiveNonRepeating"]["title"]["@label"] = d[1]
        resp, content = _get_server_response(INPUT)
        #print_error_log()
        pinfo(resp, content)
        assert resp["status"].startswith("2")
        CONTENT = json.loads(content)
        if d[0]:
            assert_same_jsons({"title": "tt"}, CONTENT["sourceResource"])
        else:
            assert "sourceResource" not in CONTENT


def test_transforming_one_thumbnail():
    INPUT = {
            "descriptiveNonRepeating": {
                "online_media": {
                    "media": {
                        "@rights": "This image was obtained from the Smithsonian...",
                        "@idsId": "NMAH-AHB2012q06315",
                        "@caption": "Orthoclone OKT 3, Muromonab-CD3 .",
                        "#text": "http://ids.si.edu/ids/dynamic?id=NMAH-AHB2012q06315",
                        "@type": "Images",
                        "@thumbnail": "http://ids.si.edu/ids/deliveryService?id=NMAH-AHB2012q06315&max=150"
                        },
                    "@mediaCount": "1"
                    },
                }
            }
    EXPECTED_THUMBNAIL = "http://ids.si.edu/ids/deliveryService?id=NMAH-AHB2012q06315&max=150"
    resp, content = _get_server_response(INPUT)
    assert resp["status"].startswith("2")
    CONTENT = json.loads(content)
    assert EXPECTED_THUMBNAIL == CONTENT["object"]

def test_transforming_multiple_thumbnails():
    INPUT = {
            "descriptiveNonRepeating": {
                "online_media": {
                    "media": [
                        {
                            "@rights": "This image was obtained from the Smithsonian...",
                            "@idsId": "NMAH-AHB2012q06320",
                            "@caption": "Recombivax HB, Hepatitus B Vaccine (Recombinant) Adult Formula, 3 mL.",
                            "#text": "http://ids.si.edu/ids/dynamic?id=NMAH-AHB2012q06320",
                            "@type": "Images",
                            "@thumbnail": "http://ids.si.edu/ids/deliveryService?id=NMAH-AHB2012q06320&max=150"
                            },
                        {
                            "@rights": "This image was obtained from the Smithsonian...",
                            "@idsId": "NMAH-AHB2012q06380",
                            "@caption": "Recombivax HB, Hepatitis B Vaccine (Recombinant) product insert, part 1 of 4.",
                            "#text": "http://ids.si.edu/ids/dynamic?id=NMAH-AHB2012q06380",
                            "@type": "Images",
                            "@thumbnail": "http://ids.si.edu/ids/deliveryService?id=NMAH-AHB2012q06380&max=150"
                            },
                        ],
                    "@mediaCount": "6"
                },
        }
    }
    EXPECTED_THUMBNAIL = "http://ids.si.edu/ids/deliveryService?id=NMAH-AHB2012q06320&max=150"
    resp, content = _get_server_response(INPUT)
    assert resp["status"].startswith("2")
    CONTENT = json.loads(content)
    assert EXPECTED_THUMBNAIL == CONTENT["object"]

def test_thumbnail_for_no_media():
    INPUT = {
            "descriptiveNonRepeating": {
                "online_media": {
                    "media": {
                        },
                    },
                }
            }
    resp, content = _get_server_response(INPUT)
    assert resp["status"].startswith("2")
    CONTENT = json.loads(content)
    assert not "object" in CONTENT
