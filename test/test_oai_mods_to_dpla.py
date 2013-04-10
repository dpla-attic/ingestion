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


URL = server() + "oai_mods_to_dpla"
def _get_server_response(body):
    return H.request(URL, "POST", body=body)

def test_subject_and_spatial_transform():
    INPUT = {
    "metadata": {
      "mods": {
        "subject": [
          {
            "name": {
                "namePart": [
                    "Cornell, Sarah Maria",
                    {
                        "#text": "1802-1832",
                        "type": "date"
                    }
                ],
                "type": "personal"
            },
            "authority": "lcsh"
          },
          {
            "name": {
                "namePart": [
                    "Avery, Ephraim K",
                    {
                        "#text": "d. 1869",
                        "type": "date"
                    }
                ],
                "type": "personal"
            },
            "authority": "lcsh"
          },
          {
            "topic": [
                "Murder",
                "Poetry"
            ],
            "geographic": "Rhode Island",
            "authority": "lcsh"
          },
          {
            "topic": [
                "Seduction",
                "Poetry"
            ],
            "geographic": "Rhode Island",
            "authority": "lcsh"
          },
          {
            "hierarchicalGeographic": {
                "country": "United States"
            }
          }
        ]
      }
    }
    }
    EXPECTED = {
        "subject": [
            "Cornell, Sarah Maria, 1802-1832",
            "Avery, Ephraim K, d. 1869",
            "Murder--Poetry--Rhode Island",
            "Seduction--Poetry--Rhode Island",
            "United States"
        ],
        "spatial": [
            "Rhode Island",
            "United States"
        ]
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content)["sourceResource"]) 

def test_origin_info_transform():
    INPUT = {
        "metadata": {
            "mods": {
                "originInfo": {
                    "publisher": "s.n.",
                    "dateOther": {
                        "#text": "1832",
                        "keyDate": "yes"
                    },
                    "place": [
                        {
                            "placeTerm": {
                                "#text": "mau",
                                "type": "code",
                                "authority": "marccountry"
                            }
                        },
                        {
                            "placeTerm": {
                                "#text": "[United States",
                                "type": "text"
                            }
                        }
                    ],
                    "issuance": "monographic",
                    "dateIssued": [
                        "1832]",
                        {
                            "#text": "1832",
                            "encoding": "marc"
                        }
                    ]
                }
            }
        }
    }
    EXPECTED = {
        "date": "1832",
        "publisher": "[United States: s.n., 1832]"
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    print_error_log()
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content)["sourceResource"]) 
