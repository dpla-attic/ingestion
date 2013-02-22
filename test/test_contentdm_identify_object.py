
import sys
from server_support import server, print_error_log, H

import os
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo
from nose.tools import nottest
############################################################################
## CONTENTDM
## TODO: move to another file


def contentdm_url(rights_field="r", download="True"):
    return server() + \
            "contentdm-identify-object?rights_field=%s&download=%s" \
            % (rights_field, download)


def test_contentdm_identify_object_without_download():
    """
    Should add a thumbnail URL made of the source URL.
    """
    INPUT = {
            u"something": "x",
            u"somethink": "y",
            u"originalRecord":
                    {"handle":
                        ["aaa", "http://repository.clemson.edu/u?/scp,104"]
                    },
            u"left": "right now!"
    }
    EXPECTED = {
            u"something": "x",
            u"somethink": "y",
            u"originalRecord": {
                "handle":
                    ["aaa", "http://repository.clemson.edu/u?/scp,104"]
                },
            u"object": {
                "@id": "http://repository.clemson.edu/cgi-bin/" +
                        "thumbnail.exe?CISOROOT=/scp&CISOPTR=104",
                "format": "",
                "rights": "right now!"
            },
            u"admin": {u"object_status": "ignore"},
            u"left": "right now!"
    }
    url = contentdm_url(u"left", "False")
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    print_error_log()
    assert str(resp.status).startswith("2")
    result = json.loads(content)

    assert_same_jsons(EXPECTED, result)


def test_contentdm_identify_object_with_download():
    """
    Should add a thumbnail URL made of the source URL.
    """
    INPUT = {
            u"something": "x",
            u"somethink": "y",
            u"originalRecord": {
                "handle": ["aaa", "http://repository.clemson.edu/u?/scp,104"]
                },
            u"left": "right now!"
    }
    EXPECTED = {
            u"something": "x",
            u"somethink": "y",
            u"originalRecord": {
                "handle": ["aaa", "http://repository.clemson.edu/u?/scp,104"]
                },
            u"object": {
                "@id": "http://repository.clemson.edu/cgi-bin/" +
                    "thumbnail.exe?CISOROOT=/scp&CISOPTR=104",
                "format": "",
                "rights": "right now!"
            },
            u"admin": {u"object_status": "pending"},
            u"left": "right now!"
    }
    url = contentdm_url(u"left", "True")

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))

    assert str(resp.status).startswith("2")
    result = json.loads(content)

    assert_same_jsons(EXPECTED, result)


def test_contentdm_identify_object_bad_json():
    """
    Should get 500 from akara for bad json.
    """
    INPUT = ["{...}", "{aaa:'bbb'}", "xxx"]
    for i in INPUT:
        url = contentdm_url()
        resp, ontent = H.request(url, "POST", body=i)
        assert resp.status == 500


def test_contentdm_identify_object_missing_source_field():
    """
    Should return original JSON if the thumbnail URL field is missing.
    """
    INPUT = {
            "aaa": "bbb",
        }
    INPUT = json.dumps(INPUT)
    url = contentdm_url()
    resp, content = H.request(url, "POST", body=INPUT)

    assert_same_jsons(INPUT, content)


def test_contentdm_identify_object_bad_url():
    """
    Should return original JSON for bad URL.
    """
    bad_urls = [u"http://repository.clemson.edu/uscp104",
        u"http://repository.clemson.edu/s?/scp,04",
        u"http://repository.clemson.edu/u/scp,04",
        u"http://repository.clemson.edu/u?/scp104",
        u"http://repository.clemson.edu/u?/scp",
        u"http://repository.clemson.edu/",
            ]
    INPUT = {
            u"something": u"x",
            u"somethink": u"y",
            u"source": u""
    }
    for bad_url in bad_urls:
        INPUT[u"source"] = bad_url
        url = contentdm_url()
        resp, content = H.request(url, "POST", body=json.dumps(INPUT))
        assert str(resp.status).startswith("2")
        assert_same_jsons(INPUT, content)


if __name__ == "__main__":
    raise SystemExit("Use nosetests")
