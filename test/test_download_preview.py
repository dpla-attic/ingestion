import sys
from server_support import server

from amara.thirdparty import httplib2
import os
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo

CT_JSON = {"Content-Type": "application/json"}
HEADERS = {
            "Content-Type": "application/json",
            "Context": "{}",
          }

H = httplib2.Http()

# Used for searching for the thumbnail URL.
URL_FIELD_NAME = u"preview_source_url"

# Used for storing the path to the local filename.
URL_FILE_PATH = u"preview_file_path"

GOOD_DATA = { "id"="clemson--cfb004",
        URL_FILE_PATH:"http://repository.clemson.edu/cgi-bin/thumbnail.exe?CISOROOT=/cfb&CISOPTR=1040"
}

def test_download_preview_bad_json():
    """
    Should get 500 from akara for bad json.
    """
    INPUT = [ "{...}", "{aaa:'bbb'}", "xxx" ]
    for i in INPUT:
        url = server() + "download_preview"
        resp,content = H.request(url,"POST",body=i,headers=HEADERS)
        assert resp.status == 500


def test_download_preview_without_id():
    """
    Should get 200 from akara and input JSON when there is missing id field.
    """
    INPUT = '{"aaa":"bbb"}'
    url = server() + "download_preview"
    resp,content = H.request(url,"POST",body=INPUT,headers=HEADERS)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)


def test_download_preview_without_thumbnail_url_field():
    """
    Should get 200 from akara and input JSON when there is missing thumbnail url_field.
    """
    INPUT = '{"aaa":"bbb", "id":"abc"}'
    url = server() + "download_preview"
    resp,content = H.request(url,"POST",body=INPUT,headers=HEADERS)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)

def test_download_preview_with_bad_url():
    """
    Should get 200 from akara and input JSON when there is bad thumbnail URL.
    """
    INPUT = '{"aaa":"bbb", "id":"abc", "%s":"aaa"}' % URL_FIELD_NAME
    url = server() + "download_preview"
    resp,content = H.request(url,"POST",body=INPUT,headers=HEADERS)
    pinfo(INPUT, url,resp,content)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)

def test_download_preview_with_bad_url():
    """
    Should get 200 from akara and input JSON when there is bad thumbnail URL.
    """
    INPUT = '{"aaa":"bbb", "id":"abc", "%s":"aaa"}' % URL_FIELD_NAME
    url = server() + "download_preview"
    resp,content = H.request(url,"POST",body=INPUT,headers=HEADERS)
    pinfo(INPUT, url,resp,content)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)
