import os
import sys
import re
import hashlib
from server_support import server, print_error_log, get_thumbs_root
from dplaingestion.akamod.download_test_image import image_png, image_jpg
from amara.thirdparty import httplib2
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

# URL for current akara server instance.
url = server() + "download_preview"

def _get_server_response(body):
    return H.request(url,"POST",body=body,headers=HEADERS)

def test_download_preview_bad_json():
    """
    Should get 500 from akara for bad json.
    """
    INPUT = [ "{...}", "{aaa:'bbb'}", "xxx" ]
    for i in INPUT:
        resp,content = _get_server_response(i)
        assert resp.status == 500


def test_download_preview_without_id():
    """
    Should get 200 from akara and input JSON when there is missing id field.
    """
    INPUT = '{"aaa":"bbb"}'
    resp,content = _get_server_response(INPUT)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)


def test_download_preview_without_thumbnail_url_field():
    """
    Should get 200 from akara and input JSON when there is missing thumbnail url_field.
    """
    INPUT = '{"aaa":"bbb", "id":"abc"}'
    resp,content = _get_server_response(INPUT)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)


def test_download_preview_with_bad_url():
    """
    Should get 200 from akara and input JSON when there is bad thumbnail URL.
    """
    INPUT = '{"aaa":"bbb", "id":"abc", "%s":"aaa"}' % URL_FIELD_NAME
    resp,content = _get_server_response(INPUT)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)


def _get_file_path(doc_id, extension):
    """
    Calculates the absolute filepath where the for 
    given document id and file extension shoul be saved 
    by the download_preview module.

    Arguments:
        doc_id      -   document id from couchdb
        extension   -   file extension, without '.'

    Returns:
        Absolute path for the file.
    """
    id = re.sub(r'[-]', '_', doc_id)
    md5 = hashlib.md5(doc_id).hexdigest().upper()
    md5_path = ""
    for i in xrange(0,32,2):
        md5_path = os.path.join(md5_path, md5[i:i+2])
    path = os.path.join(get_thumbs_root(), md5_path, id + "." + extension)
    return path


def test_download_preview():
    """
    Should get 200 from akara and input JSON when there is bad thumbnail URL.
    """
    GOOD_DATA = { "id":"clemson--cfb004",
        URL_FIELD_NAME:"http://repository.clemson.edu/cgi-bin/thumbnail.exe?CISOROOT=/cfb&CISOPTR=1040"
    }
    
    INPUT = json.dumps(GOOD_DATA)
    GOOD_DATA[URL_FILE_PATH] = _get_file_path("clemson--cfb004", "jpg")
    EXPECTED_OUTPUT = json.dumps(GOOD_DATA)
    resp,content = _get_server_response(INPUT)

    assert resp.status == 200
    assert_same_jsons(EXPECTED_OUTPUT, content)


def test_downloading_with_bad_URL():
    """
    Should return the same json, when the image URL gives 500.
    """
    GOOD_DATA = { "id":"clemson--cfb004",
            URL_FIELD_NAME: server() + 'download_test_image?extension=500'
    }
    INPUT = json.dumps(GOOD_DATA)
    resp,content = _get_server_response(INPUT)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)


def _check_downloading_image(extension, expected_content):
    """
    Calls the download_preview service passing thumbnail URL to download_test_image
    service, which 
    """
    id = 'clemson_--__qkjwerjerj_akjdasdhwqe-399394'
    GOOD_DATA = { "id":id,
            URL_FIELD_NAME: server() + 'download_test_image?extension=' + extension
    }
    INPUT = json.dumps(GOOD_DATA)
    resp,content = _get_server_response(INPUT)
    # The file should be written at:
    filepath = _get_file_path(id, extension)
    GOOD_DATA[URL_FILE_PATH] = filepath
    OUTPUT = json.dumps(GOOD_DATA)
    assert resp.status == 200
    assert_same_jsons(OUTPUT, content)

    # let's check if the files are the same
    with open(filepath, "rb") as f:
        data = f.read()
        base64_downloaded_file = data.encode("base64").rstrip('\n')
        assert base64_downloaded_file == expected_content


def test_dowloading_png_image():
    """
    Should return JSON with file path and png download the file.
    """
    _check_downloading_image("png", image_png)


def test_dowloading_jpg_image():
    """
    Should return JSON with file path and jpg download the file.
    """
    _check_downloading_image("jpg", image_jpg)


if __name__ == "__main__":
    raise SystemExit("Use nosetest")
