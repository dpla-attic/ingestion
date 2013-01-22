import sys
from server_support import server, print_error_log, get_thumbs_root

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

url = server() + "download_preview"

def test_download_preview_bad_json():
    """
    Should get 500 from akara for bad json.
    """
    INPUT = [ "{...}", "{aaa:'bbb'}", "xxx" ]
    for i in INPUT:
        resp,content = H.request(url,"POST",body=i,headers=HEADERS)
        assert resp.status == 500


def test_download_preview_without_id():
    """
    Should get 200 from akara and input JSON when there is missing id field.
    """
    INPUT = '{"aaa":"bbb"}'
    resp,content = H.request(url,"POST",body=INPUT,headers=HEADERS)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)


def test_download_preview_without_thumbnail_url_field():
    """
    Should get 200 from akara and input JSON when there is missing thumbnail url_field.
    """
    INPUT = '{"aaa":"bbb", "id":"abc"}'
    resp,content = H.request(url,"POST",body=INPUT,headers=HEADERS)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)

def test_download_preview_with_bad_url():
    """
    Should get 200 from akara and input JSON when there is bad thumbnail URL.
    """
    INPUT = '{"aaa":"bbb", "id":"abc", "%s":"aaa"}' % URL_FIELD_NAME
    resp,content = H.request(url,"POST",body=INPUT,headers=HEADERS)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)

def test_download_preview():
    """
    Should get 200 from akara and input JSON when there is bad thumbnail URL.
    """
    GOOD_DATA = { "id":"clemson--cfb004",
        URL_FIELD_NAME:"http://repository.clemson.edu/cgi-bin/thumbnail.exe?CISOROOT=/cfb&CISOPTR=1040"
    }

    def get_file_path():
        global thumbs_root
        id = 'clemson__cfb004'
        md5 = '84BA8BC32C4316A96FDC89F51BEB427D'
        pinfo(get_thumbs_root())
        path = get_thumbs_root()
        for i in xrange(0,32,2):
            path = os.path.join(path, md5[i:i+2])
        path = os.path.join(path, id+".jpg")
        return path
    
    INPUT = json.dumps(GOOD_DATA)
    GOOD_DATA[URL_FILE_PATH] = get_file_path()
    EXPECTED_OUTPUT = json.dumps(GOOD_DATA)
    resp,content = H.request(url,"POST",body=INPUT,headers=HEADERS)

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
    resp, content = H.request(url, "POST", body=INPUT, headers=HEADERS)
    assert resp.status == 200
    assert_same_jsons(INPUT, content)

from dplaingestion.akamod.download_test_image import image_png, image_jpg

def _check_downloading_image(extension, expected_content):
    """
    TODO: comment here
    """
    GOOD_DATA = { "id":"clemson--cfb004",
            URL_FIELD_NAME: server() + 'download_test_image?extension=' + extension
    }
    INPUT = json.dumps(GOOD_DATA)
    resp, content = H.request(url, "POST", body=INPUT, headers=HEADERS)
    filepath = get_thumbs_root() + "/84/BA/8B/C3/2C/43/16/A9/6F/DC/89/F5/1B/EB/42/7D/clemson__cfb004." + extension
    GOOD_DATA[URL_FILE_PATH] = filepath
    OUTPUT = json.dumps(GOOD_DATA)
    assert resp.status == 200
    assert_same_jsons(OUTPUT, content)

    # let's check if the files are the same
    base64_downloaded_file = ""
    with open(filepath, "rb") as f:
        data = f.read()
        print len(data)
        base64_downloaded_file = data.encode("base64").rstrip('\n')
    print base64_downloaded_file[-10:] + "|"
    print expected_content[-10:] + "|"
    print len(base64_downloaded_file)
    print len(expected_content)
    assert base64_downloaded_file == expected_content


def test_dowloading_png_image():
    """
    Should return JSON with file path and download the file.
    """
    _check_downloading_image("png", image_png)

def test_dowloading_jpg_image():
    """
    Should return JSON with file path and download the file.
    """
    _check_downloading_image("jpg", image_jpg)

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
