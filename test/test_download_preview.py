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

# URL for current akara server instance.
url = server() + "download_preview"


def _get_server_response(body):
    return H.request(url, "POST", body=body, headers=HEADERS)


def test_download_preview_bad_json():
    """
    Should get 500 from akara for bad json.
    """
    INPUT = ["{...}", "{aaa:'bbb'}", "xxx"]
    for i in INPUT:
        resp, content = _get_server_response(i)
        assert resp.status == 500


def example_data():
    return {
        "id": "thisisid",
        "object": {
            "@id": "aaaa",
            "format": "",
            "rights": ""
        },
        "admin": {"object_status": "pending"}
    }


def _check_missing_field(data):
    """
    Checks if the akara returns error for given data.
    """
    DATA = data
    INPUT = json.dumps(DATA)

    if "admin" in data:
        DATA["admin"]["object_status"] = "error"
    else:
        DATA["admin"] = {"object_status": "error"}

    EXPECTED_OUTPUT = json.dumps(DATA)
    resp, content = _get_server_response(INPUT)
    print_error_log()
    assert resp.status == 200
    assert_same_jsons(EXPECTED_OUTPUT, content)


def test_download_preview_without_id():
    """
    Should status="error" when there is missing id field.
    """
    DATA = example_data()
    del(DATA['id'])
    _check_missing_field(DATA)


def test_download_preview_without_object_field():
    """
    Should status="error" when there is missing object field.
    """
    DATA = example_data()
    del(DATA['object'])
    _check_missing_field(DATA)


def test_download_preview_without_object_id_field():
    """
    Should status="error" when there is missing object/@id field.
    """
    DATA = example_data()
    del(DATA['object']["@id"])
    _check_missing_field(DATA)


def test_download_preview_without_admin_field():
    """
    Should status="error" when there is missing admin field.
    """
    DATA = example_data()
    del(DATA['admin'])
    _check_missing_field(DATA)


def test_download_preview_without_admin_object_status_field():
    """
    Should status="error" when there is missing admin/object_status field.
    """
    DATA = example_data()
    print DATA
    del(DATA['admin']['object_status'])
    _check_missing_field(DATA)


def test_download_preview_with_bad_url_and_pending():
    """
    Should get changed status to "error" for bad thumbnail URL.
    """
    DATA = {
        "id": "aaa",
        "object": {
            "@id": "aaaa",
            "format": "",
            "rights": ""
        },
        "admin": {"object_status": "pending"}
    }
    INPUT = json.dumps(DATA)
    DATA["admin"]["object_status"] = "error"
    EXPECTED_OUTPUT = json.dumps(DATA)

    resp, content = _get_server_response(INPUT)
    assert resp.status == 200
    assert_same_jsons(EXPECTED_OUTPUT, content)


def test_download_preview_with_bad_url_and_ignore():
    """
    Should return "error" status when bad thumbnail URL and status = "ignore".
    """
    DATA = {
        "id": "aaa",
        "object": {
            "@id": "aaaa",
            "format": "",
            "rights": ""
        },
        "admin": {"object_status": "ignore"}
    }
    INPUT = json.dumps(DATA)
    DATA["admin"]["object_status"] = "error"
    EXPECTED_OUTPUT = json.dumps(DATA)
    resp, content = _get_server_response(INPUT)
    print_error_log()
    assert resp.status == 200
    assert_same_jsons(EXPECTED_OUTPUT, content)


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
    md5 = doc_id.upper()
    md5_path = ""
    for i in xrange(0, 32, 2):
        md5_path = os.path.join(md5_path, md5[i: i + 2])
    path = os.path.join(get_thumbs_root(), md5_path, doc_id + "." + extension)
    return path


def _get_url_path(doc_id, extension):
    """
    Calculates the url filepath where the for
    given document id and file extension shoul be saved
    by the download_preview module.

    Arguments:
        doc_id      -   document id from couchdb
        extension   -   file extension, without '.'

    Returns:
        URL path for the file.
    """
    md5 = doc_id.upper()
    md5_path = ""
    for i in xrange(0, 32, 2):
        md5_path = os.path.join(md5_path, md5[i: i + 2])

    path = os.path.join("http://aaa.bbb.com/",
            md5_path, doc_id + "." + extension)

    return path


def _check_downloading_image(extension, expected_content, status):
    """
    Calls the download_preview service passing thumbnail URL
    to download_test_image service.
    """
    id = '14428451F16BD0933F32DED43A42654A'
    GOOD_DATA = {"id": id,
            "object": {
                "@id": server() + 'download_test_image?extension=' + extension,
                "format": "",
                "rights": "lefts"
            },
            "admin": {"object_status": status}
    }
    INPUT = json.dumps(GOOD_DATA)

    if status == "pending":
        GOOD_DATA["admin"]["object_status"] = "downloaded"
        GOOD_DATA["object"]["@id"] = _get_url_path(id, extension)

    if extension == "jpg":
        GOOD_DATA["object"]["format"] = "image/jpeg"

    if extension == "png":
        GOOD_DATA["object"]["format"] = "image/png"

    resp, content = _get_server_response(INPUT)
    print_error_log()
    # The file should be written at:
    filepath = _get_file_path(id, extension)
    OUTPUT = json.dumps(GOOD_DATA)

    assert resp.status == 200
    assert_same_jsons(OUTPUT, content)

    # let's check if the files are the same, only if status == "pending"
    if status == "pending":
        with open(filepath, "rb") as f:
            data = f.read()
            base64_downloaded_file = data.encode("base64").rstrip('\n')
            assert base64_downloaded_file == expected_content


def test_dowloading_png_image():
    """
    Should return JSON with file path and png download the file.
    """
    _check_downloading_image("png", image_png, "pending")


def test_dowloading_jpg_image():
    """
    Should return JSON with file path and jpg download the file.
    """
    _check_downloading_image("jpg", image_jpg, "pending")


def test_downloading_png_image_with_ignore():
    """
    Should return INPUT JSON when status is ignore.
    """
    _check_downloading_image("png", image_png, "ignore")


def test_downloading_jpg_image_with_ignore():
    """
    Should return INPUT JSON when status is ignore.
    """
    _check_downloading_image("jpg", image_jpg, "ignore")


if __name__ == "__main__":
    raise SystemExit("Use nosetest")
