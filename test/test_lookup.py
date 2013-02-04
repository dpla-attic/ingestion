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

BASIC_URL = server() + "lookup"


def _get_server_response(body, input_field=None,
        output_field=None, subst=None):
    """
    Returns response from server using provided url.
    """
    url = BASIC_URL + "?"

    if input_field is not None:
        url += "input_field=%s&" % input_field

    if output_field is not None:
        url += "output_field=%s&" % output_field

    if subst is not None:
        url += "substitution=%s&" % subst

    print "Calling URL = [%s]" % url
    return H.request(url, "POST", body=body, headers=HEADERS)


def test_bad_INPUT_json():
    """
    Should return 500 when getting bad JSON.
    """
    INPUT = '{"aaabbb: eeee}'
    resp, content = _get_server_response(INPUT, "a", "b")
    assert resp.status == 500


def test_no_params():
    """
    Should return 500 for no params.
    """
    INPUT = '{"aaa":"bbb"}'
    resp, content = _get_server_response(INPUT)
    assert resp.status == 500


def test_missing_input_field():
    """
    Should return 500 for no param.
    """
    INPUT = '{"aaa":"bbb"}'
    resp, content = _get_server_response(INPUT)
    assert resp.status == 500


def test_missing_field_in_json():
    """
    Should return the same json for missing INPUT field in json.
    """
    INPUT = '{"aaa":"bbb"}'
    resp, content = _get_server_response(INPUT, "a", "b", "TEST_SUBSTITUTE")
    assert resp.status == 200
    assert_same_jsons(INPUT, INPUT)


def test_missing_output_field():
    """
    Should return 500 when the OUTPUT field is missing from URL.
    """
    INPUT = '{"aaa":"bbb"}'
    resp, content = _get_server_response(INPUT, "a", "", "TEST_SUBSTITUTE")
    assert resp.status == 500


def test_substitution_with_missing_subst_dict():
    """
    Should return the same JSON when the key is missing from substitution.
    """
    INPUT = '{"aaa":"bbb"}'
    resp, content = _get_server_response(INPUT, "aaa", "aaa", "aaa")
    print (content)
    assert resp.status == 500
    assert content == "Missing substitution dictionary"


def test_substitution_with_missing_key():
    """
    Should return the same JSON when the key is missing from substitution.
    """
    INPUT = '{"aaa":"bbb"}'
    resp, content = _get_server_response(INPUT, "bbb",
            "bbb", "test_substitute")
    assert resp.status == 200
    assert_same_jsons(INPUT, content)


def test_simple_substitute_for_the_same_field():
    """
    Should return substituted same json field.
    """
    INPUT = '{"aaa":"bbb"}'
    EXPECTED_OUTPUT = '{"aaa":"BBB"}'
    resp, content = _get_server_response(INPUT, "aaa",
            "aaa", "test_substitute")
    assert resp.status == 200
    assert_same_jsons(content, EXPECTED_OUTPUT)


def test_simple_substitute_for_different_field():
    """
    Should return substituted another json field.
    """
    INPUT = '{"aaa":"bbb"}'
    EXPECTED_OUTPUT = '{"aaa":"bbb", "xxx":"BBB"}'
    resp, content = _get_server_response(INPUT, "aaa",
            "xxx", "test_substitute")
    assert resp.status == 200
    assert_same_jsons(content, EXPECTED_OUTPUT)


def test_substitution_for_the_same_field_and_array():
    """
    Should return substituted json when original json is array.
    """
    data = {"xxx": "yyy", "aaa": ["aa", "bbb", "ccc", "ddd"]}
    INPUT = json.dumps(data)
    data["aaa"] = ["aa", "BBB", "CCC", "DDD"]
    EXPECTED_OUTPUT = json.dumps(data)
    resp, content = _get_server_response(INPUT, "aaa",
            "aaa", "test_substitute")
    print_error_log()
    assert resp.status == 200
    assert_same_jsons(content, EXPECTED_OUTPUT)


def test_substitution_for_differnt_fields_and_array():
    """
    Should return json when original json is array.
    """
    data = {"xxx": "yyy", "aaa": ["aa", "bbb", "ccc", "ddd"]}
    INPUT = json.dumps(data)
    data["zzz"] = ["aa", "BBB", "CCC", "DDD"]
    EXPECTED_OUTPUT = json.dumps(data)
    resp, content = _get_server_response(INPUT, "aaa",
            "zzz", "test_substitute")
    print_error_log()
    assert resp.status == 200
    assert_same_jsons(content, EXPECTED_OUTPUT)


def test_dictionary_subsitution():
    """
    Should substitute when there is dictionary field.
    """
    data = {"xxx": "yyy", "aaa": {"bbb": "ccc"}}
    INPUT = json.dumps(data)
    data["aaa"] = {"bbb": "CCC"}
    EXPECTED_OUTPUT = json.dumps(data)
    resp, content = _get_server_response(INPUT, "aaa.bbb",
            "aaa.bbb", "test_substitute")
    print_error_log()
    assert resp.status == 200
    assert_same_jsons(content, EXPECTED_OUTPUT)


def test_substitution_for_differnt_fields_and_array():
    """
    Should return substituted json when original json is array.
    """
    data = {"xxx": "yyy", "aaa": ["aa", "bbb", "ccc", "ddd"]}
    INPUT = json.dumps(data)
    data["zzz"] = ["aa", "BBB", "CCC", "DDD"]
    EXPECTED_OUTPUT = json.dumps(data)
    resp, content = _get_server_response(INPUT, "aaa",
            "zzz", "test_substitute")
    print_error_log()
    assert resp.status == 200
    assert_same_jsons(content, EXPECTED_OUTPUT)

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
