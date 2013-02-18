"""
 Akara module that provides cleaning endpoints:
 - filter_empty_values: cleans empty leaves of given json;
 - filter_fields: cleans elements of json with given top-level keys if corresponding value is empty;

 to test locally, save TEST_EXAMPLE to local file (i.e. artstor_doc.js) and then run:

 curl -X POST -d @test/artstor_doc.js -H "Content-Type: application/json" http://localhost:8879/filter_empty_values
"""

__author__ = 'Alexey R.'

import copy

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

from dplaingestion.selector import getprop, setprop, PATH_DELIM


HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'


def filter_empty_leaves(d, ignore_keys=tuple()):
    """
    Makes one iteration and removes found empty leaves from dictionary tree; (Empty leaf = key without value;)

    Ignores keys listed in ignore_keys sequence;
    Returns: cleaned dictionary;

    Side effect: modifies passed dictionary!
    """
    def sub_cleaner(el):
        if isinstance(el, dict):
            return filter_empty_leaves(copy.deepcopy(el), ignore_keys)
        else:
            return el

    for k, v in d.items():
        if k in ignore_keys:
            continue
        elif isinstance(v, dict) and v:
            d[k] = filter_empty_leaves(v, ignore_keys)
        elif isinstance(v, list) and v:
            d[k] = [sub_cleaner(e) for e in v if e]
        else:
            if not v and k not in ignore_keys:
                del d[k]
    return d

def filter_fields(d, check_keys=tuple()):
    """
    Makes one iteration and cleans found elements of dictionary with given keys if corresponding value is empty;
    Arguments:
     d - dictionary for traversing;
     check_keys - list of top-level keys to check;
    Returns:
     cleaned dictionary
    Side effect:
     modifies passed dictionary
    """
    def sub_cleaner(el):
        if isinstance(el, dict):
            return filter_empty_leaves(copy.deepcopy(el))
        else:
            return el

    for k, v in d.items():
        if k not in check_keys:
            continue
        elif isinstance(v, dict) and v:
            d[k] = filter_empty_leaves(v)
        elif isinstance(v, list) and v:
            d[k] = [sub_cleaner(e) for e in v if e]
        else:
            if not v:
                del d[k]
    return d

def filter_dict(_dict, cleaner_func, *args):
    """
    Repeatedly runs cleaner function until all empty leaves are removed (hash stops changing).
    Arguments:
     _dict - dictionary to clean;
     cleaner_func - runs given function against passed dictionary;
     *args - arguments will be passed to cleaner func
    Returns:
     cleaned dictionary
    """
    d = copy.deepcopy(_dict)
    hash_before = hash(str(d))
    hash_current = None
    while hash_before != hash_current:
        hash_before = hash_current
        hash_current = hash(str(cleaner_func(d, *args)))
    return d

def filter_path(_dict, path):
    """
    Repeatedly runs cleaner function until all empty values are removed from given path (hash stops changing).
    Arguments:
     _dict - dictionary to clean;
     path - a xpath-like path to the value, that must be checked
    Returns:
     cleaned dictionary
    """
    d = copy.deepcopy(_dict)
    try:
        value = getprop(d, path)
    except KeyError:
        logger.warning("Attempt to clean non existent property \"%s\"", path)
        return _dict
    else:
        if not value:
            embracing_path, sep, value_key = path.rpartition(PATH_DELIM)
            if value_key:
                embracing_dict = getprop(d, embracing_path)
                del embracing_dict[value_key]
                setprop(d, embracing_path, embracing_dict)
            else:
                del d[path]
            return d
        else:
            return _dict

def test_filtering():
    source = {"v1": "", "v2": "value2", "v3": {"vv1": "", "vv2": "v_value2"}, "v4": {}, "v5": {"0": {"name": ""}, "1": {"name": "name_value_1"}}, "v6": ["", "vvalue6", {}, {"v_sub": ""}], "v7": [""]}
    expected = {"v2": "value2", "v3": {"vv2": "v_value2"}, "v5": {"1": {"name": "name_value_1"}}, "v6": ["vvalue6"]}
    filtered = filter_dict(copy.deepcopy(source), filter_empty_leaves)
    assert expected == filtered, "Expected dictionary does not equal to filtered"

@simple_service('POST', 'http://purl.org/la/dp/filter_empty_values', 'filter_empty_values', HTTP_TYPE_JSON)
def filter_empty_values_endpoint(body, ctype, ignore_key="dplaSourceRecord"):
    """
    Cleans empty leaves of given json tree;
    Argument:
     ignore_key - comma separated list of keys that should be ignored while traversing the tree;
    """
    try:
        assert ctype.lower() == HTTP_TYPE_JSON, "%s is not %s" % (HTTP_HEADER_TYPE, HTTP_TYPE_JSON)
        data = json.loads(body)
    except Exception as e:
        error_text = "Bad JSON: %s: %s" % (e.__class__.__name__, str(e))
        logger.exception(error_text)
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return error_text

    ignore_keys = [k.strip() for k in ignore_key.split(",") if k]
    data = filter_dict(data, filter_empty_leaves, ignore_keys)
    return json.dumps(data)

@simple_service('POST', 'http://purl.org/la/dp/filter_fields', 'filter_fields', HTTP_TYPE_JSON)
def filter_fields_endpoint(body, ctype, keys):
    """
    Cleans elements of json with given keys if corresponding value is empty;
    Argument:
     keys - comma separated list of top-level keys that should be checked for emptiness in json tree;
    """
    try:
        assert ctype.lower() == HTTP_TYPE_JSON, "%s is not %s" % (HTTP_HEADER_TYPE, HTTP_TYPE_JSON)
        data = json.loads(body)
    except Exception as e:
        error_text = "Bad JSON: %s: %s" % (e.__class__.__name__, str(e))
        logger.exception(error_text)
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return error_text

    check_keys = [k.strip() for k in keys.split(",") if k]
    data = filter_dict(data, filter_fields, check_keys)
    return json.dumps(data)

@simple_service('POST', 'http://purl.org/la/dp/filter_paths', 'filter_paths', HTTP_TYPE_JSON)
def filter_paths_endpoint(body, ctype, paths):
    """
    Cleans elements of json with given xpath-like path if corresponding value is empty;
    Argument:
     paths - comma separated list of json paths that should be checked for emptiness in json tree;
    """
    try:
        assert ctype.lower() == HTTP_TYPE_JSON, "%s is not %s" % (HTTP_HEADER_TYPE, HTTP_TYPE_JSON)
        data = json.loads(body)
    except Exception as e:
        error_text = "Bad JSON: %s: %s" % (e.__class__.__name__, str(e))
        logger.exception(error_text)
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return error_text

    check_paths = [k.strip() for k in paths.split(",") if k]
    for path in check_paths:
        data = filter_path(data, path)
    return json.dumps(data)


