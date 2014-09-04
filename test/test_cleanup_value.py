import os
import sys
import re
import hashlib
from server_support import server, print_error_log, H
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo
from nose.tools import nottest
from urllib import quote

BASIC_URL = server() + "cleanup_value"


def _get_server_response(body, prop=None):
    url = BASIC_URL
    if prop:
        if isinstance(prop, basestring):
            url = url + "?prop=" + prop
        if isinstance(prop, list):
            d = ",".join(prop)
            d = quote(d, "")
            url = url + "?prop=" + d

    return H.request(url, "POST", body=body)


def test_changing_values():
    INPUT = [
            "hello there",
            "123",
            ". hi ",
            ".  hi",
            "             . hi there    ",
            "a banana",
            "''.more complicated....",
            '""""....even more complicated....."\'""""',
            "hello there;;",
            ";;hello there;;",
            "aaa--bbb",
            "aaa --bbb",
            "aaa-- bbb",
            "aaa --  bbb",
            "aaa  --  bbb    -- ccc - - ddd -- ",
            ["aaa", "bbb"],
            [" - aaa", " bbb --  "],
            "aaa       bbb -- ccc",
            "...,,,;;;;..,;'''\t\t\t    aaa       --       bbb      ccc       ddd;;;..;,,,,,;;;.....       \t ",
            "aaa  --  bbb       ccc\t\t\t\t\tddd",
            "   aaa -- bbb\t\t  \t\t  ccc\t\t\t   ",
            "\t\taaa\tbbb\t\t",
            """..  \t\t
                  sss ddd
                  \t\t .. """
        ]

    EXPECTED = [
            "hello there",
            "123",
                "hi",
            "hi",
            "hi there",
            "a banana",
            "more complicated",
            'even more complicated',
            "hello there",
            "hello there",
            "aaa--bbb",
            "aaa--bbb",
            "aaa--bbb",
            "aaa--bbb",
            "aaa--bbb--ccc - - ddd--",
            ["aaa", "bbb"],
            ["- aaa", "bbb--"],
            "aaa bbb--ccc",
            "aaa--bbb ccc ddd",
            "aaa--bbb ccc ddd",
            "aaa--bbb ccc",
            "aaa\tbbb",
            """sss ddd"""
        ]

    for i in xrange(0, len(INPUT)):
        data = {}
        data["aaa"] = {"bbb": INPUT[i]}
        r, c = _get_server_response(json.dumps(data), 'aaa%2Fbbb')
        exp = {}
        exp["aaa"] = {"bbb": EXPECTED[i]}
        print_error_log()
        assert_same_jsons(exp, c)


def test_prop_doesnt_exist():
    """Should return original JSON when prop doesn't exist."""
    x = {"aaa": "BBB"}
    r, c = _get_server_response(json.dumps(x), 'aaa%2Fddd')
    pinfo(x,r,c)
    print_error_log()
    assert_same_jsons(c, x)


def test_missing_prop():
    """Should return 200 when prop is not provided."""
    x = {"aaa": "BBB"}
    r, c = _get_server_response(json.dumps(x))
    assert r['status'] == '200'
    assert_same_jsons(x, c)


def test_json_is_bad():
    """Should return 500 when JSON is bad."""
    x = {"aaa": "BBB"}
    r, c = _get_server_response(json.dumps(x) + "{;-;-;}", 'aaa')
    assert r['status'] == '500'


def test_list_of_properties():
    """Should process all properties."""
    props = ["aaa", "bbb/ccc", "ddd/eee/fff"]

    INPUT = {
            "aaa": "....a -- b....",
            "bbb": {"ccc": ["aaa", "bbb", "'''''x''''"]},
            "ddd": {"eee": {"fff": ["a", "b", "...d..."]}}
    }
    EXPECTED = {
            "aaa": "a--b",
            "bbb": {"ccc": ["aaa", "bbb", "x"]},
            "ddd": {"eee": {"fff": ["a", "b", "d"]}}
    }
    r, c = _get_server_response(json.dumps(INPUT), props)
    assert r['status'] == '200'
    assert_same_jsons(EXPECTED, c)


def test_changes_using_default_prop_value():
    """Should process all default values."""
    INPUT = {
            "hasView": {"format": "... format. "},
            "aaa": "bbb...",
            "sourceResource": {
                "aaa": "bbb...",
                "creator": "....a -- b....",
                "language": ["...aaa...", "...bbb;;;.;."],
                "title": "sss...",
                "publisher": ["that's me.."],
                "relation": [".first;", """   second   relation..... \n. """, "\r\t\n\t\raaaa\r\n\t  ..."],
                "format": "... format.   '''",
                "extent": "...''',,, extent. ''',,,",
                "description": ["... desc 1.  ", "... desc 2.  ''"],
                "rights": "... rights.  ",
                "place": "... place.  ",
            },
            "bbb": "ccc..."
    }
    EXPECTED = {
            "hasView": {"format": "format."},
            "aaa": "bbb...",
            "sourceResource": {
                "aaa": "bbb...",
                "creator": "a--b",
                "language": ["aaa", "bbb"],
                "title": "sss",
                "publisher": ["that's me"],
                "relation": ["first", "second relation", "aaaa"],
                "format": "format.",
                "extent": "extent.",
                "description": ["desc 1.", "desc 2."],
                "rights": "rights.",
                "place": "place.",
            },
            "bbb": "ccc..."
    }
    r, c = _get_server_response(json.dumps(INPUT))
    assert r['status'] == '200'
    assert_same_jsons(EXPECTED, c)
