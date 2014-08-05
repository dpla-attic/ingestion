# -*- coding: utf-8 -*-
from server_support import server, H
from amara.thirdparty import json
from dict_differ import assert_same_jsons

def _get_server_response(body, prop=None):
    """
    Returns response from server using provided url.
    """
    url = server() + "enrich_language"
    return H.request(url, "POST", body=body)

def test_language_set_from_iso639_3_code():
    """Should set language from ISO639-3 code"""
    INPUT = {
        "sourceResource": {
            "language": ["upv", "paf", "ntj", "ndd", "bcf"]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "language": [
                {"iso639_3": "upv", "name": "Uripiv-Wala-Rano-Atchin"},
                {"iso639_3": "paf", "name": u"Paranawát"},
                {"iso639_3": "ntj", "name": "Ngaanyatjarra"},
                {"iso639_3": "ndd", "name": "Nde-Nsele-Nta"},
                {"iso639_3": "bcf", "name": "Bamu"}
            ]
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

def test_language_set_from_iso639_2_code():
    """Should set language from ISO639-2 code"""
    INPUT = {
        "sourceResource": {
            "language": ["aum", "mxy", "mxr", "frm", "ndi"]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "language": [
                {"iso639_3": "aum", "name": "Asu (Nigeria)"},
                {"iso639_3": "mxy", "name": u"Southeastern Nochixtlán Mixtec"},
                {"iso639_3": "mxr", "name": "Murik (Malaysia)"},
                {"iso639_3": "frm", "name": "Middle French (ca. 1400-1600)"},
                {"iso639_3": "ndi", "name": "Samba Leko"}
            ]
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))


def test_language_set_from_iso639_1_code():
    """Should convert ISO639-1 code to ISO639-3 code then set language"""
    INPUT = {
        "sourceResource": {
            "language": ["ce", "fi", "ht", "ho", "id"]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "language": [
                {"iso639_3": "che", "name": "Chechen"},
                {"iso639_3": "fin", "name": "Finnish"},
                {"iso639_3": "hat", "name": "Haitian"},
                {"iso639_3": "hmo", "name": "Hiri Motu"},
                {"iso639_3": "ind", "name": "Indonesian"}
            ]
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

def test_language_set_from_exact_regex_match():
    """Should set language from ISO639-3 language name regex match"""
    INPUT = {
        "sourceResource": {
            "language": ["mUlTiple languAges",
                         "middle French (ca. 1400-1600)",
                         "icelandic sign language"]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "language": [
                {"iso639_3": "mul", "name": "Multiple languages"},
                {"iso639_3": "frm", "name": "Middle French (ca. 1400-1600)"},
                {"iso639_3": "icl", "name": "Icelandic Sign Language"}
            ]
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

def test_language_set_from_word_boundary_regex_match():
    """Should set language from splitting on whitespace and matching against
       ISO639-1 langauge names
    """
    INPUT = {
        "sourceResource": {
            "language": ["Bananas English Spanish", "Apples German Spanish"]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "language": [
                {"iso639_3": "spa", "name": "Spanish"},
                {"iso639_3": "eng", "name": "English"},
                {"iso639_3": "ger", "name": "German"},
                {"iso639_3": "deu", "name": "German"}
            ]
        }
    }
    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

def test_language_set_from_unicode_regex_match():
    INPUT = {
        "sourceResource": {
            "language": u"Southeastern Nochixtlán Mixtec"
        }
    }
    EXPECTED = {
        "sourceResource": {
            "language": [
                {"iso639_3": "mxy", "name": u"Southeastern Nochixtlán Mixtec"}
            ]
        }
    }

    resp, content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
