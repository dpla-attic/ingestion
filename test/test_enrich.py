import sys
from server_support import server, print_error_log, H

import os
from amara.thirdparty import json
from dict_differ import DictDiffer, assert_same_jsons, pinfo
from nose.tools import nottest


def test_shred1():
    "Valid shredding"

    INPUT = {
        "id": "999",
        "prop1": "lets;go;bluejays"
    }
    EXPECTED = {
        "id": "999",
        "prop1": ["lets","go","bluejays"]
    }
    url = server() + "shred?prop=prop1"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    assert json.loads(content) == EXPECTED

def test_shred2():
    "Shredding of an unknown property"
    INPUT = {
        "id": "999",
        "prop1": "lets;go;bluejays"
    }
    EXPECTED = INPUT
    url = server() + "shred?prop=prop9"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    assert json.loads(content) == EXPECTED

def test_shred3():
    "Shredding with a non-default delimeter"
    INPUT = {
        "p":"a,d,f ,, g"
    }
    EXPECTED = {
        "p": ["a,d,f", ",,", "g"]
    }
    url = server() + "shred?prop=p&delim=%20"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    assert json.loads(content) == EXPECTED

def test_shred4():
    "Shredding multiple fields"
    INPUT = {
        "p": ["a;b;c", "d;e;f"]
    }
    EXPECTED = {
        "p": ["a","b","c","d","e","f"]
    }
    url = server() + "shred?prop=p"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    assert json.loads(content) == EXPECTED

def test_shred5():
    "Shredding multiple keys"
    INPUT = {
        "p": "a;b;c",
        "q": "d;e;f"
    }
    EXPECTED = {
        "p": ["a","b","c"],
        "q": ["d","e","f"]
    }
    url = server() + "shred?prop=p,q"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    assert json.loads(content) == EXPECTED

def test_shred6():
    "Shredding multiple keys/fields; remove duplicates"
    INPUT = {
        "p": "a;b;c;a",
        "q": "d;e;f;e",
        "r": ["a;b;c;a", "d;e;f;e"]
    }
    EXPECTED = {
        "p": ["a","b","c"],
        "q": ["d","e","f"],
        "r": ["a","b","c","d","e","f"]
    }
    url = server() + "shred?prop=p,q,r"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    assert json.loads(content) == EXPECTED

def test_shred7():
    "Shredding multiple keys/fields; do not remove duplicates"
    INPUT = {
        "p": "a;b;c;a",
        "q": "d;e;f;e",
        "r": ["a;b;c;a", "d;e;f;e"]
    }
    EXPECTED = {
        "p": ["a","b","c","a"],
        "q": ["d","e","f","e"],
        "r": ["a","b","c","a","d","e","f","e"]
    }
    url = server() + "shred?prop=p,q,r&keepdup=True"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    assert json.loads(content) == EXPECTED

def test_unshred1():
    "Valid unshredding"

    INPUT = {
        "id": "999",
        "prop1": ["lets","go","bluejays"]
    }
    EXPECTED = {
        "id": "999",
        "prop1": "lets;go;bluejays"
    }
    url = server() + "shred?action=unshred&prop=prop1"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    assert json.loads(content) == EXPECTED

def test_unshred2():
    "Unshredding of an unknown property"
    INPUT = {
        "id": "999",
        "prop1": ["lets","go","bluejays"]
    }
    EXPECTED = INPUT
    url = server() + "shred?action=unshred&prop=prop9"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    assert json.loads(content) == EXPECTED

@nottest
def test_oaitodpla_date_parse_format_ca_string():
    "Correctly transform a date of format ca. 1928"
    INPUT = {
        "date" : "ca. 1928\n"
    }
    EXPECTED = {
        u'temporal' : [{
            u'begin' : u'1928',
            u'end' : u'1928'
        }]
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['temporal'] == EXPECTED['temporal']

@nottest
def test_oaitodpla_date_parse_format_bogus_string():
    "Deal with a bogus date string"
    INPUT = {
        "date" : "BOGUS!"
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert "temporal" not in result

def test_enrich_multiple_subject_reformat_to_dict():
    "Transform an array of strings of subjects to an array of dictionaries"
    INPUT = {
        "subject" : ["Cats","Dogs","Mice"]
        }
    EXPECTED = {
        u'subject' : [
            {u'name' : u'Cats'},
            {u'name' : u'Dogs'},
            {u'name' : u'Mice'}
            ]
        }

    url = server() + "enrich-subject?prop=subject"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert result['subject'] == EXPECTED['subject']

def test_enrich_single_subject_reformat_to_dict():
    "Transform a subjects string to an array of dictionaries"
    INPUT = {
        "subject" : "Cats"
        }
    EXPECTED = {
        u'subject' : [
            {u'name' : u'Cats'}
            ]
        }

    url = server() + "enrich-subject?prop=subject"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert result['subject'] == EXPECTED['subject']

def test_enrich_subject_cleanup():
    "Test spacing correction on '--' and remove of trailing periods"
    INPUT = {
        "subject" : ["Cats","Dogs -- Mean","Mice."]
        }
    EXPECTED = {
        u'subject' : [
            {u'name' : u'Cats'},
            {u'name' : u'Dogs--Mean'},
            {u'name' : u'Mice'}
            ]
        }

    url = server() + "enrich-subject?prop=subject"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert result['subject'] == EXPECTED['subject']

def test_enrich_type_cleanup():
    "Test type normalization"
    INPUT = {
        "type" : ["Still Images", "Moving Images", "Moving Image", "Text","Statue"]
        }
    EXPECTED = {
        u'type' : [ "image", "moving image", "moving image", "text" ],
        u'TBD_physicalformat' : ["Statue"]
        }

    url = server() + "enrich-type?prop=type&alternate=TBD_physicalformat"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    pinfo(content)
    assert result['type'] == EXPECTED['type']


def test_enrich_format_cleanup_multiple():
    "Test format normalization and removal of non IMT formats"
    INPUT = {
        "format" : ["Still Images","image/JPEG","audio","Images",
            'application',  "audio/mp3 (1.46 MB; 1 min., 36 sec.)",
            "Still Images","image/JPEG","audio","Images",
            'application',  "audio/mp3 (1.46 MB; 1 min., 36 sec.)",
            "Images/jpeg", "images/jpeg"
            ]
        }
    EXPECTED = {
        u'format' : [ "image/jpeg", "audio/mpeg" ],
        u'physicalmedium' : ["Still Images", "audio", "Images", 'application'],
        u'type': "image"
        }

    url = server() + "enrich-format?prop=format&alternate=physicalmedium&typefield=type"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    print_error_log()
    assert_same_jsons(EXPECTED, content)
    assert str(resp.status).startswith("2")


def test_enrich_format_cleanup():
    "Test format normalization and removal of non IMT formats with one format"
    INPUT = {
        "format" : "image/JPEG"
        }
    EXPECTED = {
        u'format' : "image/jpeg",
        u"type": "image"
        }

    url = server() + "enrich-format?prop=format&alternate=physicalmedium&typefield=type"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert result['format'] == EXPECTED['format']
    assert not 'physicalmedium' in result.keys()


def test_physical_format_from_format_and_type():
    """
    Test physical format appending from format and type fields
    """
    INPUT = {
        "format": ["76.8 x 104 cm", "Oil on canvas"],
        "type": ["Paintings", "Painting"]
    }
    EXPECTED = {
        "TBD_physicalformat": ["Paintings", "Painting", "76.8 x 104 cm", "Oil on canvas"]
    }

    resp, content = H.request(server() + "enrich-type?prop=type&alternate=TBD_physicalformat", "POST", body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    assert json.loads(content)['TBD_physicalformat'] == ["Paintings", "Painting"]
    resp, content = H.request(server() + "enrich-format?prop=format&alternate=TBD_physicalformat", "POST", body=content)
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert result['TBD_physicalformat'] == EXPECTED['TBD_physicalformat']


def test_setting_missing_type_for_missing_format():
    "Should not change anything."
    INPUT = {
                "format": None,
                "type": ""
    }
    js = json.dumps(INPUT)

    url = server() + "enrich-format?prop=format&alternate=physicalmedium&typefield=type"

    resp,content = H.request(url, "POST", body=js)
    pinfo(content)
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert_same_jsons(INPUT, content)


def test_setting_missing_type_from_format():
    "Should set type according to format field."
    INPUT = {
            "aggregatedCHO": {
                "format": "image/jpg",
            }
    }
    EXPECTED = {
            "aggregatedCHO": {
                "format": "image/jpeg",
                "type": "image"
        }
    }
    js = json.dumps(INPUT)

    url = server() + "enrich-format?prop=aggregatedCHO/format&alternate=aggregatedCHO/physicalmedium&typefield=aggregatedCHO/type"

    resp,content = H.request(url, "POST", body=js)
    pinfo(content)
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert_same_jsons(EXPECTED, content)


def test_setting_empty_type_from_format():
    "Should set empty type according to format field according to type mapping."

    DATA = [
      {"in": {"format": "audio/mp3"},         "out": {"format": "audio/mpeg",    "type": "sound"}},
      {"in": {"format": "image/jpg"},         "out": {"format": "image/jpeg",    "type": "image"}},
      {"in": {"format": "video/mpeg"},        "out": {"format": "video/mpeg",    "type": "moving image"}},
      {"in": {"format": "text/calendar"},     "out": {"format": "text/calendar", "type": "text"}},
      {"in": {"format": "audio"},             "out": {"format": None,            "physicalmedium": "audio"}},
      {"in": {"format": "something strange"}, "out": {"format": None,            "physicalmedium": "something strange"}},
    ]

    FORMATS = ["audio"]
    EXPECTED_TYPES = []
    INPUT = {
        "aggregatedCHO": {
            "format": "FORMAT",
            "type": ""
        }
    }
    EXPECTED = {
        "aggregatedCHO": {
            "format": "FORMAT",
            "type": "TYPE"
        }
    }

    for d in DATA:
        INPUT["aggregatedCHO"] = d["in"]
        js = json.dumps(INPUT)
        url = server() + "enrich-format?prop=aggregatedCHO/format&alternate=aggregatedCHO/physicalmedium"

        resp, content = H.request(url, "POST", body=js)
        #pinfo(content)
        pinfo(INPUT)
        assert str(resp.status).startswith("2")

        EXPECTED["aggregatedCHO"] = d["out"]
        assert_same_jsons(EXPECTED, content)


if __name__ == "__main__":
    raise SystemExit("Use nosetests")
