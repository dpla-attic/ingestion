import sys
from server_support import server

from amara.thirdparty import httplib2
import os
from amara.thirdparty import json
from dict_differ import DictDiffer

def assert_same_jsons(this, that):
    """
    Checks if the dictionaries are the same.
    It compares the keys and values.
    Prints diff if they are not exact and throws exception.
    """
    d = DictDiffer(this, that)

    if not d.same():
        d.print_diff()
        assert this == that

def pinfo(*data):
    """
    Prints all the params in separate lines.
    """
    for d in data:
        print d


CT_JSON = {"Content-Type": "application/json"}
HEADERS = {
            "Content-Type": "application/json",
            "Context": "{}",
          }



H = httplib2.Http()

def test_shred1():
    "Valid shredding"

    INPUT = {
        "id": "999",
        "prop1": "lets,go,bluejays"
    }
    EXPECTED = {
        "id": "999",
        "prop1": ["lets","go","bluejays"]
    }
    url = server() + "shred?prop=prop1"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
    assert str(resp.status).startswith("2")

    assert json.loads(content) == EXPECTED

def test_shred2():
    "Shredding of an unknown property"
    INPUT = {
        "id": "999",
        "prop1": "lets,go,bluejays"
    }
    EXPECTED = INPUT
    url = server() + "shred?prop=prop9"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
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
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
    assert str(resp.status).startswith("2")

    assert json.loads(content) == EXPECTED

def test_shred4():
    "Shredding multiple fields"
    INPUT = {
        "p": ["a,b,c", "d,e,f"]
    }
    EXPECTED = {
        "p": ["a","b","c","d","e","f"]
    }
    url = server() + "shred?prop=p"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
    assert str(resp.status).startswith("2")

    assert json.loads(content) == EXPECTED

def test_shred5():
    "Shredding multiple keys"
    INPUT = {
        "p": "a,b,c",
        "q": "d,e,f"
    }
    EXPECTED = {
        "p": ["a","b","c"],
        "q": ["d","e","f"]
    }
    url = server() + "shred?prop=p,q"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
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
        "prop1": "lets,go,bluejays"
    }
    url = server() + "shred?action=unshred&prop=prop1"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
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
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
    assert str(resp.status).startswith("2")

    assert json.loads(content) == EXPECTED

def test_oaitodpla_date_single():
    "Correctly transform a single date value"
    INPUT = {
        "date" : "1928"
    }
    EXPECTED = {
        u'temporal' : [{
            u'start' : u'1928',
            u'end' : u'1928'
        }]
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['temporal'] == EXPECTED['temporal']

def test_oaitodpla_date_multiple():
    "Correctly transform a multiple date values"
    INPUT = {
        "date" : ["1928", "1406"]
    }
    EXPECTED = {
        u'temporal' : [{
            u'start' : u'1928',
            u'end' : u'1928'
        },
        {
            u'start' : u'1406',
            u'end' : u'1406'
        }
        ]
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['temporal'] == EXPECTED['temporal']


def test_oaitodpla_date_parse_format_yyyy_mm_dd():
    "Correctly transform a date of format YYYY-MM-DD"
    INPUT = {
        "date" : "1928-05-20"
    }
    EXPECTED = {
        u'temporal' : [{
            u'start' : u'1928-05-20',
            u'end' : u'1928-05-20'
        }]
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['temporal'] == EXPECTED['temporal']

def test_oaitodpla_date_parse_format_date_with_slashes():
    "Correctly transform a date of format MM/DD/YYYY"
    INPUT = {
        "date" : "05/20/1928"
    }
    EXPECTED = {
        u'temporal' : [{
            u'start' : u'1928-05-20',
            u'end' : u'1928-05-20'
        }]
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['temporal'] == EXPECTED['temporal']


def test_oaitodpla_date_parse_format_natural_string():
    "Correctly transform a date of format Month, DD, YYYY"
    INPUT = {
        "date" : "May 20, 1928"
    }
    EXPECTED = {
        u'temporal' : [{
            u'start' : u'1928-05-20',
            u'end' : u'1928-05-20'
        }]
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['temporal'] == EXPECTED['temporal']

def test_oaitodpla_date_parse_format_ca_string():
    "Correctly transform a date of format ca. 1928"
    INPUT = {
        "date" : "ca. 1928\n"
    }
    EXPECTED = {
        u'temporal' : [{
            u'start' : u'1928',
            u'end' : u'1928'
        }]
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['temporal'] == EXPECTED['temporal']

def test_oaitodpla_date_parse_format_bogus_string():
    "Deal with a bogus date string"
    INPUT = {
        "date" : "BOGUS!"
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert "temporal" not in result

def test_oaitodpla_date_parse_format_date_range():
    "Correctly transform a date of format 1960 - 1970"
    INPUT = {
        "date" : "1960 - 1970"
    }
    EXPECTED = {
        u'temporal' : [{
            u'start' : u'1960',
            u'end' : u'1970'
        }]
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['temporal'] == EXPECTED['temporal']


def test_oaitodpla_date_parse_format_date_range():
    "Correctly transform a date of format 1960-05-01 - 1960-05-15"
    INPUT = {
        "date" : "1960-05-01 - 1960-05-15"
    }
    EXPECTED = {
        u'temporal' : [{
            u'start' : u'1960-05-01',
            u'end' : u'1960-05-15'
        }]
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['temporal'] == EXPECTED['temporal']

def test_oaitodpla_date_pull_from_coverage_field():
    "Pull a date out of the coverage field"
    INPUT = {
        "date" : "1928-05-20",
        "coverage" : "1800-10-20"
    }
    EXPECTED = {
        u'temporal' : [{
            u'start' : u'1928-05-20',
            u'end' : u'1928-05-20'
        },
        {
            u'start' : u'1800-10-20',
            u'end' : u'1800-10-20'
        }]
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['temporal'] == EXPECTED['temporal']

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

    url = server() + "enrich-subject"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
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

    url = server() + "enrich-subject"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
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

    url = server() + "enrich-subject"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert result['subject'] == EXPECTED['subject']

def test_enrich_type_cleanup():
    "Test type normalization"
    INPUT = {
        "type" : ["Still Images","Text"]
        }
    EXPECTED = {
        u'type' : [ "image", "text" ]
        }

    url = server() + "enrich-type"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert result['type'] == EXPECTED['type']
    
def test_enrich_format_cleanup_multiple():
    "Test format normalization and removal of non IMT formats"
    INPUT = {
        "format" : ["Still Images","image/JPEG","audio","Images", "audio/mp3 (45kb , 12 minuetes)"]
        }
    EXPECTED = {
        u'format' : [ "image/jpeg", "audio", "audio/mp3" ],
        u'TBD_physicalformat' : ["Still Images", "Images"]
        }

    url = server() + "enrich-format"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert result['format'] == EXPECTED['format']
    assert result['TBD_physicalformat'] == EXPECTED['TBD_physicalformat']

def test_enrich_format_cleanup_single():
    "Test format normalization and removal of non IMT formats with one format"
    INPUT = {
        "format" : "image/JPEG"
        }
    EXPECTED = {
        u'format' : "image/jpeg"
        }

    url = server() + "enrich-format"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert result['format'] == EXPECTED['format']
    assert not 'TBD_physicalformat' in result.keys()


def test_identify_preview_location():
    """
    Should add a thumbnail URL made of the source URL.
    """
    INPUT = {
            u"something" : "x",
            u"somethink" : "y",
            u"source" : "http://repository.clemson.edu/u?/scp,104"
    }
    EXPECTED = {
            u"something" : "x",
            u"somethink" : "y",
            u"source" : "http://repository.clemson.edu/u?/scp,104",
            u"preview_source_url" : "http://repository.clemson.edu/cgi-bin/thumbnail.exe?CISOROOT=/scp&CISOPTR=104"
    }
    url = server() + "identify_preview_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)

    assert str(resp.status).startswith("2")
    result = json.loads(content)

    assert_same_jsons(EXPECTED, result)


def test_identify_preview_location_bad_json():
    """
    Should get 500 from akara for bad json.
    """
    INPUT = [ "{...}", "{aaa:'bbb'}", "xxx" ]
    for i in INPUT:
        url = server() + "identify_preview_location"
        resp,content = H.request(url,"POST",body=i,headers=HEADERS)
        assert resp.status == 500

def test_identify_preview_location_missing_source_field():
    """
    Should return original JSON if the 'source' field is missing.
    """
    INPUT = '{"aaa":"bbb"}'
    url = server() + "identify_preview_location"
    resp,content = H.request(url,"POST",body=INPUT,headers=HEADERS)

    pinfo(url,resp,content)

    assert_same_jsons(INPUT, content)

def test_identify_preview_location_bad_url():
    """
    Should return original JSON for bad URL.
    """
    bad_urls = [ u"http://repository.clemson.edu/uscp104",
        u"http://repository.clemson.edu/s?/scp,104",
        u"http://repository.clemson.edu/u/scp,104",
        u"http://repository.clemson.edu/u?/scp104",
        u"http://repository.clemson.edu/u?/scp",
        u"http://repository.clemson.edu/",
            ]
    INPUT = {
            u"something" : u"x",
            u"somethink" : u"y",
            u"source" : u""
    }
    for bad_url in bad_urls:
        INPUT[u"source"] = bad_url
        url = server() + "identify_preview_location"
        print url
        resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
        assert_same_jsons(INPUT, content)

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
