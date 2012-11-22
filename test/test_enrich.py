import sys
from server_support import server

from amara.thirdparty import httplib2
import os
from amara.thirdparty import json

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

def test_oaitodpla_subject_single():
    "Correctly map a single subject"
    INPUT = {
        "subject" : "Dolls -- Ceramic"
    }
    EXPECTED = {
        u'subject' : [{
            u'name' : u'Dolls -- Ceramic'
        }]
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['subject'] == EXPECTED['subject']

def test_oaitodpla_subject_multiple():
    "Correctly map a single subject"
    INPUT = {
        "subject" : ["Dolls -- Ceramic", "Dolls -- Plastic"]
    }
    EXPECTED = {
        u'subject' : [{
            u'name' : u'Dolls -- Ceramic'
        },
        {
            u'name' : u'Dolls -- Plastic'
        }
        ]
    }

    url = server() + "oai-to-dpla"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['subject'] == EXPECTED['subject']

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



if __name__ == "__main__":
    raise SystemExit("Use nosetests")
