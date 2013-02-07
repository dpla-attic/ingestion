from amara.thirdparty import httplib2
from amara.thirdparty import json
from nose.tools import nottest

from server_support import server


CT_JSON = {"Content-Type": "application/json"}
HEADERS = {
    "Content-Type": "application/json",
    "Context": "{}",
    }

H = httplib2.Http()

def test_enrich_dates_bogus_date():
    """Correctly transform a date value that cannot be parsed"""
    INPUT = {
        "date" : "could be 1928ish?"
    }
    EXPECTED = {
        u'date' : {
            'start' : None,
            'end' : None,
            'displayDate' : 'could be 1928ish?'
        }
    }

    url = server() + "enrich-date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date']


def test_enrich_date_single():
    """Correctly transform a single date value"""
    INPUT = {
        "date" : "1928"
    }
    EXPECTED = {
        u'date' : {
            'start' : u'1928',
            'end' : u'1928',
            'displayDate' : '1928'
        }
    }

    url = server() + "enrich-date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date']

def test_enrich_date_date_multiple():
    """Correctly transform a multiple date value, and take the earliest"""
    INPUT = {
        "date" : ["1928", "1406"]
    }
    EXPECTED = {
        u'date' : {
            u'start' : u'1406',
            u'end' : u'1406',
            'displayDate' : '1406'
        }
    }

    url = server() + "enrich-date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date']


def test_enrich_date_date_parse_format_yyyy_mm_dd():
    """Correctly transform a date of format YYYY-MM-DD"""
    INPUT = {
        "date" : "1928-05-20"
    }
    EXPECTED = {
        'date' : {
            'start' : u'1928-05-20',
            'end' : u'1928-05-20',
            'displayDate' : '1928-05-20'
        }
    }

    url = server() + "enrich-date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED['date']

def test_enrich_date_parse_format_date_with_slashes():
    """Correctly transform a date of format MM/DD/YYYY"""
    INPUT = {
        "date" : "05/20/1928"
    }
    EXPECTED = {
        u'date' : {
            u'start' : u'1928-05-20',
            u'end' : u'1928-05-20',
            'displayDate' : '05/20/1928'
        }
    }

    url = server() + "enrich-date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date']


def test_enrich_date_date_parse_format_natural_string():
    """Correctly transform a date of format Month, DD, YYYY"""
    INPUT = {
        "date" : "May 20, 1928"
    }
    EXPECTED = {
        'date' : {
            'start' : u'1928-05-20',
            'end' : u'1928-05-20',
            'displayDate' : 'May 20, 1928'
        }
    }

    url = server() + "enrich-date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED['date']

def test_enrich_date_date_parse_format_ca_string():
    """Correctly transform a date with circa abbreviation (ca.)"""
    INPUT = {
        "date" : "ca. May 1928"
    }
    EXPECTED = {
        'date' : {
            'start' : u'1928-05',
            'end' : u'1928-05',
            'displayDate' : 'ca. May 1928'
        }
    }

    url = server() + "enrich-date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED['date']

def test_enrich_date_date_parse_format_c_string():
    """Correctly transform a date with circa abbreviation (c.)"""
    INPUT = {
        "date" : "c. 1928"
    }
    EXPECTED = {
        'date' : {
            'start' : u'1928',
            'end' : u'1928',
            'displayDate' : 'c. 1928'
        }
    }

    url = server() + "enrich-date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED['date']

def test_enrich_date_parse_format_date_range1():
    """Correctly transform a date of format 1960 - 1970"""
    INPUT = {
        "date" : "1960 - 1970"
    }
    EXPECTED = {
        u'date' : {
            u'start' : u'1960',
            u'end' : u'1970',
            "displayDate" : "1960 - 1970"
        }
    }

    url = server() + "enrich-date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date']

def test_enrich_date_parse_format_date_range2():
    """Correctly transform a date of format 1960-05-01 - 1960-05-15"""
    INPUT = {
        "date" : "1960-05-01 - 1960-05-15"
    }
    EXPECTED = {
        u'date' : {
            u'start' : u'1960-05-01',
            u'end' : u'1960-05-15',
            "displayDate" : "1960-05-01 - 1960-05-15"
        }
    }

    url = server() + "enrich-date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date']

def test_enrich_date_parse_format_date_range3():
    """Correctly transform a date of format 1960-1970"""
    INPUT = {
        "date" : "1960-1970"
    }
    EXPECTED = {
        u'date' : {
            u'start' : u'1960',
            u'end' : u'1970',
            "displayDate" : "1960-1970"
        }
    }

    url = server() + "enrich-date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date']

def test_enrich_date_parse_format_date_range4():
    """Correctly transform a date of format 'c. YYYY-YY'"""
    INPUT = {
        "date" : "c. 1960-70"
    }
    EXPECTED = {
        u'date' : {
            u'start' : u'1960',
            u'end' : u'1970',
            "displayDate" : "c. 1960-70"
        }
    }

    url = server() + "enrich-date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=HEADERS)
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date'], "%s != %s" % (result['date'], EXPECTED[u'date'])


if __name__ == "__main__":
    raise SystemExit("Use nosetests")
