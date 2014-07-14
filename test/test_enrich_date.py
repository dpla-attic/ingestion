from amara.thirdparty import json
from nose.tools import nottest
from dplaingestion.akamod.enrich_date import check_date_format
from server_support import server, H
from dict_differ import DictDiffer, assert_same_jsons, pinfo
import sys


def test_enrich_dates_bogus_date():
    """Correctly transform a date value that cannot be parsed"""
    INPUT = {
        "date" : "could be 1928ish?"
    }
    EXPECTED = {
        u'date' : {
            'begin' : None,
            'end' : None,
            'displayDate' : 'could be 1928ish?'
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))

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
            'begin' : u'1928',
            'end' : u'1928',
            'displayDate' : '1928'
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date']

def test_enrich_pre_1900_date():
    """Pre-1900 date is transformed correctly"""
    # Lest anyone try to use datetime.datetime.strftime() in
    # enrich_date_robust_date_parser(), as this does not handle pre-1900 dates.
    INPUT = {
        "date": "1870-01-02T12:00:00"
    }
    EXPECTED = {
        u'date': {
            'begin': u'1870-01-02',
            'end': u'1870-01-02',
            'displayDate': '1870-01-02T12:00:00'
        }
    }
    url = server() + "enrich_earliest_date?prop=date"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    result = json.loads(content)
    print result
    assert result['date'] == EXPECTED[u'date']

def test_enrich_date_date_multiple():
    """Correctly transform a multiple date value, and take the earliest"""
    INPUT = {
        "date" : ["1928", "1406"]
    }
    EXPECTED = {
        u'date' : {
                u'begin':       u'1406',
                u'end':         u'1406',
                u'displayDate': u'1406'
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    assert_same_jsons(EXPECTED, content)


def test_enrich_date_date_parse_format_yyyy_mm_dd():
    """Correctly transform a date of format YYYY-MM-DD"""
    INPUT = {
        "date" : "1928-05-20"
    }
    EXPECTED = {
        'date' : {
            'begin' : u'1928-05-20',
            'end' : u'1928-05-20',
            'displayDate' : '1928-05-20'
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    pinfo(resp, content)
    result = json.loads(content)
    assert result['date'] == EXPECTED['date']


def test_enrich_date_parse_format_date_with_slashes():
    """Correctly transform a date of format MM/DD/YYYY"""
    INPUT = {
        "date" : "05/20/1928"
    }
    EXPECTED = {
        u'date' : {
            u'begin' : u'1928-05-20',
            u'end' : u'1928-05-20',
            'displayDate' : '05/20/1928'
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
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
            'begin' : u'1928-05-20',
            'end' : u'1928-05-20',
            'displayDate' : 'May 20, 1928'
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert result['date'] == EXPECTED['date']


def test_enrich_date_date_parse_format_natural_string_no_comma():
    """Correctly transform a date of format Month DD YYYY"""
    INPUT = {
        "date" : "May 20 1928"
    }
    EXPECTED = {
        'date' : {
            'begin' : u'1928-05-20',
            'end' : u'1928-05-20',
            'displayDate' : 'May 20 1928'
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
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
            'begin' : u'1928-05',
            'end' : u'1928-05',
            'displayDate' : 'ca. May 1928'
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
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
            'begin' : u'1928',
            'end' : u'1928',
            'displayDate' : 'c. 1928'
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
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
            u'begin' : u'1960',
            u'end' : u'1970',
            "displayDate" : "1960 - 1970"
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date']


def test_date_with_brackets():
    """Should transform date with brackets."""

    ranges = [
        "[1960-05-01]",
        "[  1960-05-01  ]"
    ]

    for r in ranges:
        INPUT = {"date": r}
        EXPECTED = {
            u'date' : {
                u'begin' : u'1960-05-01',
                u'end' : u'1960-05-01',
                "displayDate" : "1960-05-01"
            }
        }

        url = server() + "enrich_earliest_date?prop=date"

        resp, content = H.request(url, "POST", body=json.dumps(INPUT))
        assert str(resp.status).startswith("2")
        assert_same_jsons(EXPECTED, content)


def test_range_years_with_brackets():
    """Should transform dates range with brackets."""
    ranges = [
            ("1960 - 1963]",    "1960 - 1963"),
            ("[ 1960 - 1963 ]", "1960 - 1963"),
            ("[1960 / 1963]",   "1960 / 1963"),
            ("[ 1960 / 1963 ]", "1960 / 1963"),
    ]

    for r in ranges:
        INPUT = {"date": r[0]}
        EXPECTED = {
            u'date' : {
                u'begin' : u'1960',
                u'end' : u'1963',
                "displayDate" : r[1]
            }
        }

        url = server() + "enrich_earliest_date?prop=date"

        resp, content = H.request(url, "POST", body=json.dumps(INPUT))
        assert str(resp.status).startswith("2")
        assert_same_jsons(EXPECTED, content)


def test_range_with_brackets():
    """Should transform date range with brackets."""

    ranges = [
            ("1960-05-01 - 1960-05-15",     "1960-05-01 - 1960-05-15"),
            ("[ 1960-05-01 - 1960-05-15 ]", "1960-05-01 - 1960-05-15"),
            ("[1960-05-01 - 1960-05-15]",   "1960-05-01 - 1960-05-15"),
            ("[1960-05-01 / 1960-05-15]",   "1960-05-01 / 1960-05-15"),
            ("[1960-05-01/1960-05-15]",   "1960-05-01/1960-05-15"),
    ]

    for r in ranges:
        INPUT = {"date": r[0]}
        EXPECTED = {
            u'date' : {
                u'begin' : u'1960-05-01',
                u'end' : u'1960-05-15',
                "displayDate" : r[1]
            }
        }

        url = server() + "enrich_earliest_date?prop=date"

        resp, content = H.request(url, "POST", body=json.dumps(INPUT))
        assert str(resp.status).startswith("2")
        assert_same_jsons(EXPECTED, content)



def test_enrich_date_parse_format_date_range2():
    """Correctly transform a date of format 1960-05-01 - 1960-05-15"""
    INPUT = {
        "date" : "1960-05-01 - 1960-05-15"
    }
    EXPECTED = {
        u'date' : {
            u'begin' : u'1960-05-01',
            u'end' : u'1960-05-15',
            "displayDate" : "1960-05-01 - 1960-05-15"
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
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
            u'begin' : u'1960',
            u'end' : u'1970',
            "displayDate" : "1960-1970"
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
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
            u'begin' : u'1960',
            u'end' : u'1970',
            "displayDate" : "c. 1960-70"
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date'], "%s != %s" % (result['date'], EXPECTED[u'date'])



def test_enrich_date_parse_century_date():
    """Correctly transform a date of format '19th c.'"""
    url = server() + "enrich_earliest_date?prop=date"
    INPUT = {"date": "19th c."}
    EXPECTED = {
        "date": {
            "begin": None,
            "end": None,
            "displayDate": "19th c"  # period stripped assumed OK
        }
    }
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    result = json.loads(content)
    assert result["date"] == EXPECTED["date"], \
           "%s != %s" % (result["date"], EXPECTED["date"])
    INPUT = {"date": "19th century"}
    EXPECTED = {
        "date": {
            "begin": None,
            "end": None,
            "displayDate": "19th century"
        }
    }
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    result = json.loads(content)
    assert result["date"] == EXPECTED["date"], \
           "%s != %s" % (result["date"], EXPECTED["date"])


def test_enrich_date_parse_century_date_with_P():
    """Correctly transform a date of format ['19th c.', 'P']"""
    INPUT = {
        "date" : ["19th c.", "P"]
    }
    EXPECTED = {
        u'date' : {
            u'begin' : None,
            u'end' : None,
            u"displayDate" : u"19th c"
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    assert_same_jsons(EXPECTED, content)
    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date'], "%s != %s" % (result['date'], EXPECTED[u'date'])


def test_enrich_temporal_date():
    """Correctly enrich temporal dates"""

    INPUT = {
        "sourceResource": {
            "spatial" : [
                "1901-1999",
                " 1901 - 1999 ",
                " 1901 / 01 / 01",
                "1905-04-12",
                "01/01/1901",
                "1901",
                "North Carolina"
            ]}
    }
    EXPECTED = {
        "sourceResource": {
            "temporal": [
                {"begin": "1901", "end": "1999", "displayDate": "1901-1999"},
                {"begin": "1901", "end": "1999", "displayDate": "1901 - 1999"},
                {"begin": "1901", "end": "1901", "displayDate": "1901"},
                {"begin": "1901-01-01", "end": "1901-01-01", "displayDate": "1901 / 01 / 01"},
                {"begin": "1901-01-01", "end": "1901-01-01", "displayDate": "01/01/1901"},
                {"begin": "1905-04-12", "end": "1905-04-12", "displayDate": "1905-04-12"},
            ],
            "spatial" : ["North Carolina"]}
    }

    url = server() + "move_date_values?prop=sourceResource/spatial"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200

    url = server() + "enrich_date"
    resp, content = H.request(url, "POST", body=content)
    assert resp.status == 200
    assert_same_jsons(EXPECTED, content)


def test_enrich_date_date_parse_format_natural_string_for_multiple_dates():
    """Correctly transform a date of format Month, DD, YYYY"""
    INPUT = {
        "date" : "May 20, 1928; 2002-01-01"
    }
    EXPECTED = {
        'date' : {
              'begin':       u'1928-05-20',
              'end':         u'1928-05-20',
              'displayDate': u'May 20, 1928'
              }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    result = json.loads(content)
    assert_same_jsons(EXPECTED, content)

def test_no_date_field():
    """Handle case where date field doesn't exist"""
    INPUT = {
        "hat" : "fits"
    }
    EXPECTED = {
        "hat" : "fits"
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    result = json.loads(content)

def test_dates_with_question_marks():
    """Handle dates with question marks"""
    INPUT = ["1928?", "1928? - 1929?", "1928?/1929?",
             "1928? / 1929?", "1928? - 1929", "1928 - 1929?"]
    EXPECTED = [
        {"begin": "1928", "end": "1928", "displayDate": "1928?"},
        {"begin": "1928", "end": "1929", "displayDate": "1928? - 1929?"},
        {"begin": "1928", "end": "1929", "displayDate": "1928?/1929?"},
        {"begin": "1928", "end": "1929", "displayDate": "1928? / 1929?"},
        {"begin": "1928", "end": "1929", "displayDate": "1928? - 1929"},
        {"begin": "1928", "end": "1929", "displayDate": "1928 - 1929?"}
    ]

    url = server() + "enrich_earliest_date?prop=date"
    for i in range(len(INPUT)):
        input = {"date": INPUT[i]}
        expected = {"date": EXPECTED[i]}

        resp, content = H.request(url, "POST", body=json.dumps(input))
        assert str(resp.status).startswith("2")
        assert_same_jsons(content, expected)


def test_decade_date():
    """Should convert decades strings."""
    INPUT = ["195-", "198-", "201-",
            "1920s", "1950s", "1900s",
            ]
    EXPECTED = [
        {"begin": "1950", "end": "1959", "displayDate": "195-"},
        {"begin": "1980", "end": "1989", "displayDate": "198-"},
        {"begin": "2010", "end": "2019", "displayDate": "201-"},
        {"begin": "1920", "end": "1929", "displayDate": "1920s"},
        {"begin": "1950", "end": "1959", "displayDate": "1950s"},
        {"begin": "1900", "end": "1909", "displayDate": "1900s"},
    ]
    for i in xrange(len(INPUT)):
        url = server() + "enrich_earliest_date?prop=date"
        input = {"date": INPUT[i]}
        expected = {"date": EXPECTED[i]}

        resp, content = H.request(url, "POST", body=json.dumps(input))
        assert str(resp.status).startswith("2")
        assert_same_jsons(expected, content)

def test_bogus_date():
    """Should handle bogus dates"""
    INPUT = ["abcdef", "a-bcdef", "a - bcdef", "a-bcde-f", "a - bcde - f",
             "a/bcdef", "a / bcdef", "a/bcde/f", "a / bcde / f"]

    url = server() + "enrich_earliest_date?prop=date"
    for i in range(len(INPUT)):
        input = {"date": INPUT[i]}
        expected = {"date": {"begin": None, "end": None, "displayDate": INPUT[i]}}

        resp, content = H.request(url, "POST", body=json.dumps(input))
        assert str(resp.status).startswith("2")
        assert_same_jsons(expected, content)

def test_dates_with_between():
    INPUT = [
            "between 1840 and 1860",
            "between 1860 and 1840"
            ]
    url = server() + "enrich_earliest_date?prop=date"
    for i in range(len(INPUT)):
        input = {"date": INPUT[i]}
        expected = {"date": {"begin": "1840", "end": "1860", "displayDate": INPUT[i]}}

        resp, content = H.request(url, "POST", body=json.dumps(input))
        assert str(resp.status).startswith("2")
        assert_same_jsons(expected, content)

def test_tricky_dates1():
    """Should handle tricky dates"""
    INPUT = ["ca. 1850 - c 1856", "c 1850 - 1856", " 1850 c. - c.1856 ",
             "ca.1850 - ca.1856", "1850ca - 1856ca", "1850 to 1856",
             "c 1850 to c 1856"]

    url = server() + "enrich_earliest_date?prop=date"
    for date in INPUT:
        input = {"date": date}
        expected = {
            "date": {
                "begin": "1850",
                "end": "1856",
                "displayDate": date.strip()
            }
        }

        resp, content = H.request(url, "POST", body=json.dumps(input))
        assert str(resp.status).startswith("2")
        assert_same_jsons(expected, content)

def test_tricky_dates2():
    """Should handle day range"""
    INPUT = ["1938-08-23/24", "1938-08-23-24",
             "1938/08/23-24", "1938/08/23/24"]

    url = server() + "enrich_earliest_date?prop=date"
    for date in INPUT:
        input = {"date": date}
        expected = {"date": {"begin": "1938-08-23", "end": "1938-08-24", "displayDate": date}}

        resp, content = H.request(url, "POST", body=json.dumps(input))
        assert str(resp.status).startswith("2")
        assert_same_jsons(expected, content)

def test_year_month():
    """Should recognize YYYY-MM and not YYYY-YY"""
    INPUT = ["1940/2", "1940/02", "1940 / 2", "1940 / 02",
             "1940-2", "1940-02", "1940 - 2", "1940 - 02",
             "2/1940", "02/1940", "2 / 1940", "02 / 1940",
             "2-1940", "02-1940", "2 - 1940", "02 - 1940"]

    url = server() + "enrich_earliest_date?prop=date"
    for date in INPUT:
        d = "1940-02"
        input = {"date": date}
        expected = {"date": {"begin": d, "end": d, "displayDate": date}}

        resp, content = H.request(url, "POST", body=json.dumps(input))
        assert str(resp.status).startswith("2")
        assert_same_jsons(expected, content)

def test_day_out_of_range():
    """Should handle day out of range dates"""
    INPUT = ["1940-06-31", "1950/02/29"]
    EXPECTED = ["1940-07-01", "1950-03-01"]

    url = server() + "enrich_earliest_date?prop=date"
    for i in range(len(INPUT)):
        input = {"date": INPUT[i]}
        expected = {"date": {"begin": EXPECTED[i], "end": EXPECTED[i], "displayDate": INPUT[i]}}

        resp, content = H.request(url, "POST", body=json.dumps(input))
        assert str(resp.status).startswith("2")
        assert_same_jsons(expected, content)

def test_full_date_range():
    """Should handle full date range"""
    INPUT = ["1901-01-01-1902-01-01", "1901-01-01/1902-01-01",
             "1901/01/01-1902/01/01", "1901/01/01/1902/01/01",
             "01/01/1901-01/01/1902", "1/1/1901/1/1/1902",
             "01-01-1901/01-01-1902", "1-1-1901-1-1-1902"]

    url = server() + "enrich_earliest_date?prop=date"
    for i in range(len(INPUT)):
        input = {"date": INPUT[i]}
        expected = {"date": {"begin": "1901-01-01", "end": "1902-01-01", "displayDate": INPUT[i]}}

        resp, content = H.request(url, "POST", body=json.dumps(input))
        assert str(resp.status).startswith("2")
        assert_same_jsons(expected, content)

def test_delim_with_months():
    """Should handle date with delim and seasons"""
    INPUT = ["2004 July/August", "July/August 2004",
             "2004 July-August", "July-August 2004"]

    url = server() + "enrich_earliest_date?prop=date"
    for i in range(len(INPUT)):
        input = {"date": INPUT[i]}
        expected = {"date": {"begin": "2004-07", "end": "2004-08", "displayDate": INPUT[i]}}

        resp, content = H.request(url, "POST", body=json.dumps(input))
        assert str(resp.status).startswith("2")
        assert_same_jsons(expected, content)
   
def test_delim_with_seasons():
    """Should handle date with delim seasons"""
    INPUT = ["2004 Fall/Winter", "Fall/Winter 2004",
             "2004 Fall-Winter", "Fall-Winter 2004"]

    url = server() + "enrich_earliest_date?prop=date"
    for i in range(len(INPUT)):
        input = {"date": INPUT[i]}
        expected = {"date": {"begin": "2004", "end": "2004", "displayDate": INPUT[i]}}

        resp, content = H.request(url, "POST", body=json.dumps(input))
        assert str(resp.status).startswith("2")
        assert_same_jsons(expected, content)

def test_date_with_parentheses_and_question_mark():
    """Should handle date like 1928 (?)"""
    INPUT = {"date": "1928 (?)"}
    EXPECTED = {"date": {"begin": "1928", "end": "1928", "displayDate": "1928 (?)"}}

    url = server() + "enrich_earliest_date?prop=date"

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    assert_same_jsons(EXPECTED, content)

def test_wordy_date():
    """Should handle very wordy dates"""
    INPUT = {"date": "mid 11th century AH/AD 17th century (Mughal)"}
    EXPECTED = {"date": {"begin": None, "end": None, "displayDate": INPUT["date"]}}

    url = server() + "enrich_earliest_date?prop=date"

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    assert_same_jsons(EXPECTED, content)

def test_reversed_date_range():
    """Should handle reversed date range"""
    INPUT = {"date": "1911/0140"}
    EXPECTED = {"date": {"begin": "0140", "end": "1911", "displayDate": INPUT["date"]}}

    url = server() + "enrich_earliest_date?prop=date"

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    assert_same_jsons(EXPECTED, content)

def test_only_begin_or_end_date():
    """Should handle ranges with only begin or end date"""
    INPUT = [{"date": "1970-"}, {"date": "-1970"}, {"date": "19750-"}]
    EXPECTED = [{"date": {"begin": "1970", "end": None, "displayDate": "1970-"}},
                {"date": {"begin": None, "end": "1970", "displayDate": "-1970"}},
                {"date": {"begin": None, "end": None, "displayDate": "19750-"}}]

    url = server() + "enrich_earliest_date?prop=date"

    for i in range(len(INPUT)):
        resp, content = H.request(url, "POST", body=json.dumps(INPUT[i]))
        assert str(resp.status).startswith("2")
        assert_same_jsons(EXPECTED[i], content)

def test_enrich_dates_with_tildes_and_x():
    """Should remove tildes and x characters from dates"""
    INPUT = [{"date": "1946-10x"}, {"date": "1946-10~"}]
    EXPECTED = [{"date": {"begin": "1946-10", "end": "1946-10", "displayDate": "1946-10x"}},
                {"date": {"begin": "1946-10", "end": "1946-10", "displayDate": "1946-10~"}}]

    url = server() + "enrich_earliest_date?prop=date"

    for i in range(len(INPUT)):
        resp, content = H.request(url, "POST", body=json.dumps(INPUT[i]))
        assert str(resp.status).startswith("2")
        assert_same_jsons(EXPECTED[i], content)

def test_invalid_begin_dates():
    """Should set invalid begin dates to None"""
    INPUT = {
                "_id": "12345",
                "date": [
                    {"begin": "20130-11-12", "end": "2013-11-12", "displaDate": "2013-11-12"},
                    {"begin": "2013-110-12", "end": "2013-11-12", "displaDate": "2013-11-12"},
                    {"begin": "20130-11-120", "end": "2013-11-12", "displaDate": "2013-11-12"},
                    {"begin": "20130-11-a", "end": "2013-11-12", "displaDate": "2013-11-12"},
                    {"begin": "20130-11-", "end": "2013-11-12", "displaDate": "2013-11-12"}
                ]
            }
    EXPECTED = {"begin": None, "end": "2013-11-12", "displaDate": "2013-11-12"}

    check_date_format(INPUT, "date")
    for date in INPUT["date"]:
        assert date == EXPECTED

def test_invalid_end_dates():
    """Should set invalid end dates to None"""
    INPUT = {
                "_id": "12345",
                "date": [
                    {"end": "20130-11-12", "begin": "2013-11-12", "displaDate": "2013-11-12"},
                    {"end": "2013-110-12", "begin": "2013-11-12", "displaDate": "2013-11-12"},
                    {"end": "20130-11-120", "begin": "2013-11-12", "displaDate": "2013-11-12"},
                    {"end": "20130-11-a", "begin": "2013-11-12", "displaDate": "2013-11-12"},
                    {"end": "20130-11-", "begin": "2013-11-12", "displaDate": "2013-11-12"}
                ]
            }
    EXPECTED = {"end": None, "begin": "2013-11-12", "displaDate": "2013-11-12"}

    check_date_format(INPUT, "date")
    for date in INPUT["date"]:
        assert date == EXPECTED

def test_enrich_dates_square_brackets():
    """Should remove square brackets"""
    INPUT = {"date": "[199?]-"}
    EXPECTED = {"date": {"begin": "1990", "end": "1999", "displayDate": "199?-"}}

    url = server() + "enrich_earliest_date?prop=date"

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    assert_same_jsons(EXPECTED, content)

def test_enrich_dates_range_with_u():
    """Should not set begin and/or end if 'u' characters in date"""
    INPUT = [{"date": "18uu-"}, {"date": "-18uu"}, {"date": "18uu-199uu"}]
    EXPECTED = {"date": {"begin": None, "end": None}}

    url = server() + "enrich_earliest_date?prop=date"

    for i in range(len(INPUT)):
        resp, content = H.request(url, "POST", body=json.dumps(INPUT[i]))
        assert str(resp.status).startswith("2")
        EXPECTED["date"]["displayDate"] = INPUT[i]["date"]
        assert_same_jsons(EXPECTED, content)

def test_date_from_timestamp():
    """Date value is extracted from a timestamp"""
    INPUT = [{"date": "2003-12-27T09:07:05Z"},
             {"date": "2003-12-27T09:07:05+01:00"},
             {"date": "2003-12-27T09:07:05-04"},
             {"date": "2003-12-27T09:07:05"}]
    EXPECTED = {"date": {"end": "2003-12-27", "begin": "2003-12-27"}}
    url = server() + "enrich_earliest_date?prop=date"
    for obj in INPUT:
        print obj["date"]
        resp, content = H.request(url, "POST", body=json.dumps(obj))
        EXPECTED["date"]["displayDate"] = obj["date"]
        assert_same_jsons(EXPECTED, content)

def test_date_with_ellipses():
    """Date values are extracted from uncertain dates (with ellipses)"""
    INPUT = {"date": "[1993-08-05..1993-08-08]"}
    EXPECTED = {
                "date": {
                         "displayDate": "1993-08-05..1993-08-08",
                         "end": "1993-08-08",
                         "begin": "1993-08-05"
                        }
                }
    url = server() + "enrich_earliest_date?prop=date"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert_same_jsons(EXPECTED, content)

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
