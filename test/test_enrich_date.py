from amara.thirdparty import json
from nose.tools import nottest

from server_support import server, H, print_error_log
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
    print_error_log()
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
        print_error_log()
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
        print_error_log()
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
        print_error_log()
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
    INPUT = {
        "date" : "19th c."
    }
    EXPECTED = {
        u'date' : {
            u'begin' : u'1800',
            u'end' : u'1899',
            "displayDate" : "19th c."
        }
    }

    url = server() + "enrich_earliest_date?prop=date"

    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")

    result = json.loads(content)
    assert result['date'] == EXPECTED[u'date'], "%s != %s" % (result['date'], EXPECTED[u'date'])


def test_enrich_date_parse_century_date_with_P():
    """Correctly transform a date of format ['19th c.', 'P']"""
    INPUT = {
        "date" : ["19th c.", "P"]
    }
    EXPECTED = {
        u'date' : {
            u'begin' : u'1800',
            u'end' : u'1899',
            "displayDate" : "19th c."
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
    print_error_log()
    assert resp.status == 200

    url = server() + "enrich_date"
    resp, content = H.request(url, "POST", body=content)
    print_error_log()
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
        print_error_log()
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
        print_error_log()
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
        print_error_log()
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
        print_error_log()
        assert str(resp.status).startswith("2")
        assert_same_jsons(expected, content)

def test_year_month():
    """Should recognize YYYY-MM and not YYYY-YY"""
    INPUT = ["1940/2", "1940/02", "1940 / 2", "1940 / 02",
             "1940-2", "1940-02", "1940 - 2", "1940 - 02"]
             #"1938-08-23/24", "1938/08/23-24"]


    url = server() + "enrich_earliest_date?prop=date"
    for date in INPUT:
        d = "1940-02"
        input = {"date": date}
        expected = {"date": {"begin": d, "end": d, "displayDate": date}}

        resp, content = H.request(url, "POST", body=json.dumps(input))
        print_error_log()
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
        print_error_log()
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
        print_error_log()
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
        print_error_log()
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
        print_error_log()
        assert str(resp.status).startswith("2")
        assert_same_jsons(expected, content)

def test_date_with_parentheses_and_question_mark():
    """Should handle date like 1928 (?)"""
    INPUT = {"date": "1928 (?)"}
    EXPECTED = {"date": {"begin": "1928", "end": "1928", "displayDate": "1928 (?)"}}

    url = server() + "enrich_earliest_date?prop=date"

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    print_error_log()
    assert str(resp.status).startswith("2")
    assert_same_jsons(EXPECTED, content)

def test_wordy_date():
    """Should handle very wordy dates"""
    INPUT = {"date": "mid 11th century AH/AD 17th century (Mughal)"}
    EXPECTED = {"date": {"begin": None, "end": None, "displayDate": INPUT["date"]}}

    url = server() + "enrich_earliest_date?prop=date"

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    print_error_log()
    assert str(resp.status).startswith("2")
    assert_same_jsons(EXPECTED, content)

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
