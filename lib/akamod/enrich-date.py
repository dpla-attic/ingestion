import re
import timelib

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dateutil.parser import parse as dateutil_parse
from zen import dateparser

from dplaingestion.selector import getprop, setprop, exists



HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'
# default date used by dateutil-python to populate absent date elements during parse,
# e.g. "1999" would become "1999-01-01" instead of using the current month/day
# Set this to a date far in the future, so we can use it to check if date parsing just failed
DEFAULT_DATETIME_STR = "3000-01-01"
DEFAULT_DATETIME = dateutil_parse(DEFAULT_DATETIME_STR)

# normal way to get DEFAULT_DATIMETIME in seconds is:
# time.mktime(DEFAULT_DATETIME.timetuple())
# but it applies the time zone, which should be added to seconds to get real GMT(UTC)
# as simple solution, hardcoded UTC seconds is given
DEFAULT_DATETIME_SECS = 32503680000.0 # UTC seconds for "3000-01-01"


DATE_RANGE_RE = r'(\S+)\s*-\s*(\S+)'
DATE_RANGE_EXT_RE = r'(\S+)\s*[-/]\s*(\S+)'
def split_date(d):
    reg = DATE_RANGE_EXT_RE
    if len(d.split("/")) == 3: #so th date is like "2001 / 01 / 01"
        reg = DATE_RANGE_RE
    range = [robust_date_parser(x) for x in re.search(reg,d).groups()]
    return range

DATE_8601 = '%Y-%m-%d'
def robust_date_parser(d):
    """
    Robust wrapper around some date parsing libs, making a best effort to return
    a single 8601 date from the input string. No range checking is performed, and
    any date other than the first occuring will be ignored.

    We use timelib for its ability to make at least some sense of invalid dates,
    e.g. 2012/02/31 -> 2012/03/03

    We rely only on dateutil.parser for picking out dates from nearly arbitrary
    strings (fuzzy=True), but at the cost of being forgiving of invalid dates
    in those kinds of strings.

    Returns None if it fails
    """
    circa_re = re.compile("(ca\.|c\.)", re.I)
    dd = dateparser.to_iso8601(re.sub(circa_re, "", d, count=0).strip()) # simple cleanup prior to parse
    if dd is None:
        try:
            dd = dateutil_parse(d, fuzzy=True, default=DEFAULT_DATETIME)
            if dd.year == DEFAULT_DATETIME.year:
                dd = None
        except Exception:
            try:
                dd = timelib.strtodatetime(d, now=DEFAULT_DATETIME_SECS)
            except ValueError:
                pass
            except Exception as e:
                logger.error(e)

        if dd:
            ddiso = dd.isoformat()
            return ddiso[:ddiso.index('T')]
    return dd

year_range = re.compile("(\d{4})\s*[-/]\s*(\d{4})") # simple for digits year range
circa_range = re.compile("(?:ca\.|c\.)\s*(?P<century>\d{2})(?P<year_begin>\d{2})\s*-\s*(?P<year_end>\d{2})", re.I) # tricky "c. 1970-90" year range
century_date = re.compile("(?P<century>\d{1,2})(?:th|st|nd|rd)\s+c\.", re.I) # for dates with centuries "19th c."
def parse_date_or_range(d):
    # FIXME: could be more robust here,
    # e.g. use date range regex to handle:
    # June 1941 - May 1945
    # 1941-06-1945-05
    # and do not confuse with just YYYY-MM-DD regex
    if ' - ' in d or (len(d.split("/")) == 2) or year_range.match(d):
        a, b = split_date(d)
    elif circa_range.match(d):
        match = circa_range.match(d)
        year_begin = match.group("century") + match.group("year_begin")
        year_end = match.group("century") + match.group("year_end")
        a, b = robust_date_parser(year_begin), robust_date_parser(year_end)
    elif century_date.match(d):
        match = century_date.match(d)
        year_begin = (int(match.group("century"))-1) * 100
        year_end = year_begin + 99
        a, b = str(year_begin), str(year_end)
    else:
        parsed = robust_date_parser(d)
        a, b = parsed, parsed
    return a, b


def remove_brackets_and_strip(d):
    """Removed brackets from the date (range)."""
    return re.sub(r"(^\s*\[\s*|\s*\]\s*$)", '', d).strip()


def test_parse_date_or_range():
    DATE_TESTS = {
        "ca. July 1896": ("1896-07", "1896-07"), # fuzzy dates
        "c. 1896": ("1896", "1896"), # fuzzy dates
        "c. 1890-95": ("1890", "1895"), # fuzzy date range
        "1999.11.01": ("1999-11-01", "1999-11-01"), # period delim
        "2012-02-31": ("2012-03-02", "2012-03-02"), # invalid date cleanup
        "12-19-2010": ("2010-12-19", "2010-12-19"), # M-D-Y
        "5/7/2012": ("2012-05-07", "2012-05-07"), # slash delim MDY
        "1999 - 2004": ("1999", "2004"), # year range
        "1999-2004": ("1999", "2004"), # year range without spaces
        " 1999   -   2004  ": ("1999", "2004"), # range whitespace
    }
    for i in DATE_TESTS:
        res = parse_date_or_range(i)
        assert res == DATE_TESTS[i], "For input '%s', expected '%s' but got '%s'"%(i,DATE_TESTS[i],res)


def convert_dates(data, prop, earliest):
    """Converts dates.

    Arguments:
        data     Dict - Data for conversion.
        prop     Str  - Properties dividided with comma.
        earliest Bool - True  - the function will set only the earliest date.
                        False - the function will set all dates.

    Returns:
        Nothing, the replacement is done in place.
    """
    dates = []
    for p in prop.split(','):
        if exists(data, p):
            v = getprop(data, p)

            for s in (v if not isinstance(v, basestring) else [v]):
                for part in s.split(";"):
                    stripped = remove_brackets_and_strip(part)
                    a, b = parse_date_or_range(stripped)
                    if b != '3000-01-01':
                        dates.append( {
                                "begin": a,
                                "end": b,
                                "displayDate" : stripped
                            })

    dates.sort(key=lambda d: d["begin"] if d["begin"] is not None else DEFAULT_DATETIME_STR)

    value_to_set = dates
    if earliest:
        value_to_set = dates[0]

    if value_to_set:
        setprop(data, p, value_to_set)
    else:
        delprop(data, p)


@simple_service('POST', 'http://purl.org/la/dp/enrich_earliest_date', 'enrich_earliest_date', HTTP_TYPE_JSON)
def enrich_earliest_date(body, ctype, action="enrich_earliest_date", prop="aggregatedCHO/date"):
    """
    Service that accepts a JSON document and extracts the "created date" of the item, using the
    following rules:

    a) Looks in the list of fields specified by the 'prop' parameter
    b) Extracts all dates, and sets the created date to the earliest date 
    """
    try :
        data = json.loads(body)
    except:
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE,  HTTP_TYPE_TEXT)
        return "Unable to parse body as JSON"

    convert_dates(data, prop, True)
    return json.dumps(data)


@simple_service('POST', 'http://purl.org/la/dp/enrich_date', 'enrich_date', HTTP_TYPE_JSON)
def enrich_date(body, ctype, action="enrich_date", prop="aggregatedCHO/temporal"):
    """
    Service that accepts a JSON document and extracts the "created date" of the item, using the
    following rules:

    a) Looks in the list of fields specified by the 'prop' parameter
    b) Extracts all dates
    """
    try :
        data = json.loads(body)
    except:
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return "Unable to parse body as JSON"

    convert_dates(data, prop, False)
    return json.dumps(data)
