import re
import timelib
import datetime
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dateutil.parser import parse as dateutil_parse
from zen import dateparser
from dplaingestion.selector import getprop, setprop, delprop, exists
from dplaingestion.utilities import iterify, clean_date, \
                                    remove_brackets_and_strip

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

def out_of_range(d):
    ret = None
    try:
        dateutil_parse(d, fuzzy=True, default=DEFAULT_DATETIME)
    except ValueError:
        ret = True

    return ret

# EDTF: http://www.loc.gov/standards/datetime/pre-submission.html
# (like ISO-8601 but doesn't require timezone)
edtf_date_and_time = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

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
    # Function for a formatted date string, since datetime.datetime.strftime()
    # only works with years >= 1900.
    return_date = lambda d: "%d-%02d-%02d" % (d.year, d.month, d.day)

    # Check for EDTF timestamp first, because it is simple.
    if edtf_date_and_time.match(d):
        try:
            dateinfo = dateutil_parse(d)
            return return_date(dateinfo)
        except TypeError:
            # not parseable by dateutil_parse()
            dateinfo = None
    isodate = dateparser.to_iso8601(d)
    if isodate is None or out_of_range(d):
        try:
            dateinfo = dateutil_parse(d, fuzzy=True, default=DEFAULT_DATETIME)
            if dateinfo.year == DEFAULT_DATETIME.year:
                dateinfo = None
        except Exception:
            try:
                dateinfo = timelib.strtodatetime(d, now=DEFAULT_DATETIME_SECS)
            except ValueError:
                dateinfo = None
            except Exception as e:
                logger.error("Exception %s in %s" % (e, __name__))

        if dateinfo:
            return return_date(dateinfo)

    return isodate

# ie 1970/1971
year_range = re.compile("(?P<year1>^\d{4})[-/](?P<year2>\d{4})$")
# ie 1970-08-01/02
day_range = re.compile("(?P<year>^\d{4})[-/](?P<month>\d{1,2})[-/](?P<day_begin>\d{1,2})[-/](?P<day_end>\d{1,2}$)")
# ie 1970-90
circa_range = re.compile("(?P<century>\d{2})(?P<year_begin>\d{2})[-/](?P<year_end>\d{1,2})")
# ie 9-1970
month_year = re.compile("(?P<month>\d{1,2})[-/](?P<year>\d{4})")
# ie 195- 
decade_date = re.compile("(?P<year>\d{3})-")
# ie 1920s
decade_date_s = re.compile("(?P<year>\d{4})s")
# ie between 2000 and 2002
between_date = re.compile("between\s*(?P<year1>\d{4})\s*and\s*(?P<year2>\d{4})")

def parse_date_or_range(d):
    #TODO: Handle dates with BC, AD, AH
    #      Handle ranges like 1920s - 1930s
    #      Handle ranges like 11th - 12th century
    a, b = None, None

    if re.search("B\.?C\.?|A\.?D\.?|A\.?H\.?", d.upper()):
        pass
    is_edtf_timestamp = edtf_date_and_time.match(d)
    hyphen_split = d.split("-")
    slash_split = d.split("/")
    ellipse_split = d.split("..")
    is_hyphen_split = (len(hyphen_split) % 2 == 0)
    is_slash_split = (len(slash_split) % 2 == 0)
    is_ellipse_split = (len(ellipse_split) % 2 == 0)
    if year_range.match(d):
        match = year_range.match(d)
        a, b = sorted((match.group("year1"), match.group("year2")))
    elif (is_hyphen_split or is_slash_split or is_ellipse_split) \
            and not is_edtf_timestamp:
        # We passed over EDTF timestamps because they contain hyphens and we
        # can handle them below.  Note that we don't deal with ranges of
        # timestamps.
        #
        # Handle ranges
        if is_hyphen_split:
            delim = "-"
            split_result = hyphen_split
        elif is_slash_split:
            delim = "/"
            split_result = slash_split
        elif is_ellipse_split:
            delim = ".."
            split_result = ellipse_split
        if day_range.match(d):
            # ie 1970-08-01/02
            match = day_range.match(d)
            a = "%s-%s-%s" % (match.group("year"), match.group("month"),
                              match.group("day_begin"))
            b = "%s-%s-%s" % (match.group("year"),match.group("month"),
                              match.group("day_end"))
        elif decade_date.match(d):
            match = decade_date.match(d)
            a = match.group("year") + "0"
            b = match.group("year") + "9"
        elif any([0 < len(s) < 4
                  for s in split_result
                  if len(split_result) == 2]):
            # ie 1970-90, 1970/90, 1970-9, 1970/9, 9/1979
            match = circa_range.match(d)
            if match:
                year_begin = match.group("century") + match.group("year_begin")
                year_end = match.group("century") + match.group("year_end")
                if int(year_begin) < int(year_end):
                    # ie 1970-90
                    a = robust_date_parser(year_begin)
                    b = robust_date_parser(year_end)
                else:
                    # ie 1970-9
                    (y, m) = split_result
                    # If the second number is a month, format it to two digits
                    # and use "-" as the delim for consistency in the
                    # dateparser.to_iso8601 result
                    if int(m) in range(1,13):
                        d = "%s-%02d" % (y, int(m))
                    else:
                        # ie 1970-13
                        # Just use the year
                        d = y

                    a = robust_date_parser(d)
                    b = robust_date_parser(d)
            else:
                match = month_year.match(d)
                if match:
                    d = "%s-%02d" % (match.group("year"), int(match.group("month")))
                    a = robust_date_parser(d)
                    b = robust_date_parser(d)
        elif "" in split_result:
            # ie 1970- or -1970 (but not 19uu- nor -19uu)
            s = split_result
            if len(s[0]) == 4 and "u" not in s[0]:
                a, b = s[0], None
            elif len(s[1]) == 4 and "u" not in s[1]:
                a, b = None, s[1]
            else:
                a, b = None, None
        else:
            # ie 1970-01-01-1971-01-01, 1970 Fall/August, 1970 April/May, or
            # wordy date like "mid 11th century AH/AD 17th century (Mughal)"
            d = d.replace(" ", "")
            d = d.split(delim)
            begin = delim.join(d[:len(d)/2])
            end = delim.join(d[len(d)/2:])

            # Check if month in begin or end
            m1 = re.sub("[-\d/]", "", begin)
            m2 = re.sub("[-\d/]", "", end)
            if m1 or m2:
                # ie 2004July/August, 2004Fall/Winter, or wordy date
                begin, end = None, None

                # Extract year
                for v in d:
                    y = re.sub(r"(?i)[a-z]", "", v)
                    if len(y) == 4:
                        begin = y + m1.capitalize()
                        end = y + m2.capitalize()
                        if not dateparser.to_iso8601(begin) or not\
                               dateparser.to_iso8601(end):
                            begin, end = y, y
                        break

            if begin:
                a, b = robust_date_parser(begin), robust_date_parser(end)
    elif decade_date_s.match(d):
        match = decade_date_s.match(d)
        year_begin = match.group("year")
        year_end = match.group("year")[:3] + "9"
        a, b = year_begin, year_end
    elif between_date.match(d):
        match = between_date.match(d)
        year1 = int(match.group("year1"))
        year2 = int(match.group("year2"))
        a, b = str(min(year1, year2)), str(max(year1, year2))
    else:
        # This picks up a variety of things, in addition to timestamps.
        parsed = robust_date_parser(d)
        a, b = parsed, parsed

    return a, b

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
        " 1999 - 2004 ": ("1999", "2004"), # range whitespace
    }
    for i in DATE_TESTS:
        i = clean_date(i)
        res = parse_date_or_range(i)
        assert res == DATE_TESTS[i], "For input '%s', expected '%s' but got '%s'"%(i,DATE_TESTS[i],res)

def is_year_range_list(value):
    """Returns True if value is a list of years in increasing order, else False
    """
    return isinstance(value, list) and \
           all(v.isdigit() for v in value) and \
           value == sorted(value, key=int)

def convert_dates(data, prop, earliest):
    """Converts dates.

    Arguments:
    data Dict - Data for conversion.
    prop Str - Properties dividided with comma.
    earliest Bool - True - the function will set only the earliest date.
    False - the function will set all dates.

    Returns:
    Nothing, the replacement is done in place.
    """
    for p in prop.split(','):
        dates = []
        if exists(data, p):
            v = getprop(data, p)
            if not isinstance(v, dict):
                if is_year_range_list(v):
                    dates.append( {
                        "begin": v[0],
                        "end": v[-1],
                        "displayDate": "%s-%s" % (v[0], v[-1])
                    })
                else:
                    for s in (v if not isinstance(v, basestring) else [v]):
                        for part in s.split(";"):
                            display_date = remove_brackets_and_strip(part)
                            stripped = clean_date(display_date)
                            if len(stripped) < 4:
                                continue
                            a, b = parse_date_or_range(stripped)
                            if b != DEFAULT_DATETIME_STR:
                                dates.append( {
                                        "begin": a,
                                        "end": b,
                                        "displayDate" : display_date
                                    })
            else:
                # Already filled in, probably by mapper
                continue

            dates.sort(key=lambda d: d["begin"] if d["begin"] is not None
                                                else DEFAULT_DATETIME_STR)
            if dates:
                if earliest:
                    value_to_set = dates[0]
                else:
                    value_to_set = dates
                setprop(data, p, value_to_set)
            else:
                delprop(data, p)

def check_date_format(data, prop):
    """Checks that the begin and end dates are in the proper format"""
    date = getprop(data, prop, True)
    if date:
        for d in iterify(date):
            for k, v in d.items():
                if v and k != "displayDate":
                    try:
                        ymd = [int(s) for s in v.split("-")]
                    except:
                        err = "Invalid date.%s: non-integer in %s for %s" % \
                              (k, v, data.get("_id"))
                        logger.error(err)
                        setprop(d, k, None)
                        continue

                    year = ymd[0]
                    month = ymd[1] if len(ymd) > 1 else 1
                    day = ymd[2] if len(ymd) > 2 else 1
                    try:
                        datetime.datetime(year=year, month=month, day=day)
                    except ValueError, e:
                        logger.error("Invalid date.%s: %s for %s" %
                                     (k, e, data.get("_id")))
                        setprop(d, k, None)

@simple_service('POST', 'http://purl.org/la/dp/enrich_earliest_date', 'enrich_earliest_date', HTTP_TYPE_JSON)
def enrich_earliest_date(body, ctype, action="enrich_earliest_date", prop="sourceResource/date"):
    """
    Service that accepts a JSON document and extracts the "created date" of the item, using the
    following rules:

    a) Looks in the list of fields specified by the 'prop' parameter
    b) Extracts all dates, and sets the created date to the earliest date
    c) Checks the format of the date
    """
    try :
        data = json.loads(body)
    except:
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return "Unable to parse body as JSON"

    convert_dates(data, prop, True)
    check_date_format(data, prop)
    return json.dumps(data)


@simple_service('POST', 'http://purl.org/la/dp/enrich_date', 'enrich_date', HTTP_TYPE_JSON)
def enrich_date(body, ctype, action="enrich_date", prop="sourceResource/temporal"):
    """
    Service that accepts a JSON document and extracts the "created date" of the item, using the
    following rules:

    a) Looks in the list of fields specified by the 'prop' parameter
    b) Extracts all dates
    c) Checks the format of the date
    """
    try :
        data = json.loads(body)
    except:
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return "Unable to parse body as JSON"

    convert_dates(data, prop, False)
    check_date_format(data, prop)
    return json.dumps(data)
