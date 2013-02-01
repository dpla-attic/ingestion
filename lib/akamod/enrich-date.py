from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dateutil.parser import parse as dateutil_parse
import re
import timelib
from zen import dateparser

@simple_service('POST', 'http://purl.org/la/dp/enrich-date', 'enrich-date', 'application/json')
def enrichdate(body,ctype,action="enrich-format",prop="date"):
    '''   
    Service that accepts a JSON document and extracts the "created date" of the item, using the
    following rules:

    a) Looks in the list of fields specified by the 'prop' parameter
    b) Extracts all dates, and sets the created date to the earliest date 
    '''   

    # default date used by dateutil-python to populate absent date elements during parse,
    # e.g. "1999" would become "1999-01-01" instead of using the current month/day
    # Set this to a date far in the future, so we can use it to check if date parsing just failed
    DEFAULT_DATETIME = dateutil_parse("3000-01-01") 

    DATE_RANGE_RE = r'(\S+)\s*-\s*(\S+)'
    def split_date(d):
        range = [robust_date_parser(x) for x in re.search(DATE_RANGE_RE,d).groups()]
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
        dd = dateparser.to_iso8601(d.replace('ca.','').strip()) # simple cleanup prior to parse
        if dd is None:
            try:
                dd = dateutil_parse(d,fuzzy=True,default=DEFAULT_DATETIME)
                if dd.year == DEFAULT_DATETIME.year:
                    dd = None
            except:
                try:
                    dd = timelib.strtodatetime(d)
                except ValueError:
                    pass
        
            if dd:
                ddiso = dd.isoformat()
                return ddiso[:ddiso.index('T')]
        return dd

    def parse_date_or_range(d):
        if ' - ' in d: # FIXME could be more robust here, e.g. use year regex
            a,b = split_date(d)
        else:
            parsed = robust_date_parser(d)
            a,b = parsed,parsed
        return a,b

    DATE_TESTS = {
        "ca. July 1896": ("1896-07","1896-07"), # fuzzy dates
        "1999.11.01": ("1999-11-01","1999-11-01"), # period delim
        "2012-02-31": ("2012-03-02","2012-03-02"), # invalid date cleanup
        "12-19-2010": ("2010-12-19","2010-12-19"), # M-D-Y
        "5/7/2012": ("2012-05-07","2012-05-07"), # slash delim MDY
        "1999 - 2004": ("1999","2004"), # year range
        " 1999   -   2004  ": ("1999","2004"), # range whitespace
        }

    def test_parse_date_or_range():
        for i in DATE_TESTS:
            res = parse_date_or_range(i)
            assert res == DATE_TESTS[i], "For input '%s', expected '%s' but got '%s'"%(i,DATE_TESTS[i],res)
            
    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    date_candidates = []
    for p in prop.split(','):
        if p in data:
            date_candidates = []
            for s in (data[p] if not isinstance(data[p],basestring) else [data[p]]):
                a,b = parse_date_or_range(s)
                date_candidates.append( {
                        "start": a,
                        "end": b,
                        "displayDate" : s
                        })
        date_candidates.sort(key=lambda d: d["start"] if d["start"] is not None else "9999-12-31")
        if len(date_candidates) > 0:
            data[prop] = date_candidates[0]            

    return json.dumps(data)
