import re
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

REGEXPS = ('\.',''), ('\(',''), ('\)',''), ('-',''), (',','')

@simple_service('POST', 'http://purl.org/la/dp/enrich_location', 'enrich_location', 'application/json')
def enrichlocation(body,ctype,action="enrich_location", prop="sourceResource/spatial"):
    """
    Service that accepts a JSON document and enriches the "spatial" field of that document by
    iterating through the spatial fields and mapping to the state and iso3166-2, if not already
    mapped, through teh get_isostate function. This function takes the optional parameter abbrev,
    and if it is set it will search the fields for State name abbreviations. If a previous provider-
    specific location enrichment module ran, the default is to not search those fields for State name
    abbreviations, but only for full State names.
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data,prop):
        v = getprop(data,prop)

        # If prior provider-specific location enrichment occured,
        # v[0] will be a dictrionary
        if isinstance(v[0], dict):
            for k in v[0].keys():
                v[0][k] = remove_space_around_semicolons(v[0][k])

            if 'state' in v[0]:
                # Handle case where a previous provider-specific location
                # enrichment set the state field
                isostate = get_isostate(v[0]['state'])
                # It may be the case that the 'state' field does not contain a
                # State name
                if isostate[0]:
                    v[0]['iso3166-2'] = isostate[0]
                    v[0]['state'] = isostate[1]
                else:
                    # We may want to keep whatever non-State value was placed in
                    # state
                    v[0]['name'] = v[0]['state']
                    # Remove bogus state
                    del v[0]['state']
            else:
                # Handle case where a previous provider-specific location
                # enrichment did not set the state field
                for val in v[0].values():
                    isostate = get_isostate(val)
                    if isostate[0]:
                        v[0]['iso3166-2'] = isostate[0]
                        v[0]['state'] = isostate[1]
                        break
        else:
            # Handle the case where no previous provider-specific location
            # enrichment occured. Convert spatial from list of strings to
            # dictionary.
            sp = []
            for s in (v if not isinstance(v, basestring) else [v]):
                d= {}
                d['name'] = remove_space_around_semicolons(s)
                isostate = get_isostate(d['name'], abbrev="Yes")
                if isostate[0]:
                    d['iso3166-2'] = isostate[0]
                    d['state'] = isostate[1]
                sp.append(d)
            v = sp

        # If any of the spatial fields contain semi-colons, we need to create
        # multiple dictionaries.
        semicolons = None
        for d in v:
            for k in d.keys():
                if d[k] and ';' in d[k]:
                    semicolons = True
                    break

        setprop(data,prop,(create_dictionaries(v) if semicolons else v))

    return json.dumps(data)

def get_isostate(strg, abbrev=None):
    if not isinstance(strg, basestring):
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Non-string parameter supplied to get_isostate"

    iso_arr, state_arr = [], []
    for s in strg.split(";"):
        states = from_abbrev(s) if abbrev else s
        for state in (states if isinstance(states, list) else [states]):
            append_empty_strings = True
            for st in STATES:
                if st in state.upper():
                    iso_arr.append(STATES[st])
                    state_arr.append(st.title())
                    append_empty_strings = None
            if append_empty_strings:
                iso_arr.append("")
                state_arr.append("")

    iso = None
    state = None
    if filter(None, iso_arr):
        iso = ';'.join(iso_arr)
        if state_arr:
            state = ';'.join(state_arr)
    return (iso, state)

def from_abbrev(strg):
    states = []
    for pattern, replace in REGEXPS:
        strg = re.sub(pattern, replace, strg)
    # First check against STATES iso values, minus the "US-"
    for state, iso in STATES.iteritems():
        st = re.sub('US-','',iso)
        match = re.compile(r'\b({0})\b'.format(st)).search(strg.upper())
        if match:
            s = match.group(0)
            # Check against cases like "in"/"as"/etc
            low = re.compile(r'\b({0})\b'.format(s.lower())).search(strg)
            if not low:
                states.append(state.title())
    # If no matches, check againts the ABBREV values
    if not states:
        for state,abbrevs in ABBREV.iteritems():
            for abbrev in abbrevs.split(';'):
                match = re.compile(r'\b({0})\b'.format(abbrev)).search(strg.upper())
                if match:
                    states.append(state)
                break
    if not states:
        states.append(strg)
    return states

def create_dictionaries(data):
    # Handle multiple values separated by semi-colons in spatial fields
    dicts = []
    for d in data:
        all = [] 
        for k,v in d.iteritems():
            all.append(v.split(";"))
            all[-1].insert(0,k)

        if all:
            # Get the length of the longest array
            total = len(max(all, key=len))

            # Create dictionaries
            for i in range(1,total):
                dict = {}
                for item in all:
                    if i < len(item) and item[i]:
                        dict[item[0]] = item[i]
                # Don't add duplicates 
                if dict not in dicts: 
                    dicts.append(dict)
    return filter(None, dicts)

def remove_space_around_semicolons(strg):
    strg_arr = strg.split(';')
    for i in range(len(strg_arr)):
        strg_arr[i] = re.sub('^  *','',strg_arr[i])
        strg_arr[i] = re.sub('^\[','',strg_arr[i])
        strg_arr[i] = re.sub(' *$','',strg_arr[i])
    strg = ';'.join(strg_arr)
    return strg

STATES = {
    'ALASKA':'US-AK',
    'ALABAMA':'US-AL',
    'ARKANSAS':'US-AR',
    'AMERICAN SAMOA':'US-AS',
    'ARIZONA':'US-AZ',
    'CALIFORNIA':'US-CA',
    'COLORADO':'US-CO',
    'CONNECTICUT':'US-CT',
    'DISTRICT OF COLUMBIA':'US-DC',
    'DELAWARE':'US-DE',
    'FLORIDA':'US-FL',
    'GEORGIA':'US-GA',
    'GUAM':'US-GU',
    'HAWAII':'US-HI',
    'IOWA':'US-IA',
    'IDAHO':'US-ID',
    'ILLINOIS':'US-IL',
    'INDIANA':'US-IN',
    'KANSAS':'US-KS',
    'KENTUCKY':'US-KY',
    'LOUISIANA':'US-LA',
    'MASSACHUSETTS':'US-MA',
    'MARYLAND':'US-MD',
    'MAINE':'US-ME',
    'MICHIGAN':'US-MI',
    'MINNESOTA':'US-MN',
    'MISSOURI':'US-MO',
    'NORTHERN MARIANA ISLANDS':'US-MP',
    'MISSISSIPPI':'US-MS',
    'MONTANA':'US-MT',
    'NATIONAL':'US-NA',
    'NORTH CAROLINA':'US-NC',
    'NORTH DAKOTA':'US-ND',
    'NEBRASKA':'US-NE',
    'NEW HAMPSHIRE':'US-NH',
    'NEW JERSEY':'US-NJ',
    'NEW MEXICO':'US-NM',
    'NEVADA':'US-NV',
    'NEW YORK':'US-NY',
    'OHIO':'US-OH',
    'OKLAHOMA':'US-OK',
    'OREGON':'US-OR',
    'PENNSYLVANIA':'US-PA',
    'PUERTO RICO':'US-PR',
    'RHODE ISLAND':'US-RI',
    'SOUTH CAROLINA':'US-SC',
    'SOUTH DAKOTA':'US-SD',
    'TENNESSEE':'US-TN',
    'TEXAS':'US-TX',
    'UTAH':'US-UT',
    'VIRGINIA':'US-VA',
    'VIRGIN ISLANDS':'US-VI',
    'VERMONT':'US-VT',
    'WASHINGTON':'US-WA',
    'WISCONSIN':'US-WI',
    'WEST VIRGINIA':'US-WV',
    'WYOMING':'US-WY'
}

ABBREV = {
    "Pennsylvania": "PEN;PENN",
    "Massachusetts": "MASS"
}
