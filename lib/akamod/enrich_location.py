import re
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

REGEXPS = ('\.',''), ('\(',''), ('\)',''), ('-',''), (',','')

@simple_service('POST', 'http://purl.org/la/dp/enrich_location', 'enrich_location', 'application/json')
def enrichlocation(body,ctype,action="enrich_location", prop="spatial"):
    """
    Service that accepts a JSON document and enriches the "spatial" field of that document by
    iterating through the spatial fields and mapping to the state and iso3166-2, if not already
    mapped, through teh get_isostate function. This function takes the optional parameter frm_abbrev,
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

    if prop in data:
        # Remove any spaces around semicolons first
        for k in data[prop][0]:
            data[prop][0][k] = remove_space_around_semicolons(data[prop][0][k])

        if 'state' in data[prop][0]:
            # Handle case where a previous provider-specific location enrichment
            # set the state field
            isostate = get_isostate(data[prop][0]['state'])
            # It may be the case that the 'state' field does not contain a State name
            if isostate[0]:
                data[prop][0]['iso3166-2'] = isostate[0]
                data[prop][0]['state'] = isostate[1]
            else:
                # Remove bogus state
                del data[prop][0]['state']
        elif 'city' in data[prop][0] or 'county' in data[prop][0] or 'country' in data[prop][0]:
            # Handle case where a previous provider-specific locaiton enrichment
            # did not set the state field
            for v in data[prop][0].values():
                isostate = get_isostate(v)
                if isostate[0]:
                    data[prop][0]['iso3166-2'] = isostate[0]
                    data[prop][0]['state'] = isostate[1]
                    break 
        else:
            # Handle the case where no previous provider-specific location
            # enrichment occured
            for d in data[prop]:
                isostate = get_isostate(d['name'], frm_abbrev="Yes")
                if isostate[0]:
                    d['iso3166-2'] = isostate[0]
                    d['state'] = isostate[1]

        # If any of the spatial fields contain semi-colons, we need to create
        # multiple dictionaries.
        semicolons = None
        for k in data[prop][0]:
            if data[prop][0][k] and  ';' in data[prop][0][k]:
                semicolons = True
                break

        if semicolons:
            data[prop] = create_dictionaries(data[prop][0])

    return json.dumps(data)

def get_isostate(strg, frm_abbrev=None):
    if not isinstance(strg, basestring):
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Non-string parameter supplied to get_isostate"

    iso_arr, state_arr = [], []
    strg_arr = strg.split(';')
    for strg_item in strg_arr:
        if frm_abbrev:
            states = from_abbrev(strg_item)
        else:
            states = [strg_item]
        for state in states:
            for st in STATES:
                if st in state.upper():
                    iso_arr.append(STATES[st])
                    state_arr.append(st.title())

    iso, state = None, None
    if iso_arr:
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
    all = []
    if 'city' in data:
        all.append(filter(None, data['city'].split(';')))
        all[-1].insert(0,'city')
    if 'county' in data:
        all.append(filter(None, data['county'].split(';')))
        all[-1].insert(0,'county')
    if 'state' in data:
        all.append(filter(None, data['state'].split(';')))
        all[-1].insert(0,'state')
    if 'country' in data:
        all.append(filter(None, data['country'].split(';')))
        all[-1].insert(0,'country')
    if 'iso3166-2' in data:
        all.append(filter(None, data['iso3166-2'].split(';')))
        all[-1].insert(0,'iso3166-2')

    if all:
        # Get the length of the longest array
        total = len(max(all, key=len))

        # Create dictionaries 
        data = []
        for i in range(1,total):
            d = {}
            for item in all:
                if i < len(item):
                    d[item[0]] = item[i]
            data.append(d)
    return data 

def remove_space_around_semicolons(strg):
    strg_arr = strg.split(';')
    for i in range(len(strg_arr)):
        strg_arr[i] = re.sub('^  *','',strg_arr[i])
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
