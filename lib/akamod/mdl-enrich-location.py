from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/mdl-enrich-location', 'mdl-enrich-location', 'application/json')
def mdlenrichlocation(body,ctype,action="mdl-enrich-location", prop="sourceResource/spatial"):
    """
    Service that accepts a JSON document and enriches the "spatial" field of that document by
    combining all spatial fields into one. Will also split out country and state on a 
    best-efforts basis.

    For primary use with MDL documents.

    Possible avenues of improvement:
      - For fields with semi-colons, permute and create multiple spatial elements 
      - Create an ordered list of "names" for the geocoder to attempt to lookup 
        as opposed to our single concatenated list:
          - Everything concatenated together 
          - Everything concatenated together up to "United States" 
          - Remove left-most elements one by one
    """
    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data,prop):
        sp = {}
        v = getprop(data,prop)
        fields = len(v)
        if not fields:
            logger.error("Spatial is empty.")
            return json.dumps(data)
        else:
            # Concatenate all values together to form the name field 
            sp["name"] = ", ".join(v)

            if (1 == fields): 
                # If there is only one element present, it is a country 
                sp["country"] = clean(v[0])
            elif "United States" in v: 
                country_index = v.index("United States")
                sp["country"] = clean(v[country_index])

                # The prior item is almost always a state 
                if (country_index > 1):
                    state = clean(v[country_index - 1])
                    if (is_state(state)): 
                        sp["state"] = state

        if sp:
            sp = [sp]
            setprop(data, prop, sp)

    return json.dumps(data)


def clean(value): 
    """ Remove trailing semi-colons from the value """
    return value.strip(";")


def is_state(value): 
    """ Check whether or not the passed-in value is a state name """
    return (value.upper() in STATES)



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
