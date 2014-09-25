from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from dplaingestion.utilities import iterify

@simple_service('POST', 'http://purl.org/la/dp/scdl_enrich_location', 'scdl_enrich_location', 'application/json')
def scdl_enrich_location(body, ctype, action="scdl_enrich_location", prop="sourceResource/spatial"):
    """
    Service that accepts a JSON document and enriches the "spatial" field of that document.

    For use with the scdl profiles
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop):
        value = getprop(data,prop)
        for v in iterify(value): 
            name = replace_state_abbreviations(v["name"].rstrip())
            v["name"] = name

            # Try to extract a County
            if " county " in name.lower(): 
                # "XXX County (S.C.)" => county: XXX
                v["county"] = name[0:name.lower().index("county")].strip()
                if "(S.C.)" in name:
                    v["state"] = "South Carolina"
                    v["country"] = "United States"
            elif "(S.C.)" in name:
                # "XXX (S.C)" => city: XXX
                v["city"] = name[0:name.index("(S.C.)")].strip()
                v["state"] = "South Carolina"
                v["country"] = "United States"

    return json.dumps(data)

def replace_state_abbreviations(name):
    """
    Replace abbreviations used in SCDL data with more common versions.
    """
    ABBREV = {
        "(Ala.)": "AL",
        "(Calif.)": "CA",
        "(Conn.)": "CT",
        "(Ill.)": "IL",
        "(Mass.)": "MA",
        "(Miss.)": "MS",
        "(Tex.)": "TX",
        "(Wash.)": "WA",
    }
    for abbrev in ABBREV: 
        if (abbrev in name): 
            return name.replace(abbrev, ABBREV[abbrev])
    return name
