import re
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from dplaingestion.utilities import iterify

@simple_service('POST', 'http://purl.org/la/dp/uiuc_enrich_location', 'uiuc_enrich_location', 'application/json')
def uiuc_enrich_location(body, ctype, action="uiuc_enrich_location", prop="sourceResource/spatial"):
    """
    Service that massages a UIUC JSON document.
    """
    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if (exists(data, prop)): 
        # Check spatial dictionaries to see if they are valid
        spatials = []
        for spatial in iterify(getprop(data, prop)):
            if (is_spatial(spatial)):
                spatials.append(format_spatial(spatial))
                
        setprop(data, prop, spatials)

    return json.dumps(data)

# Strings which are present in the spatial field, which do end up being geocoded, 
#  but are not locations
NON_SPATIAL_REGEXES = [re.compile("[0-9]{4}", re.IGNORECASE),
                       re.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}", re.IGNORECASE),
                       re.compile("[0-9]{2+} to", re.IGNORECASE),
                       re.compile("century", re.IGNORECASE),
                       re.compile("[0-9]{4} B.C. ", re.IGNORECASE),
                       re.compile("c[a]?\. [0-9]{4} ", re.IGNORECASE)]

def is_spatial(spatial): 
    # Normalize lat/lng coordinates
    for regex in NON_SPATIAL_REGEXES: 
        if (regex.search(getprop(spatial, "name", True))): 
            return False

    return True

SPATIAL_FORMATTERS = [(re.compile("Africa, (.*)"), "\\1, Africa"), 
                      (re.compile("\(Continent\)"), ""),
                      (re.compile("\(Ala\.\)"), ", AL"),
                      (re.compile("\(Ill\.\)"), ", IL"),
                      (re.compile("\(Ill\.\)"), ", IL"),
                      (re.compile("\(Mass\.\)"), ", MA"),
                      (re.compile("\(Tex\.\)"), ", TX"),
                      (re.compile("\(Wash\.\)"), ", WA")]

def format_spatial(spatial): 
    name = getprop(spatial, "name", True)
    if (name):
        for regex, repl in SPATIAL_FORMATTERS: 
            if (regex.search(name)):
                spatial["name"] = regex.sub(repl, name)
        
    return spatial
