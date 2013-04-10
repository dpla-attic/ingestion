import re
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/digital_commonwealth_enrich_location', 'digital_commonwealth_enrich_location', 'application/json')
def digital_commonwealth_enrich_location(body, ctype, action="digital_commonwealth_enrich_location", prop="sourceResource/spatial"):
    """
    Service that massages a Digital Commonwealth JSON document.
    """
    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    # Strings which are present in the spatial field, which do end up being geocoded, 
    #  but are not locations
    NON_SPATIALS = ["Aerial views.",
                    "Church history.", 
                    "Dwellings",
                    "Dwellings.",
                    "History"]

    if (exists(data, prop)): 
        # Spatial field is simply a list of strings, convert to a list 
        #  of dictionaries with the name key set to the string value
        spatials = []
        for spatial in iterify(getprop(data, prop)):
            if (isinstance(spatial, basestring) \
                and spatial not in NON_SPATIALS):
                spatials.append({"name": format_spatial(spatial)})
                
        setprop(data, prop, spatials)

    return json.dumps(data)


def iterify(iterable): 
    """
    Treat iterating over a single item or an interator seamlessly.
    """
    if (isinstance(iterable, basestring) \
        or isinstance(iterable, dict)):
        iterable = [iterable]
    try:
        iter(iterable)
    except TypeError:
        iterable = [iterable]
    return iterable


def format_spatial(value): 
    # Normalize lat/lng coordinates
    LATLNG_REGEXES = ["4([23])[\-\.]?([0-9]+)\s*N[;0]?\s*7([1-3])[\-\.]?([0-9]+)\s*W", 
                      "4([23]) degrees ([0-9]+)' N\s*7([1-3]) degrees ([0-9]+)'\s?W?"]
    for regex in LATLNG_REGEXES: 
        if (re.match(regex, value)): 
            value = re.sub(regex, "4\\1.\\2N 7\\3.\\4W", value)
            break

    # Fix state names
    STATE_REGEXES = [("(.*) \(Mass\.\)", "\\1, MA"), 
                     ("(.*) Mass\.(.*)", "\\1 MA\\2")]
    for regex, repl in STATE_REGEXES: 
        if (re.match(regex, value)): 
            value = re.sub(regex, repl, value)
            break

    return value.strip()
