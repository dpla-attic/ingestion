import re
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/nara_enrich_location', 'nara_enrich_location', 'application/json')
def nara_enrich_location(body, ctype, action="nara_enrich_location", prop="sourceResource/spatial"):
    """
    Service that massages a NARA JSON document.
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
            spatials.append(format_spatial(spatial))
                
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


SPATIAL_FORMATTERS = [(re.compile("Ala\."), "AL"),
                      (re.compile("Ariz\."), "AZ"),
                      (re.compile("Calif\."), "CA"),
                      (re.compile("Colo\."), "CO"),
                      (re.compile("Fla\."), "FL"),
                      (re.compile("Ill\."), "IL"),
                      (re.compile("Ind\."), "IN"),
                      (re.compile("Kan\."), "KS"),
                      (re.compile("Mass\."), "MA"),
                      (re.compile("Mich\."), "MI"),
                      (re.compile("Miss\."), "MS"),
                      (re.compile("Minn\."), "MN"),
                      (re.compile("Mont\."), "MT"),
                      (re.compile("Neb\."), "NE"),
                      (re.compile("Nev\."), "NV"),
                      (re.compile("Tenn\."), "TN"),
                      (re.compile("Tex\."), "TX"),
                      (re.compile("W\. Va\."), "WV"),
                      (re.compile("Wash\."), "WA"),
                      (re.compile("Wis\."), "WI"),
                      (re.compile("Wyo\."), "WY"),
                      (re.compile("Ont\."), "Ontario")]

def format_spatial(spatial): 
    name = getprop(spatial, "name", True)
    if (name):
        for regex, repl in SPATIAL_FORMATTERS: 
            if (regex.search(name)):
                spatial["name"] = regex.sub(repl, name)
        
    return spatial
