import re 
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists, delprop
from dplaingestion.utilities import iterify

@simple_service('POST', 'http://purl.org/la/dp/mwdl_enrich_location', 'mwdl_enrich_location', 'application/json')
def mdlenrichlocation(body,ctype,action="mwdl_enrich_location", prop="sourceResource/spatial"):
    """
    Service that accepts a JSON document and enriches the "spatial" field of that document. 

    For primary use with MWDL documents.
    """
    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data,prop):
        spatials = []
        for spatial in iterify(getprop(data,prop)):
            if (is_spatial(spatial)): 
                spatials.append(format_spatial(spatial))

        if (len(spatials) > 0): 
            setprop(data, prop, spatials)
        else:
            delprop(data, prop)

    return json.dumps(data)

REGEX_NON_SPATIALS = [re.compile(r"(north|east|south|west)limit", re.IGNORECASE), 
                      re.compile(r"^[0-9]+\.[0-9]+ -?[0-9]+\.[0-9]+$"),
                      re.compile(r"^[0-9]+\.[0-9]+$")]

def is_spatial(spatial): 
    for regex in REGEX_NON_SPATIALS:
        if (regex.search(getprop(spatial, "name"))): 
            return False

    return True

REGEX_REPLACEMENTS = [(re.compile("(.*)\(state\)-(.*)\(county\)-(.*)\(inhabited place\)", re.IGNORECASE), "\\3, \\2, \\1"), 
                      (re.compile("\(state\)", re.IGNORECASE), ""),
                      (re.compile("inhabited place", re.IGNORECASE), ""),
                      (re.compile("Ala\."), "AL"),
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
                      (re.compile("Wyo\."), "WY")]

def format_spatial(spatial):
    name = getprop(spatial, "name")
    for regex, repl in REGEX_REPLACEMENTS: 
        if (regex.search(name)): 
            name = regex.sub(repl, name).strip()
            setprop(spatial, "name", name)

    return spatial
