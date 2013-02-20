from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/mdl-enrich-location', 'mdl-enrich-location', 'application/json')
def mdlenrichlocation(body,ctype,action="mdl-enrich-location", prop="aggregatedCHO/spatial"):
    """
    Service that accepts a JSON document and enriches the "spatial" field of that document by:

    a) Mapping to city, county, state, country, and iso3166-2 if there are 4 fields OR
    b) Mapping to city, state, country, and iso3166-2 if there are 3 fields OR
    c) Mapping to county and country if there are 2 fields

    For primary use with MDL documents.
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
        elif fields == 1:
            sp["country"] = v[0]["name"]
        elif fields == 2:
            sp["state"]   = v[0]["name"]
            sp["country"] = v[1]["name"]
        elif fields == 3:
            sp["county"]  = v[0]["name"]
            sp["state"]   = v[1]["name"]
            sp["country"] = v[2]["name"]
        elif fields == 4:
            sp["city"]    = v[0]["name"]
            sp["county"]  = v[1]["name"]
            sp["state"]   = v[2]["name"]
            sp["country"] = v[3]["name"]
        else:
            sp["city"]    = v[0]["name"]
            sp["county"]  = v[2]["name"]
            sp["state"]   = v[3]["name"]
            sp["country"] = v[4]["name"]

        if sp:
            sp = [sp]
            setprop(data, prop, sp)

    return json.dumps(data)
