from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

@simple_service('POST', 'http://purl.org/la/dp/mdl-enrich-location', 'mdl-enrich-location', 'application/json')
def mdlenrichlocation(body,ctype,action="mdl-enrich-location", prop="spatial"):
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

    if prop in data:
        sp = {}
        fields = len(data[prop])
        if not fields:
            logger.error("Spatial is empty.")
            return json.dumps(data)
        elif fields == 1:
            sp["country"] = data[prop][0]["name"]
        elif fields == 2:
            sp["state"]   = data[prop][0]["name"]
            sp["country"] = data[prop][1]["name"]
        elif fields == 3:
            sp["county"]  = data[prop][0]["name"]
            sp["state"]   = data[prop][1]["name"]
            sp["country"] = data[prop][2]["name"]
        elif fields == 4:
            sp["city"]    = data[prop][0]["name"]
            sp["county"]  = data[prop][1]["name"]
            sp["state"]   = data[prop][2]["name"]
            sp["country"] = data[prop][3]["name"]
        else:
            sp["city"]     = data[prop][0]["name"]
            sp["county"]  = data[prop][2]["name"]
            sp["state"]   = data[prop][3]["name"]
            sp["country"] = data[prop][4]["name"]

        if sp:
            data[prop] = {"spatial": sp}

    return json.dumps(data)
