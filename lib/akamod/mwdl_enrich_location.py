from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/mwdl_enrich_location',
                'mwdl_enrich_location', 'application/json')
def mwdlenrichlocation(body, ctype, action="mdl_enrich_location",
                       prop="aggregatedCHO/spatial"):
    """
    Service that accepts a JSON document and enriches the "spatial" field of
    that document by:

    For primary use with MWDL documents.
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data,prop):
        spatial = []
        values = getprop(data,prop)
        for v in values.split(";"):
            if STATE_CODES.get(v):
                spatial.append(STATE_CODES[v])
            else:
                spatial.append(v)
        setprop(data, prop, "; ".join(spatial))

    return json.dumps(data)

STATE_CODES = {
    "101": "UT",
    "102": "UT",
    "103": "UT",
    "104": "NV",
    "105": "NV",
    "106": "UT",
    "107": "UT",
    "108": "ID",
    "109": "UT",
    "110": "UT",
    "111": "NV",
    "112": "UT",
    "114": "UT",
    "115": "UT",
    "116": "UT",
    "117": "UT",
    "118": "UT",
    "119": "UT",
    "120": "UT",
    "121": "UT",
    "122": "UT",
    "123": "UT",
    "124": "UT",
    "125": "UT",
    "126": "UT",
    "127": "ID",
    "128": "hi",
    "129": "UT",
    "131": "UT",
    "132": "UT",
    "133": "UT",
    "135": "UT",
    "136": "UT",
    "137": "UT",
    "138": "UT",
    "139": "UT",
    "140": "UT",
    "141": "NV",
    "142": "NV",
    "143": "UT",
    "144": "UT",
    "146": "UT",
    "147": "UT",
    "149": "UT",
    "151": "UT",
    "200": "UT",
    "201": "UT",
    "203": "UT",
    "205": "UT",
    "206": "UT",
    "207": "UT",
    "213": "UT",
    "215": "UT",
    "217": "UT",
    "218": "UT",
    "287": "ID",
    "288": "UT"
}
