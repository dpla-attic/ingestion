import base64

from akara import logger
from akara import request, response
from akara.services import simple_service
from amara.thirdparty import json

from dplaingestion.selector import getprop as selector_getprop, exists


def getprop(d, p):
    return selector_getprop(d, p, True)


CONTEXT = {
    "@vocab": "http://purl.org/dc/terms/",
    "dpla": "http://dp.la/terms/",
    "edm": "http://www.europeana.eu/schemas/edm/",
    "LCSH": "http://id.loc.gov/authorities/subjects",
    "name": "xsd:string",
    "collection" : "dpla:aggregation",
    "aggregatedDigitalResource" : "dpla:aggregatedDigitalResource",
    "originalRecord" : "dpla:originalRecord",
    "state": "dpla:state",
    "coordinates": "dpla:coordinates",
    "stateLocatedIn" : "dpla:stateLocatedIn",
    "sourceResource" : "edm:sourceResource",
    "dataProvider" : "edm:dataProvider",
    "hasView" : "edm:hasView",
    "isShownAt" : "edm:isShownAt",
    "object" : "edm:object",
    "provider" : "edm:provider",
    "begin" : {
        "@id" : "dpla:dateRangeStart",
        "@type": "xsd:date"},
    "end" : {
        "@id" : "dpla:dateRangeEnd",
        "@type": "xsd:date"},
}


CHO_TRANSFORMER = {

}

AGGREGATION_TRANSFORMER = {
    "collection"       : lambda d, p: {"collection": getprop(d, p)},
    "id"               : lambda d, p: {"id": getprop(d, p), "@id" : "http://dp.la/api/items/" + getprop(d, p)},
    "_id"              : lambda d, p: {"_id": getprop(d, p)},
    "originalRecord"   : lambda d, p: {"originalRecord": getprop(d, p)},
    "ingestType"       : lambda d, p: {"ingestType": getprop(d, p)},
    "ingestDate"       : lambda d, p: {"ingestDate": getprop(d, p)}
}

@simple_service('POST', 'http://purl.org/la/dp/ia-to-dpla', 'ia-to-dpla', 'application/ld+json')
def ia_to_dpla(body, ctype, geoprop=None):
    """
    Convert output of Internet Archive service into the DPLA JSON-LD format.

    Parameter "geoprop" specifies the property name containing lat/long coords
    """

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    global GEOPROP
    GEOPROP = geoprop

    out = {
        "@context": CONTEXT,
        "sourceResource" : {}
    }

    # Apply all transformation rules from original document
    for p in CHO_TRANSFORMER:
        if exists(data, p):
            out["sourceResource"].update(CHO_TRANSFORMER[p](data, p))
    for p in AGGREGATION_TRANSFORMER:
        if exists(data, p):
            out.update(AGGREGATION_TRANSFORMER[p](data, p))

    # Additional content not from original document

    if 'HTTP_CONTRIBUTOR' in request.environ:
        try:
            out["provider"] = json.loads(base64.b64decode(request.environ['HTTP_CONTRIBUTOR']))
        except Exception as e:
            logger.debug("Unable to decode Contributor header value: "+request.environ['HTTP_CONTRIBUTOR']+"---"+repr(e))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
