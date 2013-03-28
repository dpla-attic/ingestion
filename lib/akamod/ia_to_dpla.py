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
    "metadata/contributor": lambda d, p: {"contributor": getprop(d, p)},
    "metadata/creator": lambda d, p: {"creator": getprop(d, p)},
    "metadata/date": lambda d, p: {"date": getprop(d, p)},
    "metadata/description": lambda d, p: {"description": getprop(d, p)},
    ("metadata/identifier", "metadata/call_number"): lambda d, p: {"identifier": getprop(d, p)},
    "metadata/language": lambda d, p: {"language": getprop(d, p)},
    "metadata/publisher": lambda d, p: {"publisher": getprop(d, p)},
    "metadata/possible-copyright-status": lambda d, p: {"rights": getprop(d, p)},
    "metadata/subject": lambda d, p: {"subject": getprop(d, p)},
    ("metadata/title", "metadata/volume"): lambda d, p: {"title": getprop(d, p)},
    "metadata/mediatype": lambda d, p: {"type": getprop(d, p)},
}

AGGREGATION_TRANSFORMER = {
    "collection": lambda d, p: {"collection": getprop(d, p)},
    "id"               : lambda d, p: {"id": getprop(d, p), "@id" : "http://dp.la/api/items/" + getprop(d, p)},
    "_id"              : lambda d, p: {"_id": getprop(d, p)},
    "originalRecord"   : lambda d, p: {"originalRecord": getprop(d, p)},
    "ingestType"       : lambda d, p: {"ingestType": getprop(d, p)},
    "ingestDate"       : lambda d, p: {"ingestDate": getprop(d, p)},
    "metadata/contributor": lambda d, p: {"dataProvider": getprop(d, p)},
    "metadata/identifier-access": lambda d, p: {"isShownAt": getprop(d, p)},
}

@simple_service('POST', 'http://purl.org/la/dp/ia-to-dpla', 'ia-to-dpla', 'application/ld+json')
def ia_to_dpla(body, ctype, geoprop=None):
    """
    Convert output of Internet Archive service into the DPLA JSON-LD format.

    Parameter "geoprop" specifies the property name containing lat/long coords
    """

    try :
        data = json.loads(body)
    except Exception as e:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON" + "\n" + str(e)

    global GEOPROP
    GEOPROP = geoprop

    out = {
        "@context": CONTEXT,
        "sourceResource" : {"format": "application/pdf"}
    }

    def multi_path_processor(data, paths, transformation):
        value = {}
        for sub_p in paths:
            if exists(data, sub_p):
                fetched = transformation[paths](data, sub_p)
                for k in fetched:
                    if k in value:
                        if isinstance(value[k], list):
                            value[k].append(fetched[k])
                        elif isinstance(value[k], basestring) and value[k] != fetched[k]:
                            value[k] = [value[k], fetched[k]]
                        elif isinstance(value[k], dict):
                            value[k].update(fetched[k])
                    else:
                        value[k] = fetched[k]
        return value


    # Apply all transformation rules from original document
    for p in CHO_TRANSFORMER:
        if isinstance(p, tuple):
            out["sourceResource"].update(multi_path_processor(data, p, CHO_TRANSFORMER))
        elif exists(data, p):
            out["sourceResource"].update(CHO_TRANSFORMER[p](data, p))
    for p in AGGREGATION_TRANSFORMER:
        if isinstance(p, tuple):
            out.update(multi_path_processor(data, p, AGGREGATION_TRANSFORMER))
        elif exists(data, p):
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
