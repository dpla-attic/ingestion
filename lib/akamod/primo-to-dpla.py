from akara import logger
from akara import request, response
from akara.services import simple_service
from amara.lib.iri import is_absolute
from amara.thirdparty import json
from functools import partial
import base64
import sys
import re
from copy import deepcopy
from dplaingestion.selector import getprop, exists

GEOPROP = None
RECORD = "PrimoNMBib/record/"
LINKS = "LINKS/"
URL = "http://thoth.library.utah.edu:1701/primo_library/libweb/action/dlDisplay.do?vid=MWDL&afterPDS=true&docId="

#FIXME not format specific, move to generic module
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
     "@type": "xsd:date"
   },
   "end" : {
     "@id" : "dpla:end",
     "@type": "xsd:date"
   }
}

def multi_transform(d, key, props, return_format="str"):
    """

    Possible values for 'return_format' are:
        - 'str' - returns string
        - 'list' - returns list
    """
    values = []

    for p in props:
        p = RECORD + p
        if exists(d, p):
            v = getprop(d, p)
            if not v: continue
            if not isinstance(v, list):
                v = [v]
            [values.append(s) for s in v if s not in values]

    if return_format == "list":
        return {key: values} if values else {}
    
    return {key: "; ".join(values)} if values else {}

# Structure mapping the original top level property to a function returning a single
# item dict representing the new property and its value
CHO_TRANSFORMER = {
    RECORD + "display/creator"      : lambda d, p: {"creator": getprop(d, p)},
    RECORD + "display/creationdate" : lambda d, p: {"date": getprop(d, p)},
    RECORD + "search/description"   : lambda d, p: {"description": getprop(d, p)},
    RECORD + "display/lds05"        : lambda d, p: {"extent": getprop(d, p)},
    RECORD + "facets/language"      : lambda d, p: {"language": getprop(d, p)},
    RECORD + "display/relation"     : lambda d, p: {"relation": getprop(d, p)},
    RECORD + "display/rights"       : lambda d, p: {"rights": getprop(d, p)},
    RECORD + "display/subject"      : lambda d, p: {"subject": getprop(d, p)},
    RECORD + "display/lds09"        : lambda d, p: {"temporal": getprop(d, p)},
    RECORD + "display/lds17"        : lambda d, p: {"type": getprop(d, p)},
    RECORD + "search/lsr03"         : lambda d, p: {"stateLocatedIn": getprop(d, p)},
    LINKS + "linktorsrc"             : lambda d, p: {"identifier": getprop(d, p)}
}

AGGREGATION_TRANSFORMER = {
    "id"                         : lambda d, p: {"id": getprop(d, p), "@id" : "http://dp.la/api/items/"+getprop(d, p)},
    "_id"                        : lambda d, p: {"_id": getprop(d, p)},
    "originalRecord"             : lambda d, p: {"originalRecord": getprop(d, p)},
    "ingestType"                 : lambda d, p: {"ingestType": getprop(d, p)},
    "ingestDate"                 : lambda d, p: {"ingestDate": getprop(d, p)},
    RECORD + "control/recordid"  : lambda d, p: {"isShownAt": URL + getprop(d, p)},
    LINKS + "thumbnail"          : lambda d, p: {"object": getprop(d, p)},
    "provider"                   : lambda d, p: {"provider": getprop(d, p)}
}

@simple_service("POST", "http://purl.org/la/dp/primo-to-dpla", "primo-to-dpla", "application/ld+json")
def primotodpla(body,ctype,geoprop=None):
    """
    Convert output of JSON-ified PRIMO (MWDL) format into the DPLA JSON-LD format.

    Parameter "geoprop" specifies the property name containing lat/long coords
    """

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header("content-type","text/plain")
        return "Unable to parse body as JSON"

    global GEOPROP
    GEOPROP = geoprop

    out = {
        "@context": CONTEXT,
        "sourceResource": {}
    }

    # Apply all transformation rules from original document
    for p in CHO_TRANSFORMER:
        if exists(data, p):
            out["sourceResource"].update(CHO_TRANSFORMER[p](data, p))
    for p in AGGREGATION_TRANSFORMER:
        if exists(data, p):
            out.update(AGGREGATION_TRANSFORMER[p](data, p))

    # Apply transformations that are dependent on more than one
    # original document field
    sp_props = ["display/lds08"]
    ipo_props = ["display/lds04"]
    title_props = ["display/title", "display/lds10"]
    out["sourceResource"].update(multi_transform(data, "spatial", sp_props, "list"))
    out["sourceResource"].update(multi_transform(data, "isPartOf", ipo_props))
    out["sourceResource"].update(multi_transform(data, "title", title_props))    

    dp_props = ["display/lds03"]
    out.update(multi_transform(data, "dataProvider", dp_props))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
