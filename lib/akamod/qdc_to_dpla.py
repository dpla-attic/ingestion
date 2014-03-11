import base64

from akara import logger
from akara import request, response
from akara.services import simple_service
from amara.lib.iri import is_absolute
from amara.thirdparty import json
from dateutil.parser import parse as dateutil_parse

from dplaingestion.selector import getprop
from dplaingestion.utilities import iterify


# default date used by dateutil-python to populate absent date elements during
# parse, e.g. "1999" would become "1999-01-01" instead of using the current
# month/day
DEFAULT_DATETIME = dateutil_parse("2000-01-01") 

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
     "@id" : "dpla:dateRangeEnd",
     "@type": "xsd:date"
   },
}

def is_shown_at_transform(d, index=None):
    is_shown_at = None
    identifiers = [id for id in iterify(d.get("handle")) if is_absolute(id)]
    if index:
        try:
            is_shown_at = identifiers[int(index)]
        except:
            pass
    if not is_shown_at:
        is_shown_at = identifiers[0]

    return {"isShownAt": is_shown_at} if is_shown_at else {}

def uiuc_format_transform(d):
    format = {}
    if "medium" not in d:
        # Use dc:format if dc:medium does not exist
        format["format"] = d.get("format")

    return format

def uiuc_date_transform(d):
    date = []
    date_fields = ("date", "created")
    for field in date_fields:
        if field in d:
            # Some values can be lists so we iterify and extend
            date.extend(iterify(d.get(field)))

    return {"date": date} if date else {}

def uiuc_relation_transform(d):
    relation = []
    relation_fields = ("relation", "source", "isPartOf")
    for field in relation_fields:
        if field in d:
            relation.append(d.get(field))
 
    return {"relation": relation} if relation else {}

def scdl_usc_relation_transform(d):
    relation = []
    relation_fields = ("relation", "source")
    for field in relation_fields:
        if field in d:
            relation.append(d.get(field))

    return {"relation": relation} if relation else {}

# Structure mapping the original property to a function returning a single
# item dict representing the new property and its value
CHO_TRANSFORMER = {}
CHO_TRANSFORMER["common"] = {
    "collection"       : lambda d: {"collection": d.get("collection")},
    "contributor"      : lambda d: {"contributor": d.get("contributor")},
    "creator"          : lambda d: {"creator": d.get("creator")},
    "description"      : lambda d: {"description": d.get("description")},
    "extent"           : lambda d: {"extent": d.get("extent")},
    "medium"           : lambda d: {"format": d.get("medium")},
    "language"         : lambda d: {"language": d.get("language")},
    "spatial"          : lambda d: {"spatial": iterify(d.get("spatial"))},
    "relation"         : lambda d: {"relation": d.get("relation")},
    "rights"           : lambda d: {"rights": d.get("rights")},
    "subject"          : lambda d: {"subject": d.get("subject")},
    "temporal"         : lambda d: {"temporal": d.get("temporal")},
    "title"            : lambda d: {"title": d.get("title")},
    "type"             : lambda d: {"type": d.get("type")},
}

CHO_TRANSFORMER["uiuc"] = {
    "format"           : uiuc_format_transform,
    "date"             : uiuc_date_transform,
    "created"          : uiuc_date_transform,
    "relation"         : uiuc_relation_transform,
    "source"           : uiuc_relation_transform,
    "isPartOf"         : uiuc_relation_transform
}

CHO_TRANSFORMER["scdl-usc"] = {
    "created"          : lambda d: {"date": d.get("created")},
    "relation"         : scdl_usc_relation_transform,
    "source"           : scdl_usc_relation_transform
}

CHO_TRANSFORMER["scdl-charleston"] = {
    "date"             : lambda d: {"date": d.get("date")},
}

CHO_TRANSFORMER["clemson"] = {
    "date"             : lambda d: {"date": d.get("date")},
}

CHO_TRANSFORMER["mdl"] = {
    "created"          : lambda d: {"date": d.get("created")},
    "isPartOf"         : lambda d: {"relation": d.get("isPartOf")},
    "source"           : lambda d: {"publisher": d.get("source")}
}

AGGREGATION_TRANSFORMER = {}
AGGREGATION_TRANSFORMER["common"] = {
    "id"               : lambda d: {"id": d.get("id"),
                                    "@id" : "http://dp.la/api/items/" +
                                            d.get("id", "")},
    "_id"              : lambda d: {"_id": d.get("_id")},
    "originalRecord"   : lambda d: {"originalRecord": d.get("originalRecord")},
    "provider"         : lambda d: {"provider": d.get("provider")},
    "publisher"        : lambda d: {"dataProvider": d.get("publisher")},
    "ingestType"       : lambda d: {"ingestType": d.get("ingestType")},
    "ingestDate"       : lambda d: {"ingestDate": d.get("ingestDate")}
}

AGGREGATION_TRANSFORMER["mdl"] = {
    "handle"           : lambda d: is_shown_at_transform(d, index=1)
}

AGGREGATION_TRANSFORMER["scdl-charleston"] = {
    "handle"           : lambda d: is_shown_at_transform(d)
}

AGGREGATION_TRANSFORMER["scdl-usc"] = {
    "handle"           : lambda d: is_shown_at_transform(d, index=1)
}

AGGREGATION_TRANSFORMER["clemson"] = {
    "handle"           : lambda d: is_shown_at_transform(d, index=1)
}

AGGREGATION_TRANSFORMER["uiuc"] = {
    "handle"           : lambda d: is_shown_at_transform(d, index=-1)
}

@simple_service('POST', 'http://purl.org/la/dp/qdc_to_dpla', 'qdc_to_dpla',
                'application/ld+json')
def qdctodpla(body, ctype, geoprop=None, provider=None):
    '''   
    Convert output of Freemix OAI service into the DPLA JSON-LD format.

    Does not currently require any enrichments to be ahead in the pipeline,
    but supports geocoding if used. In the future, subject shredding may be
    assumed too.

    Parameter "geoprop" specifies the property name containing lat/long coords
    '''

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

    # Apply all transformation rules from original document to sourceResource
    for p in data.keys():
        if p in CHO_TRANSFORMER["common"]:
            out['sourceResource'].update(CHO_TRANSFORMER["common"][p](data))
        if p in AGGREGATION_TRANSFORMER["common"]:
            out.update(AGGREGATION_TRANSFORMER["common"][p](data))
        if provider:
            if provider in CHO_TRANSFORMER and p in CHO_TRANSFORMER[provider]:
                out['sourceResource'].update(CHO_TRANSFORMER[provider][p](data))
            if provider in AGGREGATION_TRANSFORMER and p in AGGREGATION_TRANSFORMER[provider]:
                out.update(AGGREGATION_TRANSFORMER[provider][p](data))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
