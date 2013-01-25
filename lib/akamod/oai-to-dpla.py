from akara import logger
from akara import request, response
from akara.services import simple_service
from amara.lib.iri import is_absolute
from amara.thirdparty import json
from functools import partial
import base64
import sys
import re
from zen import dateparser
from dateutil.parser import parse as dateutil_parse
import timelib

GEOPROP = None
# default date used by dateutil-python to populate absent date elements during parse,
# e.g. "1999" would become "1999-01-01" instead of using the current month/day
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
   "aggregatedCHO" : "edm:aggregatedCHO",   
   "dataProvider" : "edm:dataProvider",
   "hasView" : "edm:hasView",
   "isShownAt" : "edm:isShownAt",
   "object" : "edm:object",
   "provider" : "edm:provider",
   "start" : {
     "@id" : "dpla:dateRangeStart",
     "@type": "xsd:date"
   },
   "end" : {
     "@id" : "dpla:dateRangeEnd",
     "@type": "xsd:date"
   },        
   "name": "xsd:string"
}

def spatial_transform(d):
    global GEOPROP
    spatial = []
    for i,s in enumerate((d["coverage"] if not isinstance(d["coverage"],basestring) else [d["coverage"]])):
        sp = { "name": s.strip() }
        # Check if we have lat/long for this location. Requires geocode earlier in the pipeline
        if GEOPROP in d and i < len(d[GEOPROP]) and len(d[GEOPROP][i]) > 0:
            sp["coordinates"] = d[GEOPROP][i]
        spatial.append(sp)
    return {"spatial":spatial}

def is_shown_at_transform(d):
    source = ""
    for s in (d["handle"] if not isinstance(d["handle"],basestring) else [d["handle"]]):
        if is_absolute(s):
            source = s
            break
    return {
        "isShownAt" : { 
            "@id" : source,
            "format" : d.get("format",None),
            "rights" : d.get("rights",None)
            }
        }

# Structure mapping the original property to a function returning a single
# item dict representing the new property and its value
CHO_TRANSFORMER = {
    "contributor"      : lambda d: {"contributor": d.get("contributor",None)},
    "coverage"         : lambda d: {"spatial": d.get("coverage",None)},
    "creator"          : lambda d: {"creator": d.get("creator",None)},
    "description"      : lambda d: {"description": d.get("description",None)},
    "date"             : lambda d: {"date": d.get("date",None)},
    "language"         : lambda d: {"language": d.get("language",None)},
    "format"           : lambda d: {"physicalmedium" : d.get("format",None)},
    "publisher"        : lambda d: {"publisher": d.get("publisher",None)},
    "relation"         : lambda d: {"relation": d.get("relation",None)},
    "rights"           : lambda d: {"rights": d.get("rights",None)},
    "subject"          : lambda d: {"subject": d.get("subject",None)},
    "title"            : lambda d: {"title": d.get("title",None)},
    "type"             : lambda d: {"type": d.get("type",None)}
}

AGGREGATION_TRANSFORMER = {
    "collection"       : lambda d: {"collection": d.get("collection",None)},
    "id"               : lambda d: {"id": d.get("id",None), "@id" : "http://dp.la/api/items/"+d.get("id","")},
    "handle"           : is_shown_at_transform,
    "originalRecord"   : lambda d: {"originalRecord": d.get("originalRecord",None)},
    "source"           : lambda d: {"dataProvider": d.get("source",None)},

    "ingestType"       : lambda d: {"ingestType": d.get("ingestType",None)},
    "ingestDate"       : lambda d: {"ingestDate": d.get("ingestDate",None)}
}

@simple_service('POST', 'http://purl.org/la/dp/oai-to-dpla', 'oai-to-dpla', 'application/ld+json')
def oaitodpla(body,ctype,geoprop=None):
    '''   
    Convert output of Freemix OAI service into the DPLA JSON-LD format.

    Does not currently require any enrichments to be ahead in the pipeline, but
    supports geocoding if used. In the future, subject shredding may be assumed too.

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
        "aggregatedCHO" : {}
    }

    # Apply all transformation rules from original document to aggregatedCHO
    for p in data.keys():
        if p in CHO_TRANSFORMER:
            out['aggregatedCHO'].update(CHO_TRANSFORMER[p](data))
        if p in AGGREGATION_TRANSFORMER:
            out.update(AGGREGATION_TRANSFORMER[p](data))

    # Additional content not from original document

    if 'HTTP_CONTRIBUTOR' in request.environ:
        try:
            out["dplaContributor"] = json.loads(base64.b64decode(request.environ['HTTP_CONTRIBUTOR']))
        except Exception as e:
            logger.debug("Unable to decode Contributor header value: "+request.environ['HTTP_CONTRIBUTOR']+"---"+repr(e))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
