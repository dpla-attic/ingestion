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
   "name": "xsd:string",
   "dplaContributor": "dpla:contributor",
   "dplaSourceRecord": "dpla:sourceRecord",
   "coordinates": "dpla:coordinates",
   "state": "dpla:state",                             
   "start" : {
     "@id" : "dpla:start",
     "@type": "xsd:date"
   },
   "end" : {
     "@id" : "dpla:end",
     "@type": "xsd:date"
   },
   "iso3166-2": "dpla:iso3166-2",
   "iso639": "dpla:iso639",
   "LCSH": "http://id.loc.gov/authorities/subjects"
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

def source_transform(d):
    source = ""
    if isinstance(d['handle'],basestring) and is_absolute(d['handle']):
        source = d['handle']
    else:
        for s in d["handle"]:
            if is_absolute(s):
                source = s
                break
    return {"source":source}


# Structure mapping the original property to a function returning a single
# item dict representing the new property and its value
TRANSFORMER = {
    "source"           : lambda d: {"contributor": d.get("source",None)},
    "original_record"  : lambda d: {"dplaSourceRecord": d.get("original_record",None)},
    "date"             : lambda d: {"created": d.get("date",None)},
    "coverage"         : spatial_transform,
    "title"            : lambda d: {"title": d.get("title",None)},
    "creator"          : lambda d: {"creator": d.get("creator",None)},
    "publisher"        : lambda d: {"publisher": d.get("publisher",None)},
    "type"             : lambda d: {"type": d.get("type",None)},
    "format"           : lambda d: {"format": d.get("format",None)},
    "description"      : lambda d: {"description": d.get("description",None)},
    "rights"           : lambda d: {"rights": d.get("rights",None)},
    "collection"       : lambda d: {"isPartOf": d.get("collection",None)},
    "id"               : lambda d: {"id": d.get("id",None), "@id" : "http://dp.la/api/items/"+d.get("id","")},
    "subject"          : lambda d: {"subject": d.get("subject",None)},
    "handle"           : source_transform,
    "ingestType"       : lambda d: {"ingestType": d.get("ingestType",None)},
    "ingestDate"       : lambda d: {"ingestDate": d.get("ingestDate",None)},
    "_id"              : lambda d: {"_id": d.get("_id",None)}

    # language - needs a lookup table/service. TBD.
    # subject - needs additional LCSH enrichment. just names for now
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
    }

    # Apply all transformation rules from original document
    for p in data.keys():
        if p in TRANSFORMER:
            out.update(TRANSFORMER[p](data))

    # Additional content not from original document

    if 'HTTP_CONTRIBUTOR' in request.environ:
        try:
            out["dplaContributor"] = json.loads(base64.b64decode(request.environ['HTTP_CONTRIBUTOR']))
        except Exception as e:
            logger.debug("Unable to decode Contributor header value: "+request.environ['HTTP_CONTRIBUTOR']+"---"+repr(e))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
