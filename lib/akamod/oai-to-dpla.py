from akara import logger
from akara import request, response
from akara.services import simple_service
from amara.lib.iri import is_absolute
from amara.thirdparty import json
from functools import partial
import base64
import sys
from zen import dateparser

GEOPROP = None

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

def created_transform(d):
    created = {
        "start": d["datestamp"],
        "end": d["datestamp"]
    }
    return {"created":created}

def clean_date(d):
    result = d
    result = result.replace("ca.","")
    result = result.strip()
    return result

def split_date(d):
    range = d.split('-')
    if not len(range) == 2:
        range = d.split(' - ')
    if len(range) == 2:
        range = [dateparser.to_iso8601(clean_date(x)) for x in range]
        return range
    else:
        return None, None

def parse_date(t):
    parsed = dateparser.to_iso8601(clean_date(t))
    if parsed is not None:
        a,b = parsed,parsed
    else:
        # Maybe it's a range - try splitting it up
        a,b = split_date(t)
    return a,b

def temporal_transform(d):
    temporal = []

    # First look at the date field, and log any parsing errors
    for t in (d["date"] if not isinstance(d["date"],basestring) else [d["date"]]):
        a,b = parse_date(t)
        if a is not None and b is not None:
            temporal.append( {
                "start": a,
                "end": b
            })
        else:
            logger.debug("Could not parse date: " + t)

    # Then, check out the 'coverage' field, since dates may be there
    if "coverage" in d:
        for t in (d["coverage"] if not isinstance(d["coverage"],basestring) else [d["coverage"]]):
            a,b = parse_date(t)
            if a is not None and b is not None:
                temporal.append( {
                    "start": a,
                    "end": b
                })

    return {"temporal":temporal}

def source_transform(d):
    source = ""
    for s in d["handle"]:
        if is_absolute(s):
            source = s
            break
    return {"source":source}

def subject_transform(d):
    subject = []
    for s in (d["subject"] if not isinstance(d["subject"],basestring) else [d["subject"]]):
        subject.append({
            "name" : s.strip()
        })
    return {"subject" : subject}

# Structure mapping the original property to a function returning a single
# item dict representing the new property and its value
TRANSFORMER = {
    "source"           : lambda d: {"contributor": d.get("source",None)},
    "original_record"  : lambda d: {"dplaSourceRecord": d.get("original_record",None)},
    "datestamp"        : created_transform,
    "date"             : temporal_transform,
    "coverage"         : spatial_transform,
    "title"            : lambda d: {"title": d.get("title",None)},
    "creator"          : lambda d: {"creator": d.get("creator",None)},
    "publisher"        : lambda d: {"publisher": d.get("publisher",None)},
    "type"             : lambda d: {"type": d.get("type",None)},
    "format"           : lambda d: {"format": d.get("format",None)},
    "description"      : lambda d: {"description": d.get("description",None)},
    "rights"           : lambda d: {"rights": d.get("rights",None)},
    "collection"       : lambda d: {"isPartOf": d.get("collection",None)},
    "id"               : lambda d: {"id": d.get("id",None)},
    "subject"          : subject_transform,
    "handle"           : source_transform

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
