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

from dplaingestion.selector import getprop as selector_getprop, exists


def getprop(d, p):
    selector_getprop(d, p, True)

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
    "begin" : {
        "@id" : "dpla:dateRangeStart",
        "@type": "xsd:date"
    },
    "end" : {
        "@id" : "dpla:dateRangeEnd",
        "@type": "xsd:date"
    },
    "name": "xsd:string"
}

def physical_description_handler(d, p):
    note = getprop(d, p)
    out = {}
    for _dict in note:
        if isinstance(_dict, dict) and "displayLabel" in _dict:
            if _dict["displayLabel"] == "size inches":
                out["extent"] = _dict["#text"]
            if _dict["displayLabel"] == "condition":
                out["description"] = _dict["#text"]
    return out

def subject_handler(d, p):
    orig_subject = getprop(d, p)
    subjects = {"subject": []}
    for _dict in orig_subject:
        if "topic" in _dict:
            subjects["subject"].append(_dict["topic"])
        if "name" in _dict:
            subjects["subject"].append(getprop(_dict, "name/namePart"))
    return subjects

def location_handler(d, p):
    location = getprop(d, p)
    format = getprop(d, "physicalDescription/internetMediaType", True)
    out = {}
    try:
        for _dict in location:
            if "url" in _dict:
                for url_dict in _dict["url"]:
                    if url_dict and "access" in url_dict:
                        if url_dict["access"] == "object in context":
                            out["isShownAt"] = {"@id:": url_dict["#text"], "format": format}
                        if url_dict["access"] == "preview":
                            out["hasView"] = {"@id:": url_dict["#text"], "format": format}
                            out["object"] = {"@id:": url_dict["#text"], "format": format}
            if "physicalLocation" in _dict and isinstance(_dict["physicalLocation"], basestring):
                out["dataProvider"] = _dict["physicalLocation"]
    except Exception as e:
        logger.error(e)
    finally:
        return out




# Structure mapping the original property to a function returning a single
# item dict representing the new property and its value
CHO_TRANSFORMER = {
    "recordInfo/languageOfCataloging/languageTerm/#text": lambda d, p: {"language": getprop(d, p)},
    "name/namePart": lambda d, p: {"creator": getprop(d, p)},
    "physicalDescription/note": physical_description_handler,
    "physicalDescription/form/authority": lambda d, p: {"format": getprop(d, p)},
    "originInfo/place/placeTerm": lambda d, p: {"spatial": getprop(d, p)},
    "accessCondition": lambda d, p: {"rights": [s["#text"] for s in getprop(d, p) if "#text" in s]},
    "subject": subject_handler,
    "titleInfo/title": lambda d, p: {"title": getprop(d, p)},
    "typeOfResource/#text": lambda d, p: {"type": getprop(d, p)},
    "originInfo/dateCreated": lambda d, p: {"date": getprop(d, p)}
}

AGGREGATION_TRANSFORMER = {
    "id"                         : lambda d, p: {"id": getprop(d, p), "@id" : "http://dp.la/api/items/"+getprop(d, p)},
    "_id"                        : lambda d, p: {"_id": getprop(d, p)},
    "originalRecord"             : lambda d, p: {"originalRecord": getprop(d, p)},
    "ingestType"                 : lambda d, p: {"ingestType": getprop(d, p)},
    "ingestDate"                 : lambda d, p: {"ingestDate": getprop(d, p)},
    "location": location_handler,
    "recordInfo/recordContentSource": lambda d, p: {"provider": getprop(d, p)}
}

@simple_service('POST', 'http://purl.org/la/dp/mods-to-dpla', 'mods-to-dpla', 'application/ld+json')
def mods_to_dpla(body, ctype, geoprop=None):
    """
    Convert output of JSON-ified MODS/METS format into the DPLA JSON-LD format.

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
        "aggregatedCHO" : {}
    }

    # Apply all transformation rules from original document
    for p in CHO_TRANSFORMER:
        if exists(data, p):
            out["aggregatedCHO"].update(CHO_TRANSFORMER[p](data, p))
    for p in AGGREGATION_TRANSFORMER:
        if exists(data, p):
            out.update(AGGREGATION_TRANSFORMER[p](data, p))
    if "spatial" in out["aggregatedCHO"]:
        out["aggregatedCHO"]["spatial"]["currentLocation"] = "Virginia"

    # Additional content not from original document

    if 'HTTP_CONTRIBUTOR' in request.environ:
        try:
            out["provider"] = json.loads(base64.b64decode(request.environ['HTTP_CONTRIBUTOR']))
        except Exception as e:
            logger.debug("Unable to decode Contributor header value: "+request.environ['HTTP_CONTRIBUTOR']+"---"+repr(e))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
