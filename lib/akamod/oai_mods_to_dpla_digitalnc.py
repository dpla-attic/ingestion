from akara import logger
from akara import request, response
from akara.services import simple_service
from amara.lib.iri import is_absolute
from amara.thirdparty import json
from functools import partial
import base64
import sys
import re
from dplaingestion.selector import setprop, getprop, exists
from dplaingestion.utilities import iterify

GEOPROP = None
MODS = "metadata/mods/"
HARVARD_DATA_PROVIDER = {
    "lap": "Widener Library, Harvard University",
    "crimes": "Harvard Law School Library, Harvard University",
    "scarlet": "Harvard Law School Library, Harvard University",
    "manuscripts": "Houghton Library, Harvard University"
}

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

def creator_and_contributor_transform(d, p):
    val = {}
    creator = []
    contributor = []

    for s in iterify(getprop(d, p)):
        name_part = s.get("namePart")
        if name_part:
            try:
                role_terms = [r.get("roleTerm") for r in
                              iterify(s.get("role"))]
            except:
                logger.error("Error getting name/role/roleTerm for record %s" %
                             d["_id"])
                continue

            if "creator" in role_terms:
                creator.append(name_part)
            elif "contributor" in role_terms:
                contributor.append(name_part)

    if creator:
        val["creator"] = creator
    if contributor:
        val["contributor"] = contributor

    return val

def origin_info_transform(d, p):
    val = {}
    date = []
    publisher = []

    for s in iterify(getprop(d, p)):
        # date
        if "dateCreated" in s:
            date_list = iterify(s.get("dateCreated"))
            date = [t.get("#text") for t in date_list if
                    t.get("keyDate") == "yes"]
            # Check if last date is already a range
            if "-" in date[-1] or "/" in date[-1]:
                date = date[-1]
            elif len(date) > 1:
                date = "%s-%s" % (date[0], date[-1])
            else:
                date = date[0]

        # publisher
        if "publisher" in s:
            publisher.append(s.get("publisher"))

    if date:
        val["date"] = date
    if publisher:
        val["publisher"] = publisher

    return val

def description_transform(d, p):
    desc = None
    for s in iterify(getprop(d, p)):
        try:
            if s.get("type") == "content":
                desc = s.get("#text")
                break
        except:
            logger.error("Error getting note/type from record %s" % d["_id"])
        
    return {"description": desc} if desc else {}

def format_and_spec_type_transform(d, p):
    val = {}
    format = []
    spec_type = []

    for s in iterify(getprop(d, p)):
        if "form" in s:
            for f in iterify(s.get("form")):
                if (f.lower() in ["books", "government records"] and f not in
                    spec_type):
                    spec_type.append(f.capitalize())
                elif f not in format:
                    format.append(f)

    if format:
        val["format"] = format
    if spec_type:
        val["specType"] = spec_type

    return val

def subject_and_spatial_transform(d, p):
    val = {}
    subject = []
    spatial = []

    for s in iterify(getprop(d, p)):
        if "geographic" in s:
            spatial.append(s.get("geographic"))
        elif "topic" in s:
            subject.append(s.get("topic"))

    if subject:
        val["subject"] = subject
    if spatial:
        val["spatial"] = spatial

    return val

def is_part_of_transform(d, p):
    ipo = []
    for s in iterify(getprop(d, p)):
        try:
            s.append(getprop(s, "location/url", True))
        except:
            pass
        try:
            s.append(getprop(s, "titleInfo/title", True))
        except:
            pass
    ipo = filter(None, ipo)

    return {"isPartOf": ipo} if ipo else {}

def data_provider_transform(d, p):
    data_provider = None
    for s in iterify(getprop(d, p)):
        try:
            if s.get("type") == "ownership":
                data_provider = s.get("#text")
                break
        except:
            logger.error("Error getting note/type for record %s" % d["_id"])
        
    return {"dataProvider": data_provider} if data_provider else {}

def title_transform(d, p):
    title = []
    for s in iterify(getprop(d, p)):
        title.append(s.get("title"))

    return {"title": title} if title else {}

def object_and_is_shown_at_transform(d, p): 
    val = {}
    for s in iterify(getprop(d, p)):
        try:
            url = getprop(s, "url/#text", True)
        except:
            logger.error("Prop %s does not contain dictionaries in record %s" %
                         (p, d["_id"]))
            continue

        if url:
            usage =  getprop(s, "url/usage", True)
            access = getprop(s, "url/access", True)
            if usage == "primary display" and access == "object in context":
                val["isShownAt"] = url
            elif access == "preview":
                val["object"] = url

    return val

CHO_TRANSFORMER = {
    MODS + "name"                      : creator_and_contributor_transform,
    MODS + "originInfo"                : origin_info_transform,
    MODS + "note"                      : description_transform,
    MODS + "physicalDescription"       : format_and_spec_type_transform,
    MODS + "subject"                   : subject_and_spatial_transform,
    MODS + "relatedItem"               : is_part_of_transform,
    MODS + "titleInfo"                 : title_transform,
    MODS + "accessCondition"           : lambda d, p: {"rights":
                                                       getprop(d, p)},
    MODS + "identifier"                : lambda d, p: {"identifier":
                                                       getprop(d, p)},
    MODS + "language/languageTerm"     : lambda d, p: {"language":
                                                       getprop(d, p)},
    MODS + "genre"                     : lambda d, p: {"type":
                                                       getprop(d, p)}
}

AGGREGATION_TRANSFORMER = {
    "id"             : lambda d, p: {"id": getprop(d, p),
                                     "@id": "http://dp.la/api/items/" + 
                                            getprop(d, p)},
    "_id"            : lambda d, p: {"_id": getprop(d, p)},
    "provider"       : lambda d, p: {"provider": getprop(d, p)},
    "ingestType"     : lambda d, p: {"ingestType": getprop(d, p)},
    "ingestDate"     : lambda d, p: {"ingestDate": getprop(d, p)},
    "originalRecord" : lambda d, p: {"originalRecord": getprop(d, p)},
    MODS + "note"    : data_provider_transform,
    MODS + "location": object_and_is_shown_at_transform
}

@simple_service("POST", "http://purl.org/la/dp/oai_mods_to_dpla",
                "oai_mods_to_dpla_digitalnc", "application/ld+json")
def oaimodstodpladigitalnc(body, ctype, geoprop=None):
    """
    Convert output of JSON-ified OAI MODS format into the DPLA JSON-LD format.

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

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
