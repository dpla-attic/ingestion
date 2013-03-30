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
MODS = "metadata/mods/"
HARVARD_DATA_PROVIDER = {
    "lap": "Widener Library",
    "crimes": "Harvard Law School Library",
    "scarlet": "Harvard Law School Library",
    "manuscripts": "Houghton Library"
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

# TODO Fix this
# Sometimes:
#   name = [ {"role": {"roleTerm" { ... } }}, {"namePart": "string"}]
#   name = {"role": [{"roleTerm": { ... }, {"namePar": { ... }}}]}
def creator_transform(d, p):
    creator = None
    role = getprop(d, p + "role/roleTerm", True)
    name = getprop(d, p + "role/namePart", True)

    if role and name:
        for r in (role if isinstance(role, list) else [role]):
            if r["#text"] == "creator":
                creator = name[role.index(r)]

    return {"creator": creator} if creator else {}

def subject_transform(d, p):
    subject = []
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else [v]):
        topic = getprop(s, "topic", True)
        if topic:
            for t in (topic if isinstance(topic, list) else [topic]):
                subject.append(t)

    return {"subject": subject} if subject else {}

def spatial_transform(d, p):
    spatial = []
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else [v]):
        place = getprop(s, "placeTerm", True)
        if place:
            if isinstance(place, basestring):
                spatial.append(place)
            else:
                spatial.append(getprop(place, "#text", True))

    spatial = filter(None, spatial)

    return {"spatial": spatial} if spatial else {}

def date_transform(d, p):
    date = None
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else [v]):
        if isinstance(s, basestring):
            date = s

    return {"date": date} if date else {} 

def relation_transform(d, p):
    relation = []
    v = getprop(d, p)
    for s in (v if isinstace(v, list) else[v]):
        relation.append(getprop(s, "titleInfo/title", True))

    relation = filter(None, relation)

    return {"relation": relation} if relation else {}

def title_transform(d, p):
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else[v]):
        # Sometimes there are "alternative" titles which have
        # "type" == alternative. Skip these.
        if not exists(s, "type"):
            title = getprop(s, "title")
            non_sort = getprop(s, "nonSort", True)
        if non_sort:
            title = non_sort + title

    return {"title": title}

def format_transform(d, p):
    format = []
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else [v]):
        if isinstance(s, basestring):
            format.append(s)
        else:
            format.append(getprop(s, "#text", True))
    format = filter(None, format)

    return {"format": format} if format else {}
    
def url_transform(d):
    iho = {}
    p = MODS + "location"
    if exists(d, p):
        location = getprop(d, p)
        for loc in (location if isinstance(location, list) else [location]):
            urls = getprop(loc, "url", True)
            if urls:
                for url in (urls if isinstance(urls, list) else [urls]):
                    if isinstance(url, dict):
                        usage = getprop(url, "usage", True)
                        if usage:
                            iho["isShownAt"] = getprop(url, "#text")

                        label = getprop(url, "displayLabel", True)
                        if label:
                            if label == "Full Image":
                                iho["hasView"] = {"@id": getprop(url, "#text")}
                            if label == "Thumbnail":
                                iho["object"] = getprop(url, "#text")

    return iho

def data_provider_transform(d):
    dp = None
    set = getprop(d, "header/setSpec", True)

    if set == "dag" and exists(d, MODS + "location"):
        location = getprop(d, MODS + "location")
        for loc in (location if isinstance(location, list) else [location]):
            phys = getprop(loc, "physicalLocation", True)
            if phys and getprop(phys, "displayLabel", True) == "repository":
                dp = getprop(phys, "#text").split(";")[0]

    if set in HARVARD_DATA_PROVIDER:
        dp = HARVARD_DATA_PROVIDER[set]

    return {"dataProvider": dp} if dp else {}

# Structure mapping the original top level property to a function returning a single
# item dict representing the new property and its value
CHO_TRANSFORMER = {
    MODS + "note"                                        : lambda d, p: {"description": getprop(d, p)},
    MODS + "name"                                        : creator_transform,
    MODS + "genre"                                       : format_transform,
    MODS + "related"                                     : relation_transform,
    MODS + "subject"                                     : subject_transform,
    MODS + "titleInfo"                                   : title_transform,
    MODS + "typeOfResource"                              : lambda d, p: {"type": getprop(d, p)},
    MODS + "originInfo/place"                            : spatial_transform,
    MODS + "originInfo/publisher"                        : lambda d, p: {"publisher": getprop(d, p)},
    MODS + "originInfo/dateCreated"                      : date_transform,
    MODS + "physicalDescription/extent"                  : lambda d, p: {"extent": getprop(d, p)},
    MODS + "recordInfo/languageOfCataloging/languageTerm": lambda d, p: {"language": getprop(d, p)}
}

AGGREGATION_TRANSFORMER = {
    "id"                         : lambda d, p: {"id": getprop(d, p), "@id" : "http://dp.la/api/items/"+getprop(d, p)},
    "_id"                        : lambda d, p: {"_id": getprop(d, p)},
    "collection"                 : lambda d, p: {"collection": getprop(d, p)},
    "ingestType"                 : lambda d, p: {"ingestType": getprop(d, p)},
    "ingestDate"                 : lambda d, p: {"ingestDate": getprop(d, p)},
    "originalRecord"             : lambda d, p: {"originalRecord": getprop(d, p)}
}

@simple_service("POST", "http://purl.org/la/dp/oai_mods_to_dpla", "oai_mods_to_dpla", "application/ld+json")
def oaimodstodpla(body, ctype, geoprop=None):
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

    # Apply transformations that are dependent on more than one
    # original document field
    out.update(url_transform(data))
    out.update(data_provider_transform(data))

    # Additional content not from original document
    if "HTTP_CONTRIBUTOR" in request.environ:
        try:
            out["provider"] = json.loads(base64.b64decode(request.environ["HTTP_CONTRIBUTOR"]))
        except Exception as e:
            logger.debug("Unable to decode Contributor header value: "+request.environ["HTTP_CONTRIBUTOR"]+"---"+repr(e))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
