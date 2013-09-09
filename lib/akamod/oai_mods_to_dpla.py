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

def name_from_name_part(name_part):
    type_exceptions = ("affiliation", "displayForm", "description", "role")

    name = []
    for n in (name_part if isinstance(name_part, list) else [name_part]):
        if isinstance(n, basestring):
            name.append(n)
        if (isinstance(n, dict) and "#text" in n and not
            (set(n["type"]) & set(type_exceptions))):
            name.append(n["#text"])

    return ", ".join(name)


def creator_and_contributor_transform(d, p):
    val = {}

    v = getprop(d, p)
    names = []
    for s in (v if isinstance(v, list) else [v]):
        name = {}
        name["name"] = name_from_name_part(getprop(s, "namePart", True))
        if name["name"]:
            name["type"] = getprop(s, "type", True)
            name["roles"] = []
            if "role" in s:
                roles = getprop(s, "role")
                for r in (roles if isinstance(roles, list) else [roles]):
                    role = r["roleTerm"]
                    if isinstance(role, dict):
                        role = role["#text"]
                    name["roles"].append(role)

            names.append(name)

    # Set creator
    creator = [name for name in names if "creator" in name["roles"]]
    creator = creator[0] if creator else names[0]
    names.remove(creator)
    val["creator"] = creator["name"]

    # Set contributor
    contributor = [name["name"] for name in names]
    if contributor:
        val["contributor"] = contributor

    return val

def subject_and_spatial_transform(d, p):
    val = {}
    val["subject"] = []
    val["spatial"] = []

    v = getprop(d, p)
    for s in (v if isinstance(v, list) else [v]):
        subject = []
        if "name" in s:
            subject.append(name_from_name_part(getprop(s, "name/namePart")))

        if "topic" in s:
            for t in (s["topic"] if isinstance(s["topic"], list) else
                      [s["topic"]]):
                if t not in subject:
                    subject.append(t)

        if "geographic" in s:
            for g in (s["geographic"] if isinstance(s["geographic"], list) else
                      [s["geographic"]]):
                if g not in subject:
                    subject.append(g)
                if g not in val["spatial"]:
                    val["spatial"].append(g)

        if "hierarchicalGeographic" in s:
            c = getprop(s, "hierarchicalGeographic/country", True)
            if c and c not in subject:
                subject.append(c)
            if c and c not in val["spatial"]:
                val["spatial"].append(c)

        val["subject"].append("--".join(subject))

    if not val["subject"]:
        del val["subject"]
    if not val["spatial"]:
        del val["spatial"]

    return val

def origin_info_transform(d, p):
    val = {}
    v = getprop(d, p)

    # date
    date = None 
    if "dateCreated" in v:
        date = v["dateCreated"]
    if not date and getprop(v, "dateOther/keyDate", True) == "yes":
        date = getprop(v, "dateOther/#text", True)

    if isinstance(date, list):
        dd = {}
        for i in date:
            if isinstance(i, basestring):
                dd["displayDate"] = i
            elif "point" in i:
                if i["point"] == "start":
                    dd["begin"] = i["point"]
                else:
                    dd["end"] = i["point"]
            else:
                # Odd date? Log error and investigate
                logger.error("Invalid date in record %s" % d["_id"])
        date = dd if dd else None

    if date and date != "unknown":
        val["date"] = date
    
    # publisher
    if "publisher" in v:
        val["publisher"] = []
        pub = v["publisher"]

        di = v.get("dateIssued", None)
        di = di[0] if isinstance(di, list) else di

        # Get all placeTerms of type "text"
        terms = []
        if "place" in v:
            place = v["place"]
            for p in (place if isinstance(place, list) else [place]):
                if getprop(p, "placeTerm/type", True) == "text":
                    terms.append(getprop(p, "placeTerm/#text", True))

        for t in filter(None, terms):
            if di: 
                val["publisher"].append("%s: %s, %s" % (t, pub, di))
            else:
                val["publisher"].append("%s: %s" % (t, pub))
        if len(val["publisher"]) == 1:
            val["publisher"] = val["publisher"][0]

    return val

def language_transform(d, p):
    language = []
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else [v]):
        if "#text" in s and s["#text"] not in language:
            language.append(s["#text"])

    return {"language": language} if language else {}

def is_part_of_transform(d, p):
    ipo = []
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else[v]):
        if "type" in v and v["type"] == "series":
            ipo.append(getprop(s, "titleInfo/title", True))

    ipo = filter(None, ipo)
    ipo = ipo[0] if len(ipo) == 1 else ipo

    return {"isPartOf": ipo} if ipo else {}

def title_transform(d, p):
    title = None

    v = getprop(d, p)
    for t in (v if isinstance(v, list) else [v]):
        if "type" in t and t["type"] == "alternative":
            continue

        title = t["title"]
        if "nonSort" in t:
            title = t["nonSort"] + title
        if "subTitle" in t:
            title = title + ": " + t["subTitle"]
        if "partNumber" in t:
            part = t["partNumber"]
            if not isinstance(part, list):
                part = [part]
            part = ", ".join(part)
            title = title + ". " + part + "."
        if "partName" in t:
            title = title + ". " + t["partName"]

    title = re.sub("\.\.", "\.", title)
    title = re.sub(",,", ",", title)

    return {"title": title} if title else {}

def format_transform(d, p):
    format = []
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else [v]):
        if isinstance(s, basestring):
            format.append(s)
        else:
            if "authority" in s and (s["authority"] == "marc" or
                                     s["authority"] == ""):
                format.append(s["#text"])
    format = filter(None, format)

    return {"format": format} if format else {}

def description_transform(d, p):
    desc = getprop(d, p)
    if not isinstance(desc, basestring):
        desc = desc["#text"] if "#text" in desc else None

    return {"description": desc} if desc else {}
    
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
                        if usage == "primary display":
                            iho["isShownAt"] = getprop(url, "#text")

                        label = getprop(url, "displayLabel", True)
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
                dp += ", Harvard University"

    if set in HARVARD_DATA_PROVIDER:
        dp = HARVARD_DATA_PROVIDER[set]

    return {"dataProvider": dp} if dp else {}

def identifier_transform(d):
    id = []
    obj = getprop(d, MODS + "identifier", True)
    if obj and "type" in obj and obj["type"] == "Object Number":
        id.append(obj["#text"])

    record_id = getprop(d, MODS + "recordInfo/recordIdentifier", True)
    if record_id and "source" in record_id and record_id["source"] == "VIA":
        id.append(record_id["#text"])

    id = filter(None, id)
    
    return {"identifier" : "; ".join(id)} if id else {}

# Structure mapping the original top level property to a function returning a single
# item dict representing the new property and its value
CHO_TRANSFORMER = {
    "collection"                                         : lambda d, p: {"collection": getprop(d, p)},
    MODS + "note"                                        : description_transform,
    MODS + "name"                                        : creator_and_contributor_transform,
    MODS + "genre"                                       : format_transform,
    MODS + "relatedItem"                                 : is_part_of_transform,
    MODS + "subject"                                     : subject_and_spatial_transform,
    MODS + "titleInfo"                                   : title_transform,
    MODS + "typeOfResource"                              : lambda d, p: {"type": getprop(d, p)},
    MODS + "originInfo"                                  : origin_info_transform,
    MODS + "language/languageTerm"                       : language_transform,
    MODS + "physicalDescription/extent"                  : lambda d, p: {"extent": getprop(d, p)}
}

AGGREGATION_TRANSFORMER = {
    "id"                         : lambda d, p: {"id": getprop(d, p), "@id" : "http://dp.la/api/items/"+getprop(d, p)},
    "_id"                        : lambda d, p: {"_id": getprop(d, p)},
    "ingestType"                 : lambda d, p: {"ingestType": getprop(d, p)},
    "ingestDate"                 : lambda d, p: {"ingestDate": getprop(d, p)},
    "originalRecord"             : lambda d, p: {"originalRecord": getprop(d, p)},
    "provider"                   : lambda d, p: {"provider": getprop(d, p)}
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
    out["sourceResource"].update(identifier_transform(data))
    out.update(url_transform(data))
    out.update(data_provider_transform(data))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
