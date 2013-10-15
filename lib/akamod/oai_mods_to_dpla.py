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
from dplaingestion.utilities import iterify, remove_key_prefix

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

# Common transforms
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
    for s in (iterify(v)):
        subject = []
        if "name" in s:
            subject.append(name_from_name_part(getprop(s, "name/namePart")))

        if "topic" in s:
            for t in (s["topic"] if isinstance(s["topic"], list) else
                      [s["topic"]]):
                if t not in subject:
                    subject.append(t)

        if "geographic" in s:
            for g in iterify(s["geographic"]):
                if g not in subject:
                    subject.append(g)
                if g not in val["spatial"]:
                    val["spatial"].append(g)

        if "hierarchicalGeographic" in s:
            for h in iterify(s["hierarchicalGeographic"]):
                if isinstance(h, dict):
                    for k in h.keys():
                        if k not in ["city", "county", "state", "country",
                                     "coordinates"]:
                            del h[k]
                    if h not in val["spatial"]:
                        val["spatial"].append(h)
                    if "country" in h:
                        subject.append(h["country"])

        coords = getprop(s, "cartographics/coordinates", True)
        if coords and coords not in val["spatial"]:
            val["spatial"].append(coords)

        if "temporal" in s:
            logger.debug("TEMPORAL: %s" % s["temporal"])

        val["subject"].append("--".join(subject))

    if not val["subject"]:
        del val["subject"]
    if not val["spatial"]:
        del val["spatial"]

    return val

def title_transform(d, p, unsupported_types=[], unsupported_subelements=[]):
    title = None

    v = getprop(d, p)
    for t in (v if isinstance(v, list) else [v]):
        if "type" in t and t["type"] in unsupported_types:
            continue

        title = t["title"]
        if "nonSort" in t and "nonSort" not in unsupported_subelements:
            title = t["nonSort"] + title
        if "subTitle" in t and "subTitle" not in unsupported_subelements:
            title = title + ": " + t["subTitle"]
        if "partNumber" in t and "partNumber" not in unsupported_subelements:
            part = t["partNumber"]
            if not isinstance(part, list):
                part = [part]
            part = ", ".join(part)
            title = title + ". " + part + "."
        if "partName" in t and "partName" not in unsupported_subelements:
            title = title + ". " + t["partName"]

    title = re.sub("\.\.", "\.", title)
    title = re.sub(",,", ",", title)

    return {"title": title} if title else {}

def format_transform(d, p, authority_condition=None):
    format = []
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else [v]):
        if isinstance(s, basestring):
            format.append(s)
        else:
            if authority_condition is not None:
                if "authority" in s and (s["authority"] == "marc" or
                                         s["authority"] == ""):
                    format.append(s["#text"])
            else:
                format.append(s["#text"])
    format = filter(None, format)

    return {"format": format} if format else {}

def type_transform(d, p):
    type = []
    for s in iterify(getprop(d, p)):
        if "#text" in s:
            type.append(s["#text"])
        else:
            type.append(s)

    return {"type": type} if type else {}
 
# Harvard transforms
def origin_info_transform_harvard(d, p):
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

def language_transform_harvard(d, p):
    language = []
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else [v]):
        if "#text" in s and s["#text"] not in language:
            language.append(s["#text"])

    return {"language": language} if language else {}

def is_part_of_transform_harvard(d, p):
    ipo = []
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else[v]):
        if "type" in v and v["type"] == "series":
            ipo.append(getprop(s, "titleInfo/title", True))

    ipo = filter(None, ipo)
    ipo = ipo[0] if len(ipo) == 1 else ipo

    return {"isPartOf": ipo} if ipo else {}

def title_transform_harvard(d, p):
    return title_transform(d, p,
                           unsupported_types=["alternative"],
                           unsupported_subelements=[])

def format_transform_harvard(d, p):
    return format_transform(d, p, authority_condition=True)

def url_transform_harvard(d):
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

def data_provider_transform_harvard(d):
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

def identifier_transform_harvard(d):
    id = []
    obj = getprop(d, MODS + "identifier", True)
    if obj and "type" in obj and obj["type"] == "Object Number":
        id.append(obj["#text"])

    record_id = getprop(d, MODS + "recordInfo/recordIdentifier", True)
    if record_id and "source" in record_id and record_id["source"] == "VIA":
        id.append(record_id["#text"])

    id = filter(None, id)
    
    return {"identifier" : "; ".join(id)} if id else {}

def description_transform_harvard(d, p):
    desc = getprop(d, p)
    if not isinstance(desc, basestring):
        desc = desc["#text"] if "#text" in desc else None

    return {"description": desc} if desc else {}

# BPL transforms
def origin_info_transform_bpl(d, p):
    origin_info = getprop(d, p)

    # date
    date = [] 
    for key in ["dateCreated", "dateIssued", "dateOther", "copyrightDate"]:
        _dict = getprop(origin_info, key, True)
        if _dict is not None:
            start_date = None
            end_date = None
            for item in iterify(_dict):
                if isinstance(item, basestring):
                    date.append(item)
                elif item.get("encoding") == "w3cdtf":
                    text = item.get("#text")
                    if item.get("point") == "start":
                        start_date = text
                    elif item.get("point") == "end":
                        end_date = text
                    else:
                        date.append(text)
            if start_date is not None and end_date is not None:
                date.append("%s-%s" %(start_date, end_date))

    # publisher
    publisher = []
    if "publisher" in origin_info:
        publisher.append(origin_info["publisher"])
    if "place" in origin_info:
        for p in iterify(origin_info["place"]):
            if getprop(p, "placeTerm/type", True) == "text":
                publisher.append(getprop(p, "placeTerm/#text", True))

    val = {}
    if date:
        val["date"] = date
    if publisher:
        val["publisher"] = publisher

    return val

def language_transform_bpl(d, p):
    language = []
    v = getprop(d, p)
    for s in iterify(v):
        if s.get("type") == "text" and s.get("authority") == "iso639-2b" and \
           s.get("authorityURI") == "http://id.loc.gov/vocabulary/iso639-2":
            language.append(s.get("#text"))

    return {"language": language} if language else {}

def is_part_of_transform_bpl(d, p):
    ipo = []
    v = getprop(d, p)
    host = None
    series = None
    for s in iterify(v):
        title = getprop(s, "titleInfo/title", True)
        if title is not None:
            if s.get("type") == "host":
                host = title
            if s.get("type") == "series":
                series = title

        if host:
            val = host
            if series:
                val += ". " + series
            ipo.append(val)

    ipo = ipo[0] if len(ipo) == 1 else ipo

    return {"isPartOf": ipo} if ipo else {}

def title_transform_bpl(d, p):
    return title_transform(d, p,
                           unsupported_types=[],
                           unsupported_subelements=["partNumber", "partName"])

def format_transform_bpl(d, p):
    return format_transform(d, p, authority_condition=False)
   
def identifier_transform_bpl(d, p):
    identifier = []
    types = ["local-accession", "local-other", "local-call", "local-barcode",
             "isbn", "ismn", "isrc", "issn", "issue-number", "lccn",
             "matrix-number", "music-plate", "music-publisher", "sici",
             "videorecording-identifier"]
    for item in iterify(getprop(d, p)):
        type = item.get("type")
        if type in types:
            type = " ".join(type.split("-"))
            identifier.append("%s: %s" % (type.capitalize(),
                                          item.get("#text")))
        else:
            logger.debug("Identifier type qualifier %s not in types for %s" %
                         (type, d["_id"]))

    return {"identifier": identifier} if identifier else {}

def rights_transform_bpl(d, p):
    rights = []
    for s in iterify(getprop(d, p)):
        rights.append(s.get("#text"))

    rights = ". ".join(rights).replace("..", ".")

    return {"rights": rights} if rights else {}

def location_transform_bpl(d, p): 
    def _get_media_type(d):
        pd = iterify(getprop(d, "physicalDescription", True))
        for _dict in filter(None, pd): 
            try:
                return getprop(_dict, "internetMediaType")
            except KeyError:
                pass

        return None

    location = iterify(getprop(d, p)) 
    format = _get_media_type(d)
    phys_location = None
    sub_location = None
    out = {}
    try:
        for _dict in location:
            if "url" in _dict:
                for url_dict in _dict["url"]:
                    if url_dict and "access" in url_dict:
                        if url_dict["access"] == "object in context" and \
                           url_dict.get("usage") == "primary":
                            out["isShownAt"] = url_dict.get("#text")
                        elif url_dict["access"] == "preview":
                            out["object"] = url_dict.get("#text")
                        elif url_dict["access"] == "raw object":
                            out["hasView"] = {"@id:": url_dict.get("#text"),
                                              "format": format}
            if phys_location is None:
                phys_location = getprop(_dict, "physicalLocation", True)
            if sub_location is None:
                sub_location = getprop(_dict, "holdingSimple/" +
                                       "copyInformation/subLocation", True)
        if phys_location is not None:
            out["dataProvider"] = phys_location
            if sub_location is not None:
                out["dataProvider"] += ". " + sub_location
    except Exception as e:
        logger.error(e)
    finally:
        return out

def description_transform_bpl(d, p):
    desc = []
    desc_props = [MODS + "abstract", MODS + "note"]
    for desc_prop in desc_props:
        v = getprop(d, desc_prop, True)
        if v is not None:
            for s in iterify(v):
                if isinstance(s, dict):
                    desc.append(s.get("#text"))
                else:
                    desc.append(s)
    desc = filter(None, desc)

    return {"description": desc} if desc else {}

CHO_TRANSFORMER = {}
AGGREGATION_TRANSFORMER = {}

CHO_TRANSFORMER["BPL"] = {
    MODS + "abstract"                  : description_transform_bpl,
    MODS + "note"                      : description_transform_bpl,
    MODS + "genre"                     : format_transform_bpl,
    MODS + "titleInfo"                 : title_transform_bpl,
    MODS + "identifier"                : identifier_transform_bpl,
    MODS + "originInfo"                : origin_info_transform_bpl,
    MODS + "language/languageTerm"     : language_transform_bpl,
    MODS + "relatedItem"               : is_part_of_transform_bpl,
    MODS + "accessCondition"           : rights_transform_bpl
}

CHO_TRANSFORMER["HARVARD"] = {
    MODS + "note"                      : description_transform_harvard,
    MODS + "genre"                     : format_transform_harvard,
    MODS + "relatedItem"               : is_part_of_transform_harvard,
    MODS + "titleInfo"                 : title_transform_harvard,
    MODS + "originInfo"                : origin_info_transform_harvard,
    MODS + "language/languageTerm"     : language_transform_harvard
}

CHO_TRANSFORMER["common"] = {
    "collection"                       : lambda d, p: {"collection":
                                                       getprop(d, p)},
    MODS + "name"                      : creator_and_contributor_transform,
    MODS + "subject"                   : subject_and_spatial_transform,
    MODS + "typeOfResource"            : type_transform,
    MODS + "physicalDescription/extent": lambda d, p: {"extent": getprop(d, p)}
}

AGGREGATION_TRANSFORMER["BPL"] = {
    MODS + "location": location_transform_bpl
}

AGGREGATION_TRANSFORMER["common"] = {
    "id"            : lambda d, p: {"id": getprop(d, p),
                                    "@id": "http://dp.la/api/items/" + 
                                           getprop(d, p)},
    "_id"           : lambda d, p: {"_id": getprop(d, p)},
    "provider"      : lambda d, p: {"provider": getprop(d, p)},
    "ingestType"    : lambda d, p: {"ingestType": getprop(d, p)},
    "ingestDate"    : lambda d, p: {"ingestDate": getprop(d, p)},
    "originalRecord": lambda d, p: {"originalRecord": getprop(d, p)},
}

@simple_service("POST", "http://purl.org/la/dp/oai_mods_to_dpla",
                "oai_mods_to_dpla", "application/ld+json")
def oaimodstodpla(body, ctype, geoprop=None, provider=None):
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

    if provider == "BPL":
        data = remove_key_prefix(data, "mods:")

    # Apply all transformation rules from original document
    transformer_pipeline = {}
    transformer_pipeline.update(CHO_TRANSFORMER.get(provider, {}),
                                **CHO_TRANSFORMER["common"])
    for p in transformer_pipeline:
        if exists(data, p):
            out["sourceResource"].update(transformer_pipeline[p](data, p))
    transformer_pipeline = {}
    transformer_pipeline.update(AGGREGATION_TRANSFORMER.get(provider, {}),
                                **AGGREGATION_TRANSFORMER["common"])
    for p in transformer_pipeline:
        if exists(data, p):
            out.update(transformer_pipeline[p](data, p))

    # Apply transformations that are dependent on more than one
    # original document field
    if provider == "HARVARD":
        out["sourceResource"].update(identifier_transform_harvard(data))
        out.update(url_transform_harvard(data))
        out.update(data_provider_transform_harvard(data))

    # Join dataProvider with isPartOf for BPL
    if provider == "BPL":
        try:
            ipo = getprop(out, "dataProvider") + ". " + \
                  getprop(out, "sourceResource/isPartOf")
            setprop(out, "sourceResource/isPartOf", ipo.replace("..", "."))
        except:
            pass

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
