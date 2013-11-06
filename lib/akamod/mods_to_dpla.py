from akara import logger
from akara import request, response
from akara.services import simple_service
from amara.thirdparty import json
import base64
from dplaingestion.utilities import iterify, remove_key_prefix
from dplaingestion.selector import getprop as selector_getprop, exists


def getprop(d, p):
    return selector_getprop(d, p, True)

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
    }
}

# UVA specific transforms
def subject_transform_uva(d, p):
    def _join_on_double_hyphen(subject_list):
        if any(["--" in s for s in subject_list]):
            return subject_list
        else:
            return ["--".join(subject_list)]

    subject = []
    for _dict in iterify(getprop(d, p)):
        # Extract subject from both "topic" and "name" fields
        if "topic" in _dict:
            topic = _dict["topic"]
            if isinstance(topic, list):
                subject.extend(_join_on_double_hyphen(topic))
            else:
                subject.append(topic)
        if "name" in _dict:
            name_part = getprop(_dict, "name/namePart")
            if isinstance(name_part, list):
                subj = [None, None, None]
                for name in name_part:
                    if isinstance(name, basestring):
                        subj[0] = name
                    elif isinstance(name, dict):
                        if name.get("type") == "termsOfAddress":
                            subj[1] = name["#text"]
                        elif name.get("type") == "date":
                            subj[2] = name["#text"]
                if subj:
                    subject.append(" ".join(filter(None, subj)))
                    
            else:
                subject.append(name_part)

    return {"subject": subject} if subject else {}

def creator_transform_uva(d, p):
    personal_creator = []
    corporate_creator = []
    for s in iterify(getprop(d, p)):
        creator = [None, None, None]
        for name in iterify(s.get("namePart")):
            if isinstance(name, basestring):
                    creator[0] = name
            elif isinstance(name, dict):
                type = name.get("type")
                if type == "family":
                    creator[0] = name.get("#text")
                elif type == "given":
                    creator[1] = name.get("#text")
                elif type == "termsOfAddress":
                    creator[1] = name.get("#text")
                elif type == "date":
                    creator[2] = name.get("#text")

        creator = ", ".join(filter(None, creator))

        if s.get("type") == "personal" and creator not in personal_creator:
            personal_creator.append(creator)
        elif s.get("type") == "corporate" and creator not in corporate_creator:
            corporate_creator.append(creator)

    if personal_creator:
        return {"creator": personal_creator}
    elif corporate_creator:
        return {"creator": corporate_creator}
    else:
        return {}

def title_transform_uva(d, p):
    title = []
    for s in iterify(getprop(d, p)):
        if isinstance(s, basestring):
            title.append(s)
        elif isinstance(s, dict):
            t = s.get("title")
            if t and "nonSort" in s:
                t = s.get("nonSort") + t
            title.append(t)
    title = filter(None, title)

    return {"title": title[-1]} if title else {}

def date_transform_uva(d, p):
    date = []

    date_prop = p + "/dateIssued"
    if not exists(d, date_prop):
        date_prop = p + "/dateCreated"

    for s in iterify(getprop(d, date_prop)):
        if isinstance(s, basestring):
            date.append(s)
        elif isinstance(s, dict):
            date.append(s.get("#text"))

    date = filter(None, date)

    return {"date": date} if date else {}

def identifier_transform_uva(d, p):
    identifier = []
    for s in iterify(getprop(d, p)):
        if isinstance(s, dict) and s.get("type") == "uri":
            identifier.append(s.get("#text"))
    identifier = "-".join(filter(None, identifier))

    return {"identifier": identifier} if identifier else {}

def provider_transform_uva(d, p):
    provider = getprop(d, p)

    return {"provider": provider} if isinstance(provider, basestring) else {}

def location_transform_uva(d, p):
    def _get_media_type(d):
        pd = iterify(getprop(d, "physicalDescription"))
        for _dict in pd:
            try:
                return selector_getprop(_dict, "internetMediaType")
            except KeyError:
                pass

    location = iterify(getprop(d, p))
    format = _get_media_type(d)
    out = {}
    try:
        for _dict in location:
            if "url" in _dict:
                for url_dict in _dict["url"]:
                    if url_dict and "access" in url_dict:
                        if url_dict["access"] == "object in context":
                            out["isShownAt"] = url_dict.get("#text")
                        elif url_dict["access"] == "preview":
                            out["object"] = url_dict.get("#text")
                        elif url_dict["access"] == "raw object":
                            out["hasView"] = {"@id:": url_dict.get("#text"), "format": format}
            if "physicalLocation" in _dict and isinstance(_dict["physicalLocation"], basestring):
                out["dataProvider"] = _dict["physicalLocation"]
    except Exception as e:
        logger.error(e)
    finally:
        return out

def spatial_transform_uva(d):
    spatial = []
    if "subject" in d:
        for s in iterify(getprop(d, "subject")):
            if "hierarchicalGeographic" in s:
                spatial = s["hierarchicalGeographic"]
                spatial["name"] = ", ".join(filter(None,
                                                   [spatial.get("city"),
                                                    spatial.get("county"),
                                                    spatial.get("state"),
                                                    spatial.get("country")]))
                spatial = [spatial]

    if not spatial and exists(d, "originInfo/place"):
        for s in iterify(getprop(d, "originInfo/place")):
            if "placeTerm" in s:
                for place in iterify(s["placeTerm"]):
                    if "type" in place and place["type"] != "code":
                        spatial.append(place["#text"])

    return {"spatial": spatial} if spatial else {}

def multi_field_transforms_uva(d, p):
    out = {}
    out.update(spatial_transform_uva(d))

    return out

def physical_description_transform_uva(d, p):
    pd = iterify(getprop(d, p))
    out = {}
    for _dict in pd:
        note = getprop(_dict, "note")
        if note:
            for sub_dict in note:
                if isinstance(sub_dict, dict) and "displayLabel" in sub_dict:
                    if sub_dict["displayLabel"] == "size inches":
                        out["extent"] = sub_dict.get("#text")
                    elif sub_dict["displayLabel"] == "condition":
                        out["description"] = sub_dict.get("#text")
        if "form" in _dict:
            for form in iterify(_dict["form"]):
                if "#text" in form:
                    out["format"] = form["#text"]
                    break
    return out

# NYPL specific transforms
def spatial_transform_nypl(d, p):
    spatial = []
    for s in iterify(getprop(d, p)):
        if (isinstance(s, dict) and exists(s, "geographic/authority") and
            getprop(s, "geographic/authority") == "naf"):
            spatial.append(getprop(s, "geographic/#text"))
    spatial = filter(None, spatial)

    return {"spatial": spatial} if spatial else {}

def title_transform_nypl(d, p):
    title_info = iterify(getprop(d, p))
    # Title is in the last titleInfo element
    try:
        title = title_info[-1].get("title")
    except:
        logger.error("Error setting sourceResource.title for %s" % d["_id"])
    
    return {"title": title} if title else {}

def identifier_transform_nypl(d, p):
    identifier = [s.get("#text") for s in iterify(getprop(d, p)) if
                  isinstance(s, dict) and s.get("type") in
                  ("local_bnumber", "uuid")]
    idenfitier = filter(None, identifier)

    return {"identifier": identifier} if identifier else {}

def creator_transform_nypl(d, p):
    def _update_value(d, k, v):
        if k in d:
            if isinstance(d[k], list):
                d[k].append(v)
            else:
                d[k] = [d[k], v]
        else:
            d[k] = v

    creator_roles = frozenset(("architect", "artist", "author", "cartographer",
                     "composer", "creator", "designer", "director",
                     "engraver", "interviewer", "landscape architect",
                     "lithographer", "lyricist", "musical director",
                     "performer", "project director", "singer", "storyteller",
                     "surveyor", "technical director", "woodcutter"))
    names = iterify(getprop(d, p))
    out = {}
    for creator_dict in names:
        if isinstance(creator_dict, dict) and "type" in creator_dict and "namePart" in creator_dict:
            if creator_dict["type"] == "personal" and exists(creator_dict, "role/roleTerm"):
                roles = frozenset([role_dict["#text"].lower() for role_dict in creator_dict["role"]["roleTerm"] if "#text" in role_dict])
                name = creator_dict["namePart"]
                if "publisher" in roles:
                    _update_value(out, "publisher", name)
                elif roles & creator_roles:
                    _update_value(out, "creator", name)
                else:
                    _update_value(out, "contributor", name)
    return out

def date_transform_nypl(d, p):
    def _date_created(d, p):
        date_created_list = iterify(getprop(d, p))
        keyDate, startDate, endDate = None, None, None
        for _dict in date_created_list:
            if not isinstance(_dict, dict):
                continue
            if _dict.get("keyDate") == "yes":
                keyDate = _dict.get("#text")
            if _dict.get("point") == "start":
                startDate = _dict.get("#text")
            elif _dict.get("point") == "end":
                endDate = _dict.get("#text")
        if startDate and endDate:
            return {"date": "{0} - {1}".format(startDate, endDate)}
        else:
            return {"date": keyDate}

    originInfo = getprop(d, p)
    date_field_check_order = ("dateCreated", "dateIssued")
    for field in date_field_check_order:
        if field in originInfo:
            return _date_created(d, p + "/" + field)
    return {}

def description_transform_nypl(d, p):
    note = getprop(d, "note")
    abstract = getprop(d, "abstract")

    # Extract note values first
    if note is not None:
        desc = []
        for s in iterify(note):
            if "#text" in s:
                desc.append(s["#text"])
            else:
                desc.append(s)

    # Override note values if abstract exists
    if abstract is not None:
        desc = []
        for s in iterify(abstract):
            if "#text" in s:
                desc.append(s["#text"])
            else:
                desc.append(s)

    return {"description": desc} if desc else {}

def is_shown_at_transform_nypl(d, p):
    is_shown_at = None
    collection_title = getprop(d, p + "/title")

    if collection_title:
        # Handle NYPL isShownAt for collection with title
        # "Lawrence H. Slaughter Collection of English maps, charts, 
        #  globes, books and atlases"
        # For collection with title
        # "Thomas Addis Emmet collection, 1483-1876, bulk (1700-1800)",
        # use placeholder item link: http://archives.nypl.org/mss/927
        lawrence_title = ("Lawrence H. Slaughter Collection of English maps," +
                          " charts, globes, books and atlases")
        emmet_title = ("Thomas Addis Emmet collection, 1483-1876, bulk" +
                       " (1700-1800)")
        placeholder_item_link = "http://archives.nypl.org/mss/927"
        if exists(d, "tmp_item_link"):
            if collection_title == lawrence_title:
                is_shown_at = getprop(d, "tmp_item_link")
            elif collection_title == emmet_title:
                is_shown_at = placeholder_item_link

    return {"isShownAt": is_shown_at} if is_shown_at else {}

def collection_transform_nypl(d, p):
    collection = getprop(d, p)
    title_info = getprop(d, "titleInfo")
    if title_info is not None:
        collection["title"] = iterify(title_info)[0].get("title")

    return {"collection": collection}

# MODS transforms (applies to both UVA and NYPL)
def language_transform(d, p):
    language = []
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else [v]):
        if "#text" in s and s["#text"] not in language:
            language.append(s["#text"])

    return {"language": language} if language else {}

CHO_TRANSFORMER = {}
AGGREGATION_TRANSFORMER = {}

# UVA TRANSFORMERs
CHO_TRANSFORMER["UVA"] = {
    "name"                  : creator_transform_uva,
    "subject"               : subject_transform_uva,
    "titleInfo"             : title_transform_uva,
    "identifier"            : identifier_transform_uva,
    "originInfo"            : date_transform_uva,
    "physicalDescription"   : physical_description_transform_uva,
    "language/languageTerm" : language_transform,
    "collection"            : lambda d, p: {"collection": getprop(d, p)},
    "originInfo/publisher"  : lambda d, p: {"publisher": getprop(d, p)},
    "typeOfResource/#text"  : lambda d, p: {"type": getprop(d, p)},
    "accessCondition"       : lambda d, p: {"rights": [s["#text"] for s in
                                                       getprop(d, p) if "#text"
                                                       in s]},

    # Run multi-field dependent transforms. Using the "_id" as the key
    # guarantees that multi_field_transforms will run since all records
    # contain an "_id"
    "_id"                   : multi_field_transforms_uva
}

AGGREGATION_TRANSFORMER["UVA"] = {
    "location"                      : location_transform_uva,
    "recordInfo/recordContentSource": provider_transform_uva
}

# NYPL TRANSFORMERs
CHO_TRANSFORMER["NYPL"] = {
    "name"                          : creator_transform_nypl,
    "subject"                       : spatial_transform_nypl,
    "titleInfo"                     : title_transform_nypl,
    "originInfo"                    : date_transform_nypl,
    "identifier"                    : identifier_transform_nypl,
    "typeOfResource/#text"          : lambda d, p: {"type": getprop(d, p)},
    "relatedItem/titleInfo/title"   : lambda d, p: {"isPartOf": getprop(d, p)},
    "collection"                    : collection_transform_nypl,
    "note"                          : description_transform_nypl,
    "abstract"                      : description_transform_nypl
}

AGGREGATION_TRANSFORMER["NYPL"] = {
    "collection": is_shown_at_transform_nypl,
}

# Common TRANSFORMERs
CHO_TRANSFORMER["common"] = {}
AGGREGATION_TRANSFORMER["common"] = {
    "_id"                        : lambda d, p: {"_id": getprop(d, p)},
    "ingestType"                 : lambda d, p: {"ingestType": getprop(d, p)},
    "ingestDate"                 : lambda d, p: {"ingestDate": getprop(d, p)},

    "originalRecord"             : lambda d, p: ({"originalRecord":
                                                 getprop(d, p)}),

    "id"                         : lambda d, p: ({"id": getprop(d, p),
                                                 "@id": "http://dp.la/api/" +
                                                 "items/" + getprop(d, p)}),
    "provider"                   : lambda d, p: {"provider": getprop(d, p)}
}


@simple_service('POST', 'http://purl.org/la/dp/mods-to-dpla', 'mods-to-dpla',
                'application/ld+json')
def mods_to_dpla(body, ctype, geoprop=None, provider=None):
    """
    Convert output of JSON-ified MODS/METS format into the DPLA JSON-LD format.

    Parameter "geoprop" specifies the property name containing lat/long coords.

    Parameter "provider" specifies the mapping dictionary.
    """

    try :
        data = json.loads(body)
    except Exception:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    global GEOPROP
    GEOPROP = geoprop

    out = {
        "@context": CONTEXT,
        "sourceResource" : {}
    }

    if provider == "UVA":
        # Remove "mods:" prefix in keys
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

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
