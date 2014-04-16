from akara import logger
from akara import request, response
from akara.services import simple_service
from amara.thirdparty import json
import base64
from dplaingestion.utilities import iterify, remove_key_prefix
from dplaingestion.selector import getprop as selector_getprop, exists
from dplaingestion.marc_code_to_relator import MARC_CODE_TO_RELATOR


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
                            out["hasView"] = {"@id": url_dict.get("#text"), "format": format}
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

# MODS transforms (applies to both UVA and NYPL)
def language_transform(d, p):
    language = []
    v = getprop(d, p)
    for s in (v if isinstance(v, list) else [v]):
        if "#text" in s and s["#text"] not in language:
            language.append(s["#text"])

    return {"language": language} if language else {}

def description_transform_nypl(d):
    def txt(n):
        if not n:
            return ""
        if type(n) == dict:
            return n.get("#text") or ""
        elif isinstance(n, basestring):
            return n
        else:
            return ""
    note = txt(getprop(d, "note"))
    pnote = txt(getprop(d, "physicalDescription/note"))
    return {"description": note or pnote}

def format_transform_nypl(d, p):
    format = []
    for v in iterify(getprop(d, p)):
        if isinstance(v, dict):
            f = v.get("$")
            if not f:
                msg = "An item in physicalDescription/form has an empty " + \
                      "\"$\" field; %s" % d["_id"]
                logger.error(msg)
            else:
                format.append(f)
        else:
            msg = "Value in physicalDescription/form not a dict; %s" % d["_id"]

    return {"format": format} if format else {}

def language_transform_nypl(d, p):
    code = ""
    text = ""
    for v in iterify(getprop(d, p)):
        if v.get("type") == "code":
            if not code:
                code = v.get("$")
            else:
                msg = "Multiple codes in language/languageTerm; %s" % d["_id"]
                logger.error(msg)
        if v.get("type") == "text":
            if not text:
                text = v.get("$")
            else:
                msg = "Multiple texts in language/languageTerm; %s" % d["_id"]
                logger.error(msg)
    if code and text:
        return {"language": {"name": text, "iso639_3": code}}
    else:
        logger.error("Only one value (code/text) found; %s" % d["_id"])
        return {}

def date_publisher_and_spatial_transform_nypl(d, p):
    """
    Examine the many possible originInfo elements and pick out date, spatial,
    and publisher information.

    Dates may come in multiple originInfo elements, in which case we take the
    last one.
    """
    date = []
    spatial = []
    publisher = []
    date_fields = ("dateIssued", "dateCreated", "dateCaptured", "dateValid",
                   "dateModified", "copyrightDate", "dateOther")
    date_origin_info = []

    def datestring(date_data):
        """
        Given a "date field" element from inside an originInfo, return a
        string representation of the date or dates represented.
        """
        if type(date_data) == dict:
            # E.g. single dateCaptured without any attributes; just take it.
            return date_data.get("#text")
        if type(date_data) == unicode:
            return date_data
        keyDate, startDate, endDate = None, None, None
        for _dict in date_data:
            if _dict.get("keyDate") == "yes":
                keyDate = _dict.get("#text")
            if _dict.get("point") == "start":
                startDate = _dict.get("#text")
            elif _dict.get("point") == "end":
                endDate = _dict.get("#text")
        if startDate and endDate:
            return "%s - %s" % (startDate, endDate)
        elif keyDate:
            return keyDate
        else:
            return None

    for origin_info in iterify(getprop(d, p)):
        # Put aside date-related originInfo elements for later ...
        for field in date_fields:
            if field in origin_info:
                date_origin_info.append(origin_info)
                break
        else:
            # Map publisher
            if ("publisher" in origin_info and
                    origin_info["publisher"] not in publisher):
                publisher.append(origin_info["publisher"])
            # Map spatial
            if exists(origin_info, "place/placeTerm"):
                for place_term in iterify(getprop(origin_info, "place/placeTerm")):
                    if isinstance(place_term, basestring):
                        pass
                    elif isinstance(place_term, dict):
                        place_term = place_term.get("#text")
                        if place_term is None:
                            logger.error("No/empty field #text in originInfo/" +
                                         "place/placeTerm; %s" % d["_id"])
                            continue
                    else:
                        logger.error("Value in originInfo/place/placeTerm is " +
                                     "neither a string or a dict; %s" % d["_id"])
                        continue
                    if place_term not in spatial:
                        spatial.append(place_term)

    # Map dates.  Only use the last date-related originInfo element.
    try:
        last_date_origin_info = date_origin_info[-1]
        for field in date_fields:
                if field in last_date_origin_info:
                    s = datestring(last_date_origin_info[field])
                    if s not in date:
                        date.append(s)
    except Exception as e:
        logger.info("Can not get date from %s" % d["_id"])

    out = {}
    if date:
        out["date"] = date
    if spatial:
        out["spatial"] = spatial
    if publisher:
        out["publisher"] = publisher

    return out

def identifier_transform_nypl(d, p):
    id_values = ("local_imageid", "isbn", "isrc", "isan", "ismn", "iswc",
                 "issn","uri", "urn")
    identifier = []
    for v in iterify(getprop(d, p)):
        for id_value in id_values:
            if (id_value in v.get("displayLabel", "") or
                id_value in v.get("type", "")):
                identifier.append(v.get("#text"))
                break

    return {"identifier": identifier} if identifier else {}

def spec_type_transform_nypl(d, p):
    spec_type = []
    for v in iterify(getprop(d, p)):
        if isinstance(v, basestring):
            if "book" in v or "periodical" in v or "magazine" in v:
                spec_type.append(v)

    return {"specType": spec_type} if spec_type else {}

def subject_transform_nypl(d):
    # Mapped from subject and genre
    #
    # Per discussion with Amy on 10 April 2014, don't worry
    # about checking whether heading maps to authority file. Amy simplified
    # the crosswalk.
    #
    # TODO: When present, we should probably pull in the valueURI and
    # authority values into the sourceResource.subject - this would represent
    # an index/API change, however. 
    subject = []

    if exists(d, "subject"):
        for v in iterify(getprop(d, "subject")):
            if "topic" in v:
                if isinstance(v, basestring):
                    subject.append(v["topic"])
                elif isinstance(v["topic"], dict):
                    subject.append(v["topic"].get("#text"))
                else:
                    logger.error("Topic is not a string nor a dict; %s" % d["_id"])
            if exists(v, "name/namePart"):
                subject.append(getprop(v, "name/namePart"))

    if exists(d, "genre"):
        for v in iterify(getprop(d, "genre")):
            if isinstance(v, basestring):
                subject.append(v)
            elif isinstance(v, dict):
                subject.append(v.get("#text"))
            else:
                logger.error("Genre is not a string nor a dict; %s" % d["_id"])

    return {"subject": subject} if subject else {}

def collection_and_relation_transform_nypl(d):
    out = {
        "collection": getprop(d, "collection")
    }

    if exists(d, "relatedItem"):
        related_items = iterify(getprop(d, "relatedItem"))
        # Map relation
        relation = filter(None, [getprop(item, "titleInfo/title") for item in
                                 related_items])
        if relation:
            relation.reverse()
            relation = ". ".join(relation).replace("..", ".")
            out["relation"] = relation


        # Map collection title
        host_types = [item for item in related_items if
                      item.get("type") == "host"]
        if host_types:
            title = getprop(host_types[-1], "titleInfo/title")
            if title:
                out["collection"]["title"] = title

    return out

def data_provider_transform_nypl(d, p):
    data_provider = None
    for v in iterify(getprop(d, p)):
        if "physicalLocation" in v:
            for p in iterify(v["physicalLocation"]):
                if (p.get("type") == "division" and
                    p.get("authority") != "marcorg"):
                    phys_location = p.get("#text")
                    while phys_location.endswith("."):
                        phys_location = phys_location[:-1]
                    data_provider = phys_location + \
                                    ". The New York Public Library" 

    return {"dataProvider": data_provider} if data_provider else {}

NYPL_CREATOR_ROLES = ["architect", "artist", "author", "cartographer",
    "composer", "creator", "designer", "director", "engraver", "interviewer",
    "landscape architect", "lithographer", "lyricist", "musical director",
    "performer", "project director", "singer", "storyteller", "surveyor",
    "technical director", "woodcutter"]

def creator_and_contributor_transform_nypl(d, p):
    creator = set()
    contributor = set()
    for v in iterify(getprop(d, p)):
        if exists(v, "role"):
            for role in iterify(v["role"]):
                if exists(role, "roleTerm"):
                    for role_term in iterify(role["roleTerm"]):
                        rt = role_term.get("#text").lower().strip(' .')
                        if rt in NYPL_CREATOR_ROLES:
                            creator.add(v["namePart"])
                        elif rt != "publisher":
                            contributor.add(v["namePart"])

    out = {}
    if creator:
        out["creator"] = list(creator)
    cont = contributor - creator
    if cont:
        out['contributor'] = list(cont)

    return out


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
    "physicalDescription/form"      : format_transform_nypl,
    "language/languageTerm"         : language_transform_nypl,
    "originInfo"                    : date_publisher_and_spatial_transform_nypl,
    "identifier"                    : identifier_transform_nypl,
    "genre"                         : spec_type_transform_nypl,
    "titleInfo"                     : title_transform_nypl,
    "name"                          : creator_and_contributor_transform_nypl,
    "physicalDescription/extent"    : lambda d, p: {"extent": getprop(d, p)},
    "tmp_rights_statement"          : lambda d, p: {"rights": getprop(d, p)},
    "subject/temporal"              : lambda d, p: {"temporal": getprop(d, p)},
    "typeOfResource"                : lambda d, p: {"type": getprop(d, p)},
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
    "provider"                   : lambda d, p: {"provider": getprop(d, p)},
    "location"                   : data_provider_transform_nypl,
    "tmp_item_link"              : lambda d, p: {"isShownAt": getprop(d, p)}
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

    out["sourceResource"].update(description_transform_nypl(data))
    out["sourceResource"].update(subject_transform_nypl(data))
    out["sourceResource"].update(collection_and_relation_transform_nypl(data))
    out["sourceResource"].update({"stateLocatedIn": "New York"})

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
