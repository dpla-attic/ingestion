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
from dplaingestion.selector import getprop, setprop, exists

GEOPROP = None

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
     "@id" : "dpla:end",
     "@type": "xsd:date"
   }
}


def transform_description(d):
    description = None
    items = arc_group_extraction(d, "freetext", "notes")
    for item in (items if isinstance(items, list) else [items]):
        if "@label" in item and item["@label"] == "Notes":
            if "#text" in item:
                description = item["#text"]
                break;
    return {"description": description} if description else {}


def extract_date(d, group_key, item_key):
    dates = []
    items = arc_group_extraction(d, group_key, item_key)
    for item in (items if isinstance(items, list) else [items]):
        if "#text" in item:
            dates.append(item["#text"])

    return {"date": "; ".join(dates)} if dates else {}


def date_transform(d, groupKey, itemKey):
    date = None
    if isinstance(itemKey, list):
        begin = arc_group_extraction(d, groupKey, itemKey[0])[0]
        end = arc_group_extraction(d, groupKey, itemKey[1])[0]
        date = "%s-%s" %(begin, end)
    else:
        date = arc_group_extraction(d, groupKey, itemKey)[0]

    return {"date": date} if date else {}


def is_part_of_transform(d):
    is_part_of = []
    items = arc_group_extraction(d, "freetext", "setName")
    for item in (items if isinstance(items, list) else [items]):
        if "#text" in item:
            is_part_of.append(item["#text"])

    return {"isPartOf": "; ".join(is_part_of)} if is_part_of else {}


def source_transform(d):
    source = None
    for s in d["handle"]:
        if is_absolute(s):
            source = s
            break
    return {"source": source} if source else {}


def is_shown_at_transform(d):
    object = "http://research.archives.gov/description/%s" % d["arc-id-desc"]

    return {"isShownAt": object}


def collection_transform(d):
    collections = []
    items = arc_group_extraction(d, "freetext", "setName")
    for item in (items if isinstance(items, list) else [items]):
        if "#text" in item:
            collections.append(item["#text"])
    return {"collection": collections} if collections else {}


def creator_transform(d):
    creator = None
    creators = arc_group_extraction(d, "freetext", "name")
    for c in (creators if isinstance(creators, list) else [creators]):
        if c["@label"] in creator_field_names:
            creator = c["#text"]
            break;
    return {"creator": creator} if creator else {}


def transform_format(d):
    f = []
    labels = ["Physical description", "Medium"]
    formats = arc_group_extraction(d, "freetext", "physicalDescription")
    [f.append(e["#text"]) for e in formats if e["@label"] in labels]

    return {"format": f} if f else {}


def transform_rights(d):
    p = []
    ps = arc_group_extraction(d, "freetext", "creditLine")
    if ps != [None]:
        [p.append(e["#text"]) for e in ps if "@label" in e and e["@label"] == "Credit line"]

    ps = arc_group_extraction(d, "freetext", "objectRights")
    if ps != [None]:
        [p.append(e["#text"]) for e in ps if "@label" in e and e["@label"] == "Rights"]

    return {"rights": p} if p else {}


def transform_publisher(d):
    p = []
    ps = arc_group_extraction(d, "freetext", "publisher")
    if ps:
        [p.append(e["#text"]) for e in ps]

    return {"publisher": p} if p else {}


def transform_place(d):
    place = []
    labels = ["Place", "Country", "Site"]
    places = arc_group_extraction(d, "freetext", "place")
    [place.append(e["#text"]) for e in places if e["@label"] in labels]

    return {"place": place} if place else {}


def transform_title(d):
    p = []
    labels = ["Title", "Object Name"]
    ps = arc_group_extraction(d, "title")
    if ps != [None]:
        [p.append(e["#text"]) for e in ps if e["@label"] in labels]
    
    return {"title": p} if p else {}


def transform_subject(d):
    p = []
    ps = arc_group_extraction(d, "freetext", "topic")
    if ps != [None]:
        [p.append(e["#text"]) for e in ps if e["@label"] == "Topic"]
    
    ps = arc_group_extraction(d, "freetext", "culture")
    if ps != [None]:
        [p.append(e["#text"]) for e in ps if e["@label"] == "Nationality"]

    return {"subject": p} if p else {}


def transform_identifier(d):
    extent = []
    extents = arc_group_extraction(d, "freetext", "identifier")
    [extent.append(e) for e in extents if e["@label"].startswith("Catalog") or e["@label"].startswith("Accession")]

    return {"extent": extent} if extent else {}


def extent_transform(d):
    extent = []
    extents = arc_group_extraction(d, "freetext", "physicalDescription")
    [extent.append(e) for e in extents if e["@label"] == "Dimensions"]

    return {"extent": extent} if extent else {}


def subject_and_spatial_transform(d):
    v = {}
    subject = []
    spatial = []

    sub = arc_group_extraction(d, "subject-references", "subject-reference")
    if sub:
        for s in sub:
            if s["subject-type"] == "SUBJ":
                subject.append(s["display-name"])
            if s["subject-type"] == "TGN":
                spatial.append(s["display-name"])

    phys = arc_group_extraction(d, "physical-occurrences",
                                "physical-occurrence")
    for p in phys:
        s = arc_group_extraction(p, "reference-units", "reference-unit",
                                 "state")[0]
        if s:
            logger.debug("SP: %s"%s)
            spatial.append(s)
   
    if subject:
        v["subject"] = subject
    if spatial:
        v["spatial"] = []
        # Remove duplicate values
        [v["spatial"].append(s) for s in spatial if not v["spatial"].count(s)]
    
    return v


def type_transform(d):
    type = []

    if "general-records-types" in d:
        type = arc_group_extraction(d, "general-records-types", "general-records-type",
                                    "general-records-type-desc")
    if "physical-occurrences" in d:
        phys_occur = getprop(d, "physical-occurrences/physical-occurrence")
        type_key = "media-occurrences/media-occurrence/media-type"
        for p in phys_occur:
            if exists(p, type_key):
                type.append(getprop(p, type_key))

    return {"type": "; ".join(type)} if type else {}


def has_view_transform(d):
    has_view = []

    def add_views(has_view,rge,url,format=None):
        for i in range(0,rge):
            view = {}
            view["url"] = url[i]
            if format:
                view["format"] = format[i]
            has_view.append(view)
        return has_view

    if "objects" in d:
        groupKey = "objects"
        itemKey = "object"
        urlKey = "file-url"
        formatKey = "mime-type"
        url = arc_group_extraction(d, groupKey, itemKey, urlKey)
        format = arc_group_extraction(d, groupKey, itemKey, formatKey)
        has_view = add_views(has_view, len(url), url, format)

    if "online-resources" in d:
        groupKey = "online-resources"
        itemKey = "online-resource"
        urlKey = "online-resource-url"
        url = arc_group_extraction(d, groupKey, itemKey, urlKey)
        has_view = add_views(has_view, len(url), url)

    return {"hasView": has_view} if has_view else {}


def arc_group_extraction(d, groupKey, itemKey, nameKey=None):
    """
    Generalization of what proved to be an idiom in ARC information extraction,
    e.g. in the XML structure;
    <general-records-types>
      <general-records-type num="1">
         <general-records-type-id>4237049</general-records-type-id>
         <general-records-type-desc>Moving Images</general-records-type-desc>
      </general-records-type>
    </general-records-types>

    "groupKey" is the name of the containing property, e.g. "creators"
    "itemKey" is the name of the contained property, e.g. "creator"
    "nameKey" is the property of the itemKey-named resource to be extracted
      if present, otherwise the value of the nameKey property
    """
    group = d.get(groupKey)
    if not group:
        data = None
    else:
        data = []
        # xmltodict converts what would be a list of length 1, into just that
        # lone dict. we have to deal with that twice here.
        # could definitely benefit from being more examplotron-like.
        if isinstance(group, list):
            # FIXME we pick the first in the list. unlikely to be
            # optimal and possibly incorrect in some cases
            item = group[0]
        else:
            item = group

        subitem = item.get(itemKey)
        if not isinstance(subitem,basestring):
            for s in (subitem if isinstance(subitem,list) else [subitem]):
                if nameKey:
                    data.append(s.get(nameKey,None))
                else:
                    data.append(s)
        else:
            data.append(subitem)

    return data

# Structure mapping the original top level property to a function returning a single
# item dict representing the new property and its value
CHO_TRANSFORMER = {
    "freetext/physicalDescription"  : extent_transform,
    "freetext/name"                 : creator_transform,
    "freetext/setName"              : is_part_of_transform,
    "freetext/date"                 : lambda d: extract_date(d,"freetext","date"),
    "freetext/notes"                : transform_description,
    "freetext/identifier"           : transform_identifier,
    "language"                      : lambda d: {"language": d.get("language") },
    "freetext/physicalDescription"  : transform_format,
    "freetext/place"                : transform_place,
    "freetext/publisher"            : transform_publisher,
    "title"                         : transform_title,
}

AGGREGATION_TRANSFORMER = {
    "id"                    : lambda d: {"id": d.get("id"), "@id" : "http://dp.la/api/items/"+d.get("id","")},
    "_id"                   : lambda d: {"_id": d.get("_id")},
    "originalRecord"        : lambda d: {"originalRecord": d.get("originalRecord",None)},
    "ingestType"            : lambda d: {"ingestType": d.get("ingestType")},
    "ingestDate"            : lambda d: {"ingestDate": d.get("ingestDate")},
    "collection"            : lambda d: {"collection": d.get("collection")},
}

@simple_service("POST", "http://purl.org/la/dp/edan-to-dpla", "edan-to-dpla", "application/ld+json")
def edantodpla(body,ctype,geoprop=None):
    """   
    Convert output of JSON-ified EDAN (Smithsonian) format into the DPLA JSON-LD format.

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
        "aggregatedCHO": {}
    }

    logger.debug("x"*60)
    # Apply all transformation rules from original document
    for k, v in CHO_TRANSFORMER.items():
        if exists(data, k):
            out["aggregatedCHO"].update(v(data))
    for k, v in AGGREGATION_TRANSFORMER.items():
        if exists(data, k):
            out.update(v(data))

    # Apply transformations that are dependent on more than one
    # original document  field
    #out["aggregatedCHO"].update(type_transform(data))
    out["aggregatedCHO"].update(transform_rights(data))
    out["aggregatedCHO"].update(transform_subject(data))
    #out["aggregatedCHO"].update(subject_and_spatial_transform(data))
    #out.update(has_view_transform(data))

    logger.debug("x"*60)

    if exists(out, "aggregatedCHO/date"):
        logger.debug("OUTTYPE: %s"%getprop(out, "aggregatedCHO/date"))

    # Additional content not from original document
    if "HTTP_CONTRIBUTOR" in request.environ:
        try:
            out["provider"] = json.loads(base64.b64decode(request.environ["HTTP_CONTRIBUTOR"]))
        except Exception as e:
            logger.debug("Unable to decode Contributor header value: "+request.environ["HTTP_CONTRIBUTOR"]+"---"+repr(e))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)


creator_field_names = [
"Architect",
"Artist",
"Artists/Makers",
"Attributed to",
"Author",
"Cabinet Maker",
"Ceramist",
"Circle of",
"Co-Designer",
"Creator",
"Decorator",
"Designer",
"Draftsman",
"Editor",
"Embroiderer",
"Engraver",
"Etcher",
"Executor",
"Follower of",
"Graphic Designer",
"Instrumentiste",
"Inventor",
"Landscape Architect",
"Landscape Designer",
"Maker",
"Model Maker/maker",
"Modeler",
"Painter",
"Photographer",
"Possible attribution",
"Possibly",
"Possibly by",
"Print Maker",
"Printmaker",
"Probably",
"School of",
"Sculptor",
"Studio of",
"Workshop of",
"Weaver",
"Writer",
"animator",
"architect",
"artist",
"artist.",
"artist?",
"artist attribution",
"author",
"author.",
"author?",
"authors?",
"caricaturist",
"cinematographer",
"composer",
"composer, lyricist",
"composer; lyrcist",
"composer; lyricist",
"composer; performer",
"composer; recording artist",
"composer?",
"creator",
"creators",
"designer",
"developer",
"director",
"editor",
"engraver",
"ethnographer",
"fabricator",
"filmmaker",
"filmmaker, anthropologist",
"garden designer",
"graphic artist",
"illustrator",
"inventor",
"landscape Architect",
"landscape architect",
"landscape architect, photographer",
"landscape designer",
"lantern slide maker",
"lithographer",
"lyicist",
"lyicrist",
"lyricist",
"lyricist; composer",
"maker",
"maker (possibly)",
"maker or owner",
"maker; inventor",
"original artist",
"performer",
"performer; composer; lyricist",
"performer; recording artist",
"performers",
"performing artist; recipient",
"performing artist; user",
"photgrapher",
"photograher",
"photographer",
"photographer and copyright claimant",
"photographer and/or colorist",
"photographer or collector",
"photographer?",
"photographerl",
"photographerphotographer",
"photographers",
"photographers?",
"photographer}",
"photographic firm",
"photogrpaher",
"playwright",
"poet",
"possible maker",
"printer",
"printmaker",
"producer",
"recordig artist",
"recording artist",
"recording artist; composer",
"recordist",
"recordng artist",
"sculptor",
"shipbuilder",
"shipbuilders",
"shipping firm",
"weaver",
"weaver or owner",
]
