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

def transform_thumbnail(d):
    url = None
    thumbs = arc_group_extraction(d, "objects", "object")
    if thumbs and not thumbs == [None]:
        d = thumbs
        if isinstance(thumbs, list) and thumbs:
            d = thumbs[0]
        
        if isinstance(d, dict) and "thumbnail-url" in d:
            url = d["thumbnail-url"]

    return {"object": url} if url else {}


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
    lods = ["series", "file unit"]
    items = arc_group_extraction(d, "hierarchy", "hierarchy-item")
    for item in (items if isinstance(items, list) else [items]):
        if item["hierarchy-item-lod"].lower() in lods:
            is_part_of.append("%s: %s" % (item["hierarchy-item-lod"],
                                          item["hierarchy-item-title"]))

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

def creator_transform(d):
    creator = None
    creators = arc_group_extraction(d, "creators", "creator")
    for c in (creators if isinstance(creators, list) else [creators]):
        if c["creator-type"] == "Most Recent":
            creator = c["creator-display"]
            break
    return {"creator": creator} if creator else {}

def extent_transform(d):
    extent = []
    extents = arc_group_extraction(d, "physical-occurrences",
                                   "physical-occurrence", "extent")
    if extents:
        [extent.append(e) for e in extents if e and e not in extent]

    return {"extent": extent} if extent else {}

def transform_state_located_in(d):
    phys = arc_group_extraction(d, "physical-occurrences", "physical-occurrence")
    state_located_in = None
    if phys:
        for p in phys:
            s = arc_group_extraction(p, "reference-units", "reference-unit", "state")[0]
            if s:
                state_located_in = s

    res = {"stateLocatedIn": {"name": state_located_in}}
    return res if state_located_in else {}

def data_provider_transform(d):
    data_provider = []
    phys = arc_group_extraction(d, "physical-occurrences",
                                "physical-occurrence")

    for p in phys:
        dp = arc_group_extraction(p, "reference-units", "reference-unit",
                                  "name")[0]
        if dp and dp not in data_provider:
            data_provider.append(dp)

    if len(data_provider) == 1:
        data_provider = data_provider[0]

    return {"dataProvider": data_provider} if data_provider else {}

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
   
    if subject:
        v["subject"] = subject
    if spatial:
        v["spatial"] = []
        # Remove duplicate values
        [v["spatial"].append(s) for s in spatial if not v["spatial"].count(s)]
    
    return v

def rights_transform(d):
    rights = []

    r = arc_group_extraction(d, "access-restriction", "restriction-status")
    if r and r[0] != None:
        rights.append("Restrictions: %s" % r[0])
    r = arc_group_extraction(d, "use-restriction", "use-status")
    if r and r[0] != None:
        rights.append("Use status: %s" % r[0])

    return {"rights": "; ".join(filter(None,rights))} if rights else {}

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
    "physical-occurrences"  : extent_transform,
    "creators"              : creator_transform,
    "hierarchy"             : is_part_of_transform,
    "release-dates"         : lambda d: date_transform(d,"release-dates", "release-date"),
    "broadcast-dates"       : lambda d: date_transform(d,"broadcast-dates", "broadcast-date"),
    "production-dates"      : lambda d: date_transform(d,"production-dates", "production-date"),
    "coverage-dates"        : lambda d: date_transform(d,"coverage-dates", ["cov-start-date", "cov-end-date"]),
    "copyright-dates"       : lambda d: date_transform(d,"copyright-dates", "copyright-date"),
    "title"                 : lambda d: {"title": d.get("title-only")},
    "scope-content-note"    : lambda d: {"description": d.get("scope-content-note")}, 
    "languages"             : lambda d: {"language": arc_group_extraction(d, "languages", "language")},
    "collection"            : lambda d: {"collection": d.get("collection")}
}

AGGREGATION_TRANSFORMER = {
    "id"                    : lambda d: {"id": d.get("id"), "@id" : "http://dp.la/api/items/"+d.get("id","")},
    "_id"                   : lambda d: {"_id": d.get("_id")},
    "originalRecord"        : lambda d: {"originalRecord": d.get("originalRecord",None)},
    "ingestType"            : lambda d: {"ingestType": d.get("ingestType")},
    "ingestDate"            : lambda d: {"ingestDate": d.get("ingestDate")},
    "arc-id-desc"           : is_shown_at_transform,
    "physical-occurrences"  : data_provider_transform
}

@simple_service("POST", "http://purl.org/la/dp/arc-to-dpla", "arc-to-dpla", "application/ld+json")
def arctodpla(body,ctype,geoprop=None):
    """   
    Convert output of JSON-ified ARC (NARA) format into the DPLA JSON-LD format.

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
    for p in data.keys():
        if p in CHO_TRANSFORMER:
            out["sourceResource"].update(CHO_TRANSFORMER[p](data))
        if p in AGGREGATION_TRANSFORMER:
            out.update(AGGREGATION_TRANSFORMER[p](data))

    # Apply transformations that are dependent on more than one
    # original document  field
    out["sourceResource"].update(type_transform(data))
    out["sourceResource"].update(rights_transform(data))
    out["sourceResource"].update(subject_and_spatial_transform(data))
    out.update(has_view_transform(data))
    out["sourceResource"].update(transform_state_located_in(data))

    if exists(out, "sourceResource/date"):
        logger.debug("OUTTYPE: %s"%getprop(out, "sourceResource/date"))

    if exists(data, "objects/object"):
        out.update(transform_thumbnail(data))

    # Additional content not from original document
    if "HTTP_CONTRIBUTOR" in request.environ:
        try:
            out["provider"] = json.loads(base64.b64decode(request.environ["HTTP_CONTRIBUTOR"]))
        except Exception as e:
            logger.debug("Unable to decode Contributor header value: "+request.environ["HTTP_CONTRIBUTOR"]+"---"+repr(e))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
