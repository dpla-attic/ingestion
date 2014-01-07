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
from dplaingestion.utilities import iterify

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
    objects = arc_group_extraction(d, "objects", "object")
    for object in objects:
        if "thumbnail-url" in object:
            url = object["thumbnail-url"]
            break

    return {"object": url} if url else {}

def date_transform(d):
    date = None
    groupKeys = ["coverage-dates", "copyright-dates", "production-dates",
                 "broadcast-dates", "release-dates"]
    for groupKey in groupKeys:
        if groupKey in d:
            if groupKey == "coverage-dates":
                begin = arc_group_extraction(d, groupKey, "cov-start-date")
                end = arc_group_extraction(d, groupKey, "cov-end-date")
                date = "%s-%s" % (begin, end)
            else:
                date = arc_group_extraction(d, groupKey, groupKey[:-1])
            if date: break

    return {"date": date} if date else {}

def relation_transform(d):
    relation = []
    lods = ["series", "file unit"]
    items = arc_group_extraction(d, "hierarchy", "hierarchy-item")
    for item in iterify(items):
        if item["hierarchy-item-lod"].lower() in lods:
            relation.append(item["hierarchy-item-title"])

    return {"relation": relation} if relation else {}

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
    for c in iterify(creators):
        if c["creator-type"] == "Most Recent":
            creator = c["creator-display"]
            break
    return {"creator": creator} if creator else {}

def extent_transform(d):
    extent = []
    extents = arc_group_extraction(d, "physical-occurrences",
                                   "physical-occurrence", "extent")
    [extent.append(e) for e in extents if e not in extent]

    return {"extent": extent} if extent else {}

def transform_state_located_in(d):
    phys = arc_group_extraction(d, "physical-occurrences",
                                "physical-occurrence")
    state_located_in = None
    for p in phys:
        s = arc_group_extraction(p, "reference-units", "reference-unit",
                                 "state")
        if s:
            state_located_in = s

    res = {"stateLocatedIn": {"name": state_located_in}}
    return res if state_located_in else {}

def data_provider_transform(d):
    data_provider = None
    phys_occur = arc_group_extraction(d, "physical-occurrences",
                                      "physical-occurrence")

    # Use "Reproduction-Reference" copy-status
    p = [phys for phys in phys_occur if "copy-status" in phys and
         phys.get("copy-status") == "Reproduction-Reference"]
    if not p:
        # Use "Preservation" copy-status
        p = [phys for phys in phys_occur if "copy-status" in phys and
             phys.get("copy-status") == "Preservation"]

    if p:
        ref = arc_group_extraction(p[0], "reference-units",
                                  "reference-unit")
        if ref and ref[0]["num"] == "1":
            data_provider = ref[0]["name"]

    return {"dataProvider": data_provider} if data_provider else {}

def subject_and_spatial_transform(d):
    v = {}
    subject = []
    spatial = []

    sub = arc_group_extraction(d, "subject-references", "subject-reference")
    for s in sub:
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

def type_transform(d):
    type = []

    if "general-records-types" in d:
        type = arc_group_extraction(d, "general-records-types",
                                    "general-records-type",
                                    "general-records-type-desc")
    if exists(d, "physical-occurrences/physical-occurrence"):
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
            view["@id"] = url[i]
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

def contributor_and_publisher_transform(d, groupKey, itemKey):
    v = {
        "publisher": [],
        "contributor": []
    }

    for cp in iterify(arc_group_extraction(d, groupKey, itemKey)):
        type = cp.get("contributor-type")
        display = cp.get("contributor-display")
        if type == "Publisher":
            v["publisher"].append(display)
        else:
            # If contributor-type is "Most Recent" use only this
            # contributor
            if type == "Most Recent":
                v["contributor"] = display
            elif isinstance(v["contributor"], list):
                v["contributor"].append(display)

    for k in v.keys():
        if not v[k]:
            del v[k]

    return v

def description_transform(d):
    description = arc_group_extraction(d, "general-notes", "general-note")

    return {"description": "; ".join(description)} if description else {}

def identifier_transform(d):
    identifier = [d.get("arc-id")]
    variants = arc_group_extraction(d, "variant-control-numbers",
                                    "variant-control-number")
    for variant in iterify(variants):
        identifier.append(variant.get("variant-type"))
        identifier.append(variant.get("variant-number-desc"))

    identifier = list(set(filter(None, identifier)))

    return {"identifier": identifier} if identifier else {}

def rights_transform(d):
    rights = []
    access = arc_group_extraction(d, "access-restriction", "restriction-status")
    for right in iterify(access):
        if right not in rights:
            rights.append(right)

    return {"rights": rights} if rights else {}

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
    data = []
    group = d.get(groupKey)
    if group:
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
        if not isinstance(subitem, basestring):
            for s in iterify(subitem):
                if nameKey:
                    data.append(s.get(nameKey, None))
                else:
                    data.append(s)
        else:
            data.append(subitem)

    return filter(None, data)

# Structure mapping the original top level property to a function returning a
# single item dict representing the new property and its value
CHO_TRANSFORMER = {
    "physical-occurrences"  : extent_transform,
    "creators"              : creator_transform,
    "hierarchy"             : relation_transform,
    "collection"            : lambda d: {"collection": d.get("collection")},
    "title"                 : lambda d: {"title": d.get("title-only")},
    "languages"             : lambda d: {"language":
                                         arc_group_extraction(d, "languages",
                                                              "language")},
    "contributors"          : lambda d: contributor_and_publisher_transform(
                                                              d,
                                                              "contributors",
                                                              "contributor"
                                                              ),
    "variant-control-numbers": identifier_transform,
    "access-restriction": rights_transform
}

AGGREGATION_TRANSFORMER = {
    "id"                    : lambda d: {"id": d.get("id"),
                                         "@id" : "http://dp.la/api/items/" +
                                                 d.get("id","")},
    "_id"                   : lambda d: {"_id": d.get("_id")},
    "originalRecord"        : lambda d: {"originalRecord":
                                         d.get("originalRecord")},
    "ingestType"            : lambda d: {"ingestType": d.get("ingestType")},
    "ingestDate"            : lambda d: {"ingestDate": d.get("ingestDate")},
    "arc-id-desc"           : is_shown_at_transform,
    "physical-occurrences"  : data_provider_transform,
    "provider"              : lambda d: {"provider": d.get("provider")}
}

@simple_service("POST", "http://purl.org/la/dp/arc-to-dpla", "arc-to-dpla",
                "application/ld+json")
def arctodpla(body,ctype,geoprop=None):
    """   
    Convert output of JSON-ified ARC (NARA) format into the DPLA JSON-LD
    format.

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
    out["sourceResource"].update(subject_and_spatial_transform(data))
    out["sourceResource"].update(date_transform(data))
    out["sourceResource"].update(description_transform(data))
    out.update(has_view_transform(data))
    out["sourceResource"].update(transform_state_located_in(data))

    if exists(data, "objects/object"):
        out.update(transform_thumbnail(data))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
