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


def transform_description(d):
    description = None
    items = arc_group_extraction(d, "freetext", "notes")
    for item in (items if isinstance(items, list) else [items]):
        if "@label" in item and item["@label"] == "Description":
            if "#text" in item:
                description = item["#text"]
                break;
    return {"description": description} if description else {}


def transform_date(d):
    date = []
    dates = arc_group_extraction(d, "freetext", "date")
    for item in dates:
        if "@label" in item and "#text" in item:
            date.append(item["#text"])
            
    return {"temporal": date} if date else {}

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
            is_part_of.append({"name": item["#text"]})

    res = is_part_of
    if len(res) == 1:
        res = res[0]
    return {"isPartOf": res} if res else {}


def source_transform(d):
    source = None
    for s in d["handle"]:
        if is_absolute(s):
            source = s
            break
    return {"source": source} if source else {}


def transform_is_shown_at(d):
    #propname = "descriptiveNonRepeating/online_media/media/#text"
    tmpl="http://collections.si.edu/search/results.htm?q=record_ID%%3A%s&repo=DPLA"
    propname = "descriptiveNonRepeating/record_ID"
    
    obj = getprop(d, propname, True)
    return {"isShownAt": tmpl % obj} if obj else {}


def transform_object(d):
    propname = "descriptiveNonRepeating/online_media/media"

    obj = getprop(d, propname, True)

    if not obj:
        return {}

    item = obj
    if isinstance(obj, list):
        item = obj[0]

    return {"object": item["@thumbnail"]} if "@thumbnail" in item else {}


def collection_transform(d):
    import re
    collections = []
    items = arc_group_extraction(d, "freetext", "setName")
    for item in (items if isinstance(items, list) else [items]):
        if "#text" in item:
            c = item["#text"]
            c = re.sub(r'[,]', '', c)
            c = re.sub(r'\s+', '--', c)
            collections.append(c)
    return {"collection": collections} if collections else {}


def creator_transform(d):
    creator = []
    creators = arc_group_extraction(d, "freetext", "name")
    for c in (creators if isinstance(creators, list) else [creators]):
        if c["@label"] in creator_field_names:
            if "#text" in c:
                creator.append(c["#text"])

    res = creator
    if len(creator) == 1:
        res = res[0]

    return {"creator": res} if res else {}


def transform_format(d):
    f = []
    labels = ["Physical description", "Physical description", "Medium"]
    formats = arc_group_extraction(d, "freetext", "physicalDescription")
    [f.append(e["#text"]) for e in formats if e["@label"] in labels]

    res = f
    if len(res) == 1:
        res = res[0]
    return {"format": res} if res else {}


def transform_rights(d):
    p = []
    ps = arc_group_extraction(d, "freetext", "creditLine")
    if ps and ps != [None]:
        [p.append(e["#text"]) for e in ps if "@label" in e and "#text" in e and e["@label"] == "Credit Line"]

    ps = arc_group_extraction(d, "freetext", "objectRights")
    if ps and ps != [None]:
        [p.append(e["#text"]) for e in ps if "@label" in e and "#text" in e and e["@label"] == "Rights"]

    res = p
    if len(p) == 1:
        res = p[0]

    return {"rights": res} if res else {}


def transform_publisher(d):
    p = []
    ps = arc_group_extraction(d, "freetext", "publisher")
    if ps:
        [p.append(e["#text"]) for e in ps if "@label" in e and e["@label"] == "Publisher"]

    return {"publisher": p} if p else {}


def transform_spatial(d):
    result = []
    place = []
    location_states = []

    places = arc_group_extraction(d, "freetext", "place")
    if places:
        for p in places:
            if isinstance(p, dict):
                if "#text" in p:
                    place.append(p["#text"])

    if len(place) == 1:
        place = place[0]


    def convert_location(location, name):
        """Converts one location to a spatial record."""
        city_keys    = ["City", "Town"]
        state_keys   = ["State", "Province", "Department", "Country", "District", "Republic", "Sea", "Gulf", "Bay"]
        county_keys  = ["County", "Island"]
        country_L1_keys = ["Continent", "Ocean"]
        country_L2_keys = ["Country", "Nation", "Sea", "Gulf", "Bay", "Sound"]
        cities    = []
        states    = []
        counties  = []
        countries = []
        regions   = []
        points    = []
        
        res = {}
        def update(res, name, val):
            if not val:
                return
            if len(val) == 1:
                res.update({name: val[0]})
            elif val:
                res.update({name: val})
        
        for k, v in location.items():
            if not ("#text" in v and "@type" in v):
                continue
            tp = v["@type"]
            tx = v["#text"]

            if k == "L5" and tp in city_keys:
                cities.append(tx)
            elif k == "L3" and tp in state_keys:
                states.append(tx)
                location_states.append(tx)
            elif k == "L4" and tp in county_keys:
                counties.append(tx)
            #elif k == "L1" and tp in country_L1_keys:
            #    countries.append(tx)
            elif k == "L2" and tp in country_L2_keys:
                countries.append(tx)
            elif k in [ "L2", "L3", "L4", "L5"]:
                regions.append(tx)
            elif k == "points":
                points.append(v)


        update(res, "name", place)
        update(res, "city", cities)
        update(res, "state", states)
        update(res, "county", counties)
        update(res, "country", countries)
        update(res, "region", regions)
        update(res, "lat_long", points)
        
        return res

    geo = arc_group_extraction(d, "indexedStructured", "geoLocation")

    if geo:
        for g in geo:
            
            if not g:
                continue

            loc = convert_location(g, place)
            if loc:
                result.append(loc)


    
    ret = {}
    if len(result) == 1:
        ret = {"spatial": result[0]}
    elif result:
        ret = {"spatial": result}

    # Also add currentLocation
    l = list(set(location_states))
    if len(l) == 1:
        l = l[0]

    if l:
        ret.update({"currentLocation": l}) 

    return ret


def transform_online_media(d):


    media = arc_group_extraction(d, "descriptiveNonRepeating", "online_media")
    if media == [None]:
        return {}
    
    media = media[0]
    c = 0
    if "@mediaCount" in media:
        c = media["@mediaCount"]
    try:
        c = int(c)
    except ValueError as e:
        logger.error("Couldn't convert %s to int" % c)
        return {}
    if not "media" in media:
        return {}

    m = media
    if c == 1:
        m = [media["media"]]

    res = []
    for mm in m:
        item = {}
        if "@type" in mm:
            item["format"] = mm["@type"]
        if "rights" in mm:
            item["rights"] = mm["rights"]
        if item.keys():
            res.append(item)

    if len(res) == 1:
        return {"hasView": res[0]}
    if len(res) > 1:
        return {"hasView": res}
    return {}

def transform_title(d):
    p = None
    labels = ["title", "Title", "Object Name"]
    ps = arc_group_extraction(d, "descriptiveNonRepeating", "title")
    if ps != [None]:
        for e in ps:
            if e["@label"] in labels:
                p = e["#text"]
    
    return {"title": p} if p else {}

def transform_subject(d):

    p = []
    topic_labels = ["Topic", "subject", "event"]
    ps = arc_group_extraction(d, "freetext", "topic")
    if ps and ps != [None]:
        [p.append(e["#text"]) for e in ps if e["@label"] in topic_labels]

    ps = arc_group_extraction(d, "freetext", "culture")
    if ps and ps != [None]:
        [p.append(e["#text"]) for e in ps if e["@label"] == "Nationality"]

    fields = ["topic","name","culture","tax_kingdom","tax_phylum",
              "tax_division","tax_class","tax_order","tax_family",
              "tax_sub-family","scientific_name","common_name","strat_group",
              "strat_formation","strat_member"]
    if "freetext" in d:
        for key, item in d["freetext"].items():
            if key in fields:
                if "#text" in item:
                    p.append(item["#text"])

    res = list(set(p))
    if len(res) == 1:
        res = res[0]

    return {"subject": res} if res else {}


def transform_identifier(d):
    identifier = []
    ids = arc_group_extraction(d, "freetext", "identifier")
    [identifier.append(e["#text"]) for e in ids if e["@label"].startswith("Catalog") or e["@label"].startswith("Accession")]

    id = identifier
    if len(identifier) == 1:
        id = identifier[0]

    return {"identifier": id} if id else {}


def transform_data_provider(d):
    ds = None
    dss = arc_group_extraction(d, "descriptiveNonRepeating", "data_source")
    if dss != [None]:
        ds = dss[0]

    return {"dataProvider": ds} if ds else {}


def extent_transform(d):
    extent = []
    extents = arc_group_extraction(d, "freetext", "physicalDescription")
    [extent.append(e["#text"]) for e in extents if e["@label"] == "Dimensions"]

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
            spatial.append(s)
   
    if subject:
        v["subject"] = subject
    if spatial:
        v["spatial"] = []
        # Remove duplicate values
        [v["spatial"].append(s) for s in spatial if not v["spatial"].count(s)]
    
    return v


def slugify_field(data, fieldname):
    if exists(data, fieldname):
        import re
        p = getprop(data, fieldname)
        parts = p.split("/")
        c = parts[-1:]
        if c:
            c = c[0]
            c = re.sub(r'[,]', '', c)
            c = re.sub(r'\s+', '--', c)
            parts[len(parts)-1] = c
            slugged = "/".join(parts)
            setprop(data, fieldname, slugged)


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


def transform_collection(d):
    name = arc_group_extraction(d, "freetext", "SetName")

    if not name:
        return

    if "#text" not in name:
        return

    value = name["#text"]

    collection = arc_group_extraction(d, "collection")
    collection["title"] = value

    return collection


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
    "freetext/date"                 : transform_date,
    "freetext/notes"                : transform_description,
    "freetext/identifier"           : transform_identifier,
    "language"                      : lambda d: {"language": d.get("language") },
    "freetext/physicalDescription"  : transform_format,
    "freetext/publisher"            : transform_publisher,
    "descriptiveNonRepeating/title" : transform_title,
    #"descriptiveNonRepeating/data_source" : transform_data_provider,
    #"descriptiveNonRepeating/online_media" : transform_online_media,
    "collection"            : lambda d: {"collection": d.get("collection")}
}

AGGREGATION_TRANSFORMER = {
    "id"                    : lambda d: {"id": d.get("id"), "@id" : "http://dp.la/api/items/"+d.get("id","")},
    "_id"                   : lambda d: {"_id": d.get("_id")},
    "originalRecord"        : lambda d: {"originalRecord": d.get("originalRecord",None)},
    "ingestType"            : lambda d: {"ingestType": d.get("ingestType")},
    "ingestDate"            : lambda d: {"ingestDate": d.get("ingestDate")},
}

@simple_service("POST", "http://purl.org/la/dp/edan_to_dpla", "edan_to_dpla", "application/ld+json")
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
        "sourceResource": {}
    }

    # Apply all transformation rules from original document
    for k, v in CHO_TRANSFORMER.items():
        if exists(data, k):
            out["sourceResource"].update(v(data))
    for k, v in AGGREGATION_TRANSFORMER.items():
        if exists(data, k):
            out.update(v(data))

    # Apply transformations that are dependent on more than one
    # original document  field
    #out["sourceResource"].update(type_transform(data))
    out["sourceResource"].update(transform_rights(data))
    out["sourceResource"].update(transform_subject(data))
    out["sourceResource"].update(transform_spatial(data))

    out.update(transform_is_shown_at(data))
    out.update(transform_object(data))
    out.update(transform_data_provider(data))

    slugify_field(out, "sourceResource/collection/@id")

    if exists(out, "sourceResource/isPartOf/name"):
        out["sourceResource"]["collection"]["title"] = out["sourceResource"]["isPartOf"]["name"]

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
