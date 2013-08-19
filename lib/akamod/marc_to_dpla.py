from akara import logger
from akara import request, response
from akara.services import simple_service
from amara.thirdparty import json
import base64
import re
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

from dplaingestion.selector import getprop as selector_getprop, setprop, exists

def getprop(d, p):
    return selector_getprop(d, p, True)

# Global variable to be used for provider-specific mapping
PROVIDER = None

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

def _as_list(v):
    return v if isinstance(v, (list, tuple)) else [v]

def datafield_type_transform(values):
    types = OrderedDict([
        ("AJ", ("Journal", "Text")),
        ("AN", ("Newspaper", "Text")),
        ("BI", ("Biography", "Text")),
        ("BK", ("Book", "Text")),
        ("CF", ("Computer File", "Interactive Resource")),
        ("CR", ("CDROM", "Interactive Resource")),
        ("CS", ("Software", "Software")),
        ("DI", ("Dictionaries", "Text")),
        ("DR", ("Directories", "Text")),
        ("EN", ("Encyclopedias", "Text")),
        ("HT", ("HathiTrust", None)),
        ("MN", ("Maps-Atlas", "Image")),
        ("MP", ("Map", "Image")),
        ("MS", ("Musical Score", "Text")),
        ("MU", ("Music", "Text")),
        ("MV", ("Archive", "Collection")),
        ("MW", ("Manuscript", "Text")),
        ("MX", ("Mixed Material", "Collection")),
        ("PP", ("Photograph/Pictorial Works", "Image")),
        ("RC", ("Audio CD", "Sound")),
        ("RL", ("Audio LP", "Sound")),
        ("RM", ("Music", "Sound")),
        ("RS", ("Spoken word", "Sound")),
        ("RU", (None, "Sound")),
        ("SE", ("Serial", "Text")),
        ("SX", ("Serial", "Text")),
        ("VB", ("Video (Blu-ray)", "Moving Image")),
        ("VD", ("Video (DVD)", "Moving Image")),
        ("VG", ("Video Games", "Moving Image")),
        ("VH", ("Video (VHS)", "Moving Image")),
        ("VL", ("Motion Picture", "Moving Image")),
        ("VM", ("Visual Material", "Image")),
        ("WM", ("Microform", "Text")),
        ("XC", ("Conference", "Text")),
        ("XS", ("Statistics", "Text"))
    ])

    spec_type = []
    type = []
    for v in values:
        if v in types:
            spec_type.append(types[v][0])
            type.append(types[v][1])

    return {"type": type, "specType": spec_type} if type else {}

def provider_transform(values):
    provider = {}
    if "HT" in values and "avail_ht" in values:
        provider["@id"] = "http://dp.la/api/contributor/hathitrust"
        provider["name"] = "HathiTrust"

    return {"provider": provider} if provider else {}

def dataprovider_transform_hathi(values):
    providers = {
        "bc": "Boston College",
        "chi": "University of Chicago",
        "coo": "Cornell University",
        "dul1": "Duke University",
        "gri": "Getty Research Institute",
        "hvd": "Harvard University",
        "ien": "Northwestern University",
        "inu": "Indiana University",
        "loc": "Library of Congress",
        "mdl": "Minnesota Digital Library",
        "mdp": "University of Michigan",
        "miua": "University of Michigan",
        "miun": "University of Michigan",
        "namespace": "description",
        "nc01": "University of North Carolina",
        "ncs1": "North Carolina State University",
        "njp": "Princeton University",
        "nnc1": "Columbia University",
        "nnc2": "Columbia University",
        "nyp": "New York Public Library",
        "psia": "Penn State University",
        "pst": "Penn State University",
        "pur1": "Purdue University",
        "pur2": "Purdue University",
        "uc1": "University of California",
        "uc2": "University of California",
        "ucm": "Universidad Complutense de Madrid",
        "ufl1": "University of Florida",
        "uiug": "University of Illinois",
        "uiuo": "University of Illinois",
        "umn": "University of Minnesota",
        "usu": "Utah State University Press",
        "uva": "University of Virginia",
        "wu": "University of Wisconsin",
        "yale": "Yale University"
    }

    data_provider = []
    for v in values:
        data_provider.append(providers.get(v.split(".")[0], None))
    data_provider = filter(None, data_provider)

    return {"dataProvider": data_provider} if data_provider else {}

def get_gov_spec_type(control_008_28, datafield_086_or_087):
    if (control_008_28 in ("a", "c", "f", "i", "l", "m", "o", "s") or
        datafield_086_or_087):
        return "Government Document"
    else:
        return None

def format_transform(control_char, leader_char):
    control = {
        "a": "Map",
        "c": "Electronic resource",
        "d": "Globe",
        "f": "Tactile material",
        "g": "Projected graphic",
        "h": "Microform",
        "k": "Nonprojected graphic",
        "m": "Motion picture",
        "o": "Kit",
        "q": "Notated music",
        "r": "Remote-sensing image",
        "s": "Sound recording",
        "t": "Text",
        "v": "Videorecording",
        "z": "Unspecified"
    }

    leader = {
        "a": "Language material",
        "c": "Notated music",
        "d": "Manuscript",
        "e": "Cartographic material",
        "f": "Manuscript cartographic material",
        "g": "Projected medium",
        "i": "Nonmusical sound recording"
    }

    format = []
    format.append(control.get(control_char, None))
    format.append(leader.get(leader_char, None))
    format = filter(None, format)

    return format

def _get_subfields(_dict):
    if "subfield" in _dict:
        for subfield in _as_list(_dict["subfield"]):
            yield subfield
    else:
        return
            
def _get_values(_dict, codes=None):
    """Extracts the appropriate "#text" values from _dict given a string
       of codes. If codes is None, all "#text" values are extracted. If
       codes starts with "!", codes are excluded.
    """
    values = []
    exclude = False

    if codes and codes.startswith("!"):
        exclude = True
        codes = codes[1:]

    for subfield in _get_subfields(_dict):
        if not codes:
            pass
        elif not exclude and ("code" in subfield and subfield["code"] in
                              list(codes)):
            pass
        elif exclude and ("code" in subfield and subfield["code"] not in
                          list(codes)):
            pass
        else:
            continue

        if "#text" in subfield:
            values.append(subfield["#text"])

    return values

def _get_spatial_values(_dict, tag, codes=None):
    """Removes trailing periods for spatial values from subject tags 650 and
       651
    """
    values = _get_values(_dict, codes)
    if tag in ("650", "651"):
        for i in range(len(values)):
            values[i] = re.sub("\.$", "", values[i])

    return values

def _get_contributor_values(_dict, codes=None):
    """Extracts the appropriate "#text" values from _dict for the
       contributor field. If subfield e is "aut" or "cre", returns an
       empty list.
    """
    values = []
    for subfield in _get_subfields(_dict):
        if not codes or ("code" in subfield and subfield["code"] in
                         list(codes)):
            if "#text" in subfield:
                values.append(subfield["#text"])

        # Do not use any subfield values if code is e and #text is aut or
        # cre
        if subfield.get("code") == "e" and (subfield.get("#text") in ("aut",
                                                                      "cre")):
            return []
    
    return values

def _get_subject_values(_dict, tag):
    """Extracts the "#text" values from _dict for the subject field and
       incrementally joins the values by the tag/code dependent delimiter
    """
    def _delimiters(tag, code):
        """Returns the appropriate delimiter(s) based on the tag and code"""
        if tag == "658":
            if code == "b":
                return [":"]
            elif code == "c":
                return [" [", "]"]
            elif code == "d":
                return ["--"]
        elif ((tag == "653") or
              (int(tag) in range(690, 700)) or
              (code == "b" and tag in ("654", "655")) or
              (code in ("v", "x", "y", "z"))):
            return ["--"]
        elif code == "d":
            return [", "]

        return [". "]

    values = []
    for subfield in _get_subfields(_dict):
        code = subfield.get("code", "")
        if not code or code.isdigit():
            # Skip codes that are numeric
            continue

        if "#text" in subfield:
            values.append(subfield["#text"])
            delimiters = _delimiters(tag, code)
            for delim in delimiters:
                values = [delim.join(values)]
                if delim != delimiters[-1]:
                    # Append an empty value for subsequent joins
                    values.append("")

    return values

def _join_sourceresource_values(prop, values):
    """Joins the prop values retrieved from all of a tags codes"""
    join_props = (["subject"], ""), (["isPartOf"], ". "), \
                 (["contributor", "creator", "publisher", "extent",
                   "identifier"], " ")
    for prop_list, delim in join_props:
        if prop in prop_list:
            if delim == ". ":
                # Remove any existing periods at end of values
                for i in range(len(values)):
                    if values[i].endswith(delim[0]):
                        values[i] = values[i][:-1]
                values[-1] += delim[0]
            if values:
                values = [delim.join(values)]
    return values

def all_transform(d, p):
    global PROVIDER
    logger.debug("TRANSFORMING %s" % d["_id"])

    # For spec_type use
    control_008_28 = None
    datafield_086_or_087 = None

    data = {
        "sourceResource": {
            "identifier": [],
            "contributor": [],
            "creator": [],
            "date": [],
            "description": [],
            "extent": [],
            "language": [],
            "spatial": [],
            "publisher": [],
            "isPartOf": [],
            "rights": [],
            "stateLocatedIn": [],
            "subject": [],
            "temporal": [],
            "title": [None, None, None],
            "format": [],
            "type": [],
            "specType": []
        }
    }

    # Mapping dictionaries for use with datafield:
    # Keys are used to check if there is a tag match. If so, the value provides
    # a list of (property, code) tuples. In the case where certain tags have
    # prominence over others, the tuples will be of the form
    # (property, index, code). To exclude a code, prefix it with a "!":
    # [("format", "!cd")] will exclude the "c" and "d" codes (see def
    # _get_values). 
    data_map = {
        lambda t: t == "856":           [("isShownAt", "u")],
        lambda t: t == "973":           [("provider", "ab")],
        lambda t: t == "974":           [("dataProvider", "u")],
        lambda t: t == "852":           [("dataProvider", "a")]
    }
    source_resource_map = {
        lambda t: t in ("020", "022",
                        "035"):         [("identifier", "a")],
        lambda t: t == "050":           [("identifier", "ab")],
        lambda t: t in ("100", "110",
                        "111"):         [("creator", None)],
        lambda t: t == "041":           [("language", "a")],
        lambda t: t == "260":           [("date", "c"), ("publisher", "ab")],
        lambda t: t == "270":           [("stateLocatedIn", "c")],
        lambda t: t == "300":           [("extent", "ac")],
        lambda t: t in ("337", "338"):  [("format", "a")],
        lambda t: t == "340":           [("format", "a"), ("extent", "b")],
        lambda t: t.startswith("5"):    [("description", "a")],
        lambda t: t in ("506", "540"):  [("rights", None)],
        lambda t: t == "648":           [("temporal", None)],
        lambda t: t in ("700", "710",
                        "711", "720"):  [("contributor", None)],
        #lambda t: t == "662":          [("sourceResource/spatial", None)],
        lambda t: t == "240":           [("title", 2, None)],
        lambda t: t == "242":           [("title", 1, None)],
        lambda t: t == "245":           [("title", 0, "!c")],
        lambda t: t == "970":           [("type", "a")],
        lambda t: t == "651":           [("spatial", "a")],
        lambda t: int(t) in set([600, 650, 651] +
                            range(610, 620) +
                            range(653, 659) +
                            range(690, 700)):   [("subject", None),
                                                 ("format", "v"),
                                                 ("temporal", "y"),
                                                 ("spatial", "z")],
        lambda t: (760 <= int(t) <= 787):       [("isPartOf", None)],

    }

    # Handle datafield
    for item in _as_list(getprop(d, p)):
        for _dict in _as_list(item):
            tag = _dict.get("tag", None)
            # Skip cases where there is no tag or where tag == "ERR"
            try:
                int(tag)
            except:
                continue
            # Handle data_map matches
            for match, tuples in data_map.iteritems():
                if match(tag):
                    for tup in tuples:
                        prop, codes = tup
                        values = _get_values(_dict, codes)
                        if prop == "provider":
                            data.update(provider_transform(values))
                        elif prop == "dataProvider":
                            if tag == "974" and PROVIDER == "hathitrust":
                                dp = dataprovider_transform_hathi(values)
                                data.update(dp)
                            elif tag == "852" and PROVIDER == "uiuc":
                                if values:
                                    data["dataProvider"] = values[0]
                        else:
                            if values:
                                data[prop] = values[0]
            # Handle source_resource_map matches
            for match, tuples in source_resource_map.iteritems():
                if match(tag):
                    for tup in tuples:
                        if len(tup) == 2:
                            prop, codes = tup
                            if prop == "contributor":
                                # Handle values for contributor
                                values = _get_contributor_values(_dict, codes)
                            elif prop == "subject":
                                # Handle values for subject
                                values = _get_subject_values(_dict, tag)
                            elif prop == "spatial":
                                # Handle values for spatial
                                values = _get_spatial_values(_dict, tag, codes)
                            else:
                                # Handle values for all other sourceResource
                                # fields
                                values = _get_values(_dict, codes)
                            if prop == "identifier":
                                # Handle identifier labeling
                                label = None
                                if tag == "020":
                                    label = "ISBN:"
                                elif tag == "022":
                                    label = "ISSN:"
                                elif tag == "050":
                                    label = "LC call number:"
                                if label:
                                    # Insert label as first value item as
                                    # values will be joined
                                    values.insert(0, label)
                            values = _join_sourceresource_values(prop, values)
                            if prop == "type":
                                data["sourceResource"].update(
                                    datafield_type_transform(values)
                                )
                            else:
                                data["sourceResource"][prop].extend(values)
                        elif len(tup) == 3:
                            prop, index, codes = tup
                            values = _get_values(_dict, codes)
                            data["sourceResource"][prop][index] = values 
            if tag == "662":
                # Test: Log document with 662 (spatial)
                logger.debug("Document has 662: %s" % d["_id"])
            elif tag == "086" or tag == "087":
                datafield_086_or_087 = True

    # Handle sourceResource/title
    title = filter(None, data["sourceResource"]["title"])
    if title:
        for i in range(len(title)):
            title[i] = " ".join(title[i])
        data["sourceResource"]["title"] = title
    else:
        del data["sourceResource"]["title"]

    # Handle controlfield: values from here are needed to update
    # sourceResource/identifier, sourceResource/language, and
    # sourceResource/format
    format_char_control = None
    format_char_leader = None
    for item in _as_list(getprop(d, "controlfield")):
        if "#text" in item and "tag" in item:
            # Map tag 001 only for Hathi
            if item["tag"] == "001" and PROVIDER == "hathitrust":
                value = "Hathi: " + item["#text"]
                data["sourceResource"]["identifier"].append(value)
            if item["tag"] == "007":
                # For format use
                format_char_control = item["#text"][0]
            if item["tag"] == "008":
                if len(item["#text"]) > 28:
                    # For spec_type use
                    control_008_28 = item["#text"][28]
                if len(item["#text"]) > 37:
                    data["sourceResource"]["language"].append(
                        item["#text"][35:38]
                    )
    leader = getprop(d, "leader")
    if len(leader) > 6:
        format_char_leader = leader[6]

    format_values = format_transform(format_char_control, format_char_leader)
    data["sourceResource"]["format"].extend(format_values)
        
    # Split language
    language = []
    for lang_str in data["sourceResource"]["language"]:
        language.extend([lang_str[i:i+3] for i in range(0, len(lang_str), 3)])
    data["sourceResource"]["language"] = language


    # Add "Government Document" to spec_type if applicable
    gov_spec_type = get_gov_spec_type(control_008_28, datafield_086_or_087)
    if gov_spec_type:
        data["sourceResource"]["specType"].append(gov_spec_type)

    # Remove empty sourceResource values
    del_keys = [key for key in data["sourceResource"] if not
                data["sourceResource"][key]]
    for key in del_keys:
        del data["sourceResource"][key]

    # Handle Hathi isShownAt
    is_shown_at = None
    for id in _as_list(getprop(data, "sourceResource/identifier")):
        if id.startswith("Hathi: "):
            id = id.split("Hathi: ")[-1]
            is_shown_at = "http://catalog.hathitrust.org/Record/%s" % id
            break
    if is_shown_at:
        setprop(data, "isShownAt", is_shown_at)

    return data

def leader_type_transform(d, p):
    types = OrderedDict([
        ("am", ("Book", "Text")),
        ("asn", ("Newspapers", "Text")),
        ("as", ("Serial", "Text")),
        ("aa", ("Book", "Text")),
        ("a(?![mcs])", ("Serial", "Text")),
        ("[cd].*", ("Musical Score", "Text")),
        ("t.*", ("Manuscript", "Text")),
        ("[ef].*", ("Maps", "Image")),
        ("g.[st]", ("Photograph/Pictorial Works", "Image")),
        ("g.[cdfo]", ("Film/Video", "Moving Image")),
        ("g.*", (None, "Image")),
        ("k.*", ("Photograph/Pictorial Works", "Image")),
        ("i.*", ("Nonmusic", "Sound")),
        ("j.*", ("Music", "Sound")),
        ("r.*", (None, "Physical object")),
        ("p[cs].*", (None, "Collection")),
        ("m.*", (None, "Interactive Resource")),
        ("o.*", (None, "Collection")),
    ])

    leader = getprop(d, p)
    control = getprop(d, "controlfield")
    spec_type, type = None, None

    # Get relevant controlfield values
    control_007_01 = ""
    control_008_21 = ""
    control_008_28 = None
    for item in _as_list(control):
        if "#text" in item and "tag" in item:
            if item["tag"] == "007" and len(item["#text"]) > 1:
                control_007_01 = item["#text"][1]
            elif item["tag"] == "008" and len(item["#text"]) > 21:
                control_008_21 = item["#text"][21]
            elif item["tag"] == "008" and len(item["#text"]) > 28:
                control_008_28 = item["#text"][28]

    # Create transform string
    transform_string = leader[6] + leader[7] + control_007_01 + control_008_21

    for str in types:
        if re.match("^{0}".format(str), transform_string):
            spec_type, type = types[str] 
            break

    # Add "Government Document" to spec_type if applicable
    datafield_086_or_087 = None
    for item in _as_list(getprop(d, "datafield")):
        if "tag" in item and (item["tag"] == "086" or item["tag"] == "087"):
            datafield_086_or_087 = True
            break
    gov_spec_type = get_gov_spec_type(control_008_28, datafield_086_or_087)
    if gov_spec_type:
        if spec_type:
            spec_type = [spec_type]
            spec_type.append(gov_spec_type)
        else:
            spec_type = gov_spec_type

    return {"type": type, "specType": spec_type} if type else {}

TRANSFORMER = {
    "id"                : lambda d, p: {"id": d[p],
                                        "@id": "http://dp.la/api/items/" + 
                                               d[p]},
    "_id"               : lambda d, p: {"_id": d[p]},
    "source"            : lambda d, p: {"dataProvider": d[p]},
    "ingestType"        : lambda d, p: {"ingestType": d[p]},
    "ingestDate"        : lambda d, p: {"ingestDate": d[p]},
    "originalRecord"    : lambda d, p: {"originalRecord": d[p]},
    "datafield"         : all_transform
}

CHO_TRANSFORMER = {
    "leader"            : leader_type_transform
}

@simple_service('POST', 'http://purl.org/la/dp/marc_to_dpla', 'marc_to_dpla', 'application/ld+json')
def marc_to_dpla(body, ctype, geoprop=None):
    """
    Convert output of JSON-ified MARC format into the DPLA JSON-LD format.

    Parameter "geoprop" specifies the property name containing lat/long coords
    """

    try :
        data = json.loads(body)
    except Exception:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    global GEOPROP
    global PROVIDER
    GEOPROP = geoprop
    PROVIDER = getprop(data, "_id").split("--")[0]

    out = {
        "@context": CONTEXT,
        "sourceResource": {}
    }

    # The controlfield, datafield, and leader fields are nested within the
    # metadata/record for UIUC.
    if exists(data, "metadata/record"):
        data.update(getprop(data, "metadata/record"))

    # Apply all transformation rules from original document
    for p in TRANSFORMER:
        if exists(data, p):
            out.update(TRANSFORMER[p](data, p))
    # CHO_TRANFORMER must run after TRANSORMER in order for sourceResource/type
    # to be overriden by the leader mapping (see Hathi "Type/TypeSpec" Google
    # spreadsheet).
    for p in CHO_TRANSFORMER:
        if exists(data, p):
            out["sourceResource"].update(CHO_TRANSFORMER[p](data, p))

    # Strip out keys with None/null values?
    out = dict((k, v) for (k, v) in out.items() if v)

    return json.dumps(out)
