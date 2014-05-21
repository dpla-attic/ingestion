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

from dplaingestion.selector import getprop as get_prop, setprop, exists
from dplaingestion.utilities import remove_key_prefix

def getprop(d, p):
    return get_prop(d, p, True)

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

DESC_FREQ = {
    "a": "Annual",
    "b": "Bimonthly",
    "c": "Semiweekly",
    "d": "Daily",
    "e": "Biweekly",
    "f": "Semiannual",
    "g": "Biennial",
    "h": "Triennial",
    "i": "Three times a week",
    "j": "Three times a month",
    "k": "Continuously updated",
    "m": "Monthly",
    "q": "Quarterly",
    "s": "Semimonthly",
    "t": "Three times a year",
    "u": "Unknown",
    "w": "Weekly",
    "z": "Other"
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
            if v[0] not in spec_type:
                spec_type.append(types[v][0])
            if v[1] not in type:
                type.append(types[v][1])

    return {"type": type, "specType": spec_type} if type else {}

def get_gov_spec_type(control_008_28, datafield_086_or_087):
    if (control_008_28 in ("a", "c", "f", "i", "l", "m", "o", "s") or
        datafield_086_or_087):
        return "Government Document"
    else:
        return None

def get_gov_publication(control_008_28):
    if control_008_28 == "f":
        return "Government Publication"

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
    """Removes trailing periods for spatial values."""
    values = [re.sub("\.$", "", v) for v in _get_values(_dict, codes)]

    return values

def _get_contributor_values(_dict):
    """Extracts the appropriate "#text" values from _dict for the
       contributor field. If code "e" is "aut" or "cre", returns an
       empty list.
    """
    values = []
    for subfield in _get_subfields(_dict):
        if ("#text" in subfield and not (subfield.get("code") == "e" and
                                         subfield.get("#text") == "author")):
            values.append(subfield["#text"])

    return values

def _get_creator_values(_dict, tag, codes=None):
    """Extracts the appropriate "#text" values from _dict for the creator
       field. If tag is 700, 710, or 711 we take only the valude "#text" value
       for code "e" if it's value is "author".
    """
    values = []
    for subfield in _get_subfields(_dict):
        if "#text" in subfield:
            if (tag in (700, 710, 711) and not
                (subfield.get("code") == "e" and
                 subfield.get("#text") == "author")):
                continue

            values.append(subfield["#text"])

    return values

def _set_global_date_values(_dict, tag, codes=None):
    global DATE
    if codes:
        values = _get_values(_dict, codes)
    else:
        values = []
        for subfield in _get_subfields(_dict):
            if "#text" in subfield:
                values.append(subfield["#text"])

    DATE[tag].extend(values)

def _set_global_description_values(_dict, tag, codes=None):
    global DESC
    if codes:
        values = _get_values(_dict, codes)
    else:
        values = []
        for subfield in _get_subfields(_dict):
            if "#text" in subfield:
                values.append(subfield["#text"])

    if tag in ("310", "583"):
        DESC[tag].extend(values)
    else:
        DESC["5xx"].extend(values)

def _set_global_spatial_values(_dict, tag, codes=None):
    global SPATIAL
    if codes:
        values = _get_values(_dict, codes)
    else:
        values = []
        for subfield in _get_subfields(_dict):
            if "#text" in subfield:
                values.append(subfield["#text"])

    values = [re.sub("\.$", "", v) for v in values]
    [SPATIAL[tag].append(v) for v in values if v not in SPATIAL[tag]]

def _set_global_isa_values(_dict, tag, codes=None):
    global ISA
    if codes:
        values = _get_values(_dict, codes)
    else:
        values = []
        for subfield in _get_subfields(_dict):
            if "#text" in subfield:
                values.append(subfield["#text"])

    if "u" in codes:
        # Use only first 856u
        if not ISA["856u"]:
            ISA["856u"] = values
    else:
        ISA["856z3"].extend(values)


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
            values.append(subfield["#text"].rstrip(", "))
            delimiters = _delimiters(tag, code)
            for delim in delimiters:
                values = [delim.join(values)]
                if delim != delimiters[-1]:
                    # Append an empty value for subsequent joins
                    values.append("")

    return values

def _join_sourceresource_values(prop, values):
    """Joins the prop values retrieved from all of a tags codes"""
    join_props = (["subject"], ""), (["relation"], ". "), \
                 (["contributor", "creator", "publisher", "extent",
                   "identifier"], " ")
    for prop_list, delim in join_props:
        if prop in prop_list:
            if delim == ". ":
                # Remove any existing periods at end of values, except
                # for last value
                values = [re.sub("\.$", "", v) for v in values]
                values[-1] += "."
            if values:
                values = [delim.join(values)]

    # Remove any double periods (excluding those in ellipsis)
    values = [re.sub("(?<!\.)\.{2}(?!\.)", ".", v) for v in values]

    return values

def all_transform(d, p):
    global CTRL_008
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
            "relation": [],
            "rights": [],
            "subject": [],
            "temporal": [],
            "title": [],
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
    }
    source_resource_map = {
        # GPO
        lambda t: t in ("700", "710", "711"):  [("contributor", None)],
        lambda t: t in ("100", "110", "111",
                        "700", "710", "711"):  [("creator", None)],
        lambda t: t in ("260", "264"):         [("date", "c"),
                                                ("publisher", "ab")],
        lambda t: t == "362":                  [("date", None)],
        lambda t: t == "300":                  [("extent", "a")],
        lambda t: t in ("001", "020", "022"):  [("identifier", None)],
        lambda t: t in ("035", "050", "074",
                        "082", "086"):         [("identifier", "a")],
        lambda t: t == "506":                  [("rights", None)],
        lambda t: t in ("600", "610", "611",
                        "630", "650", "651"):  [("subject", None)],
        lambda t: t in ("600", "610", "650",
                        "651"):                [("temporal", "y")],
        lambda t: t == "611":                  [("temporal", "d")],
        lambda t: t in ("255", "310"):         [("description", None)],
        lambda t: t == "583":                  [("description", "z")],
        lambda t: int(t) in (range(500, 538) +
                             range(539, 583) +
                             range(584, 600)): [("description", None)],
        lambda t: t in ("337", "338", "340"):  [("format", "a")],
        lambda t: t in ("041", "546"):         [("language", None)],
        lambda t: t == "650":                  [("spatial", "z")],
        lambda t: t == "651":                  [("spatial", "a")],
        lambda t: int(t) in (range(760, 787) +
                             ["490", "730",
                              "740", "830"]):  [("relation", None)],
        lambda t: t == "337":                  [("type", "a")],
        lambda t: t == "655":                  [("type", None)],
        lambda t: t == "245":                  [("title", None)],
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
                        if prop == "isShownAt":
                            _set_global_isa_values(_dict, tag, codes)
                        values = _get_values(_dict, codes)
                        if values:
                            pass
            # Handle source_resource_map matches
            for match, tuples in source_resource_map.iteritems():
                if match(tag):
                    for tup in tuples:
                        if len(tup) == 2:
                            values = None
                            prop, codes = tup
                            if prop == "contributor":
                                values = _get_contributor_values(_dict)
                            elif prop == "creator":
                                values = _get_creator_values(_dict, tag,
                                                              codes)
                            elif prop == "date":
                                _set_global_date_values(_dict, tag, codes)
                            elif prop == "subject":
                                values = _get_subject_values(_dict, tag)
                            elif prop == "description":
                                _set_global_description_values(_dict, tag,
                                                               codes)
                            elif prop == "spatial":
                                _set_global_spatial_values(_dict, tag, codes)
                            else:
                                # Handle values for all other sourceResource
                                # fields
                                values = _get_values(_dict, codes)
                            if values:
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
                                values = _join_sourceresource_values(prop,
                                                                     values)
                                if prop == "type":
                                    data["sourceResource"].update(
                                        datafield_type_transform(values)
                                    )
                                else:
                                    data["sourceResource"][prop].extend(values)
                        elif len(tup) == 3:
                            prop, index, codes = tup
                            values = _get_values(_dict, codes)
                            if values:
                                s = data["sourceResource"][prop][index]
                                if s:
                                    # There may be multiple tag values (ie, two
                                    # 245a values), so we join them there.
                                    s.extend(values)
                                    # Remove trailing period from first value
                                    s[0] = re.sub("\.$", "", s[0].strip())
                                    values = ["; ".join(s)]
                                data["sourceResource"][prop][index] = values 
            if tag == "662":
                # Test: Log document with 662 (spatial)
                logger.debug("Document has 662: %s" % d["_id"])
            elif tag == "086" or tag == "087":
                datafield_086_or_087 = True

    # Handle controlfield: values from here are needed to update
    # sourceResource/identifier, sourceResource/language, and
    # sourceResource/format, and to set isShownAt
    format_char_control = None
    format_char_leader = None
    isa_uri = "http://catalog.gpo.gov/F/?func=direct&doc_number=%s&format=999"
    for item in _as_list(getprop(d, "controlfield")):
        if "#text" in item and "tag" in item:
            if item["tag"] == "001":
                data["isShownAt"] = isa_uri % item["#text"]
            if item["tag"] == "007":
                # For format use
                format_char_control = item["#text"][0]
            if item["tag"] == "008":
                CTRL_OO8 = item["#text"]
                if len(item["#text"]) > 28:
                    # For spec_type use
                    control_008_28 = item["#text"][28]
                if len(item["#text"]) > 37:
                    # If language not set from tag 041, set it here
                    if not data["sourceResource"]["language"]:
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
    gov_publication = get_gov_publication(control_008_28)
    if spec_type:
        spec_type = [spec_type]
    else:
        spec_type = []
    if gov_spec_type:
        spec_type.append(gov_spec_type)
    if gov_publication:
        spec_type.append(gov_publication)

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
    "provider"          : lambda d, p: {"provider": d.get("provider", None)},
    "datafield"         : all_transform
}

CHO_TRANSFORMER = {
    "leader"            : leader_type_transform
}

@simple_service('POST', 'http://purl.org/la/dp/marc_to_dpla_gpo',
                 'marc_to_dpla_gpo', 'application/ld+json')
def marc_to_dpla_gpo(body, ctype, geoprop=None):
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

    out = {
        "@context": CONTEXT,
        "sourceResource": {}
    }

    # The controlfield, datafield, and leader fields are nested within the
    # metadata/record for GPO.
    data = remove_key_prefix(data, "marc:")
    if exists(data, "metadata/record"):
        data.update(getprop(data, "metadata/record"))

    # Set global variables
    global LDR
    global DATE
    global DESC
    global SPATIAL
    global ISA
    global CTRL_008
    LDR = getprop(data, "leader")
    DATE = {
        "260": [],
        "264": [],
        "362": []
    }
    DESC = {
        "310": [],
        "5xx": [],
        "583": []
    }
    SPATIAL = {
        "650": [],
        "651": [],
        "034": [],
        "255": []
    }
    ISA = {
        "856u": [],
        "856z3": []
    }

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

    # Handle date values
    date = None
    if DATE["362"]:
        date = DATE["362"]
    elif LDR[7] == "m":
        if DATE["260"]:
            date = DATE["260"]
        elif DATE["264"]:
            date = DATE["264"]
    if date:
        out["sourceResource"].update({"date": date})

    # Handle description values
    if not DESC["310"] and LDR[7] == "s":
        try:
            description["frequency"] = DESC_FREQ.get(CTRL_008[18])
        except:
            logger.error("CTRL_008 error for _id: %s" % data["_id"])
    description = [v for values in DESC.values() for v in values if v]
    if description:
        out["sourceResource"].update({"description": description})
    
    # Handle spatial values
    if LDR[6] == "e":
        if SPATIAL["034"]:
            SPATIAL["255"] = []
    else:
        SPATIAL["034"] = []
        SPATIAL["255"] = []
    spatial = [v for values in SPATIAL.values() for v in values if v]
    if spatial:
        out["sourceResource"].update({"spatial": spatial})
 
    # Handle isShownAt values
    isa = [v for values in ISA.values() for v in values if v]
    if isa:
        out.update({"isShownAt": isa})

    # Handle title values
    if out["sourceResource"]["title"]:
        title = " ".join(out["sourceResource"]["title"])
        out["sourceResource"]["title"] = title

    # Handle rights
    if not out["sourceResource"]["rights"]:
        rights = "Pursuant to Title 17 Section 105 of the United States " + \
                 "Code, this file is not subject to copyright protection " + \
                 "and is in the public domain. For more information " + \
                 "please see http://www.gpo.gov/help/index.html#" + \
                 "public_domain_copyright_notice.htm"
        out["sourceResource"]["rights"] = rights

    return json.dumps(out)
