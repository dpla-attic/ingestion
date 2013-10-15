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
from dplaingestion.utilities import remove_key_prefix, iterify

GEOPROP = None
META = "metadata/metadata/"

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

RIGHTS_TERM_LABEL = {
    "by": "Attribution.",
    "by-nc": "Attribution Noncommercial.",
    "by-nc-nd": "Attribution Non-commercial No Derivatives.",
    "by-nc-sa": "Attribution Noncommercial Share Alike.",
    "by-nd": "Attribution No Derivatives.",
    "by-sa": "Attribution Share Alike.",
    "copyright": "Copyright.",
    "pd": "Public Domain."
}

DATAPROVIDER_TERM_LABEL = {
    "ABB": "Bryan Wildenthal Memorial Library (Archives of the Big Bend)",
    "ABPL": "Abilene Public Library",
    "ACGS": "Anderson County Genealogical Society",
    "ACHC": "Anderson County Historical Commission",
    "ACRM": "Amon Carter Museum",
    "ACTUMT": "Archives of the Central Texas Conference United Methodist Church",
    "ACUL": "Abilene Christian University Library",
    "APL": "Alvord Public Library",
    "ASPL": "Austin History Center, Austin Public Library",
    "ASTC": "Austin College",
    "BCHC": "Bosque County Historical Commission",
    "BDPL": "Boyce Ditto Public Library",
    "BPL": "Lena Armstrong Public Library",
    "CCGS": "Collin County Genealogical Society",
    "CCHM": "North Texas History Center",
    "CCHS": "Clay County Historical Society",
    "CCMH": "Childress County Heritage Museum",
    "CCMS": "Corpus Christi Museum of Science and History",
    "CHM": "Clark Hotel Museum",
    "CHOS": "Courthouse-on-the-Square Museum",
    "CTRM": "Cattle Raisers Museum",
    "CUA": "Concordia University at Austin",
    "CWTC": "Cowtown Coliseum",
    "DAPL": "Dallas Public Library",
    "DHS": "Dallas Historical Society",
    "DHVG": "Dallas Heritage Village",
    "DPL": "Denton Public Library",
    "DSCL": "Deaf Smith County Library",
    "EPL": "Euless Public Library",
    "ETGS": "East Texas Genealogical Society",
    "FBCM": "FBC Heritage Museum",
    "FBM": "Fort Bend Museum",
    "FCPA": "First Christian Church of Port Arthur",
    "FPL": "Ferris Public Library",
    "FWJA": "Fort Worth Jewish Archives",
    "FWPL": "Fort Worth Public Library",
    "GLO": "Texas General Land Office",
    "GMHP": "Genevieve Miller Hitchcock Public Library",
    "GR": "George Ranch Historical Park",
    "HCGS": "Hutchinson County Genealogical Society",
    "HHSM": "Heritage House Museum",
    "HPUL": "Howard Payne University Library",
    "HSUL": "Hardin-Simmons University Library",
    "IPL": "Irving Archives",
    "JFRM": "Jacob Fontaine Religious Museum",
    "KCT": "Killeen City Library System",
    "KHSY": "Kemah Historical Society",
    "KUMC": "Krum United Methodist Church",
    "LCVG": "Log Cabin Village",
    "LDMA": "Lockheed Martin Aeronautics Company, Fort Worth",
    "LPL": "Laredo Public Library",
    "MARD": "Museum of the American Railroad",
    "MFAH": "The Museum of Fine Arts, Houston",
    "MMMM": "Medicine Mound Museum",
    "MMPL": "Moore Memorial Public Library",
    "MMUL": "McMurry University Library",
    "MPLI": "Marshall Public Library",
    "MRPL": "Marfa Public Library",
    "OKHS": "Oklahoma Historical Society",
    "OSAGC": "Old Settler's Association of Grayson County",
    "PANAM": "The University of Texas-Pan American",
    "PCBG": "Private Collection of Bouncer Goin",
    "PCCRD": "Private Collection of Charles R. Delphenis",
    "PCCRS": "Private Collection of Caroline R. Scrivner Richards",
    "PCHBM": "Private Collection of Howard and Brenda McClurkin",
    "PCJEH": "Private Collection of Joe E. Haynes",
    "PPL": "Palestine Public Library",
    "RPL": "Richardson Public Library",
    "RSMT": "Rose Marine Theatre",
    "SJMH": "San Jacinto Museum of History",
    "SMU": "Southern Methodist University Libraries",
    "SRPL": "Sanger Public Library",
    "STAR": "Star of the Republic Museum",
    "SSPL": "Sulphur Springs Public Library",
    "TCC": "Tarrant County College NE, Heritage Room",
    "TCU": "Texas Christian University",
    "TSGS": "Texas State Genealogical Society",
    "TSLAC": "Texas State Library and Archives Commission",
    "TWU": "Texas Woman's University",
    "TXLU": "Texas Lutheran University",
    "UH": "University of Houston Libraries' Special Collections",
    "UNT": "UNT Libraries",
    "UT": "University of Texas",
    "UTA": "University of Texas at Arlington Library",
    "UTSW": "UT Southwestern Medical Center Library",
    "VCHC": "Val Verde County Historical Commission",
    "WCHM": "Wolf Creek Heritage Museum",
    "WEAC": "Weatherford College",
    "WEBM": "Weslaco Museum",
    "OTHER": "Other",
    "RICE": "Rice University Woodson Research Center",
    "HCDC": "Henderson County District Clerk's Office",
    "ORMM": "The Old Red Museum",
    "SFMDP": "The Sixth Floor Museum at Dealey Plaza",
    "CCTX": "City of Clarendon",
    "PCEBF": "The Private Collection of the Ellis and Blanton Families",
    "DCCCD": "Dallas County Community College District",
    "THF": "Texas Historical Foundation",
    "SWCL": "Swisher County Library",
    "WYU": "Wiley College",
    "LBJSM": "LBJ Museum of San Marcos",
    "DSMA": "Dallas Municipal Archives",
    "FRLM": "French Legation Museum ",
    "PCSF": "The Private Collection of the Sutherlin Family",
    "ARPL": "Arlington Public Library and Fielder House",
    "BEHC": "Bee County Historical Commission",
    "CGHPC": "City of Granbury Historic Preservation Commission",
    "ELPL": "El Paso Public Library ",
    "GPHO": "Grand Prairie Historical Organization",
    "MLCC": "Matthews Family and Lambshead Ranch",
    "NELC": "Northeast Lakeview College",
    "PAPL": "Port Arthur Public Library",
    "PBPM": "Permian Basin Petroleum Museum, Library and Hall of Fame",
    "RVPM": "River Valley Pioneer Museum",
    "SFASF": "Stephen F. Austin Assn. dba Friends of the San Felipe State Historic Site",
    "UTSA": "University of Texas at San Antonio",
    "VCUH": "Victoria College/University of Houston-Victoria Library",
    "HCLY": "Hemphill County Library",
    "BACHS": "Bartlett Activities Center and the Historical Society of Bartlett",
    "CAH": "The Dolph Briscoe Center for American History ",
    "UNTA": "UNT Archives",
    "UNTRB": "UNT Libraries Rare Book and Texana Collections",
    "UNTCVA": "UNT College of Visual Arts + Design",
    "UNTDP": "UNT Libraries Digital Projects Unit",
    "UNTGD": "UNT Libraries Government Documents Department",
    "UNTML": "UNT Music Library",
    "UNTP": "UNT Press",
    "UNTLML": "UNT Media Library",
    "UNTCOI": "UNT College of Information",
    "BRPL": "Breckenridge Public Library",
    "STWCL": "Stonewall County Library",
    "NPSL": "Nicholas P. Sims Library",
    "PCJB": "Private Collection of Jim Bell",
    "MQPL": "Mesquite Public Library",
    "BWPL": "Bell/Whittington Public Library",
    "CHMH": "Cedar Hill Museum of History",
    "CLHS": "Cleveland Historic Society",
    "CKCL": "Cooke County Library",
    "DFFM": "Dallas Firefighters Museum",
    "FSML": "Friench Simpson Memorial Library",
    "HSCA": "Harris County Archives",
    "HTPL": "Haslet Public Library",
    "LVPL": "Longview Public Library",
    "MWSU": "Midwestern State University",
    "STPC": "St. Philips College",
    "UTHSC": "University of Texas Health Science Center Libraries",
    "WCHS": "Wilson County Historical Society",
    "TSHA": "Texas State Historical Association",
    "MCMPL": "McAllen Public Library",
    "UNTLTC": "UNT Linguistics and Technical Communication Department",
    "PCMB": "Private Collection of Melvin E. Brewer",
    "SGML": "Singletary Memorial Library",
    "URCM": "University Relations, Communications & Marketing  department for UNT",
    "TXDTR": "Texas Department of Transportation",
    "TYPL": "Taylor Public Library",
    "WILLM": "The Williamson Museum",
    "ATPS": "Austin Presbyterian Theological Seminary",
    "BUCHC": "Burnet County Historical Commission",
    "DHPS": "Danish Heritage Preservation Society",
    "GCHS": "Gillespie County Historical Society",
    "HMRC": "Houston Metropolitan Research Center at Houston Public Library",
    "ITC": "University of Texas at San Antonio Libraries Special Collections",
    "DISCO": "Digital Scholarship Cooperative (DiSCo)",
    "MAMU": "Mexic-Arte Museum",
    "MMLUT": "Moody Medical Library, UT",
    "MGC": "Museum of the Gulf Coast",
    "NML": "Nesbitt Memorial Library",
    "PAC": "Panola College ",
    "PJFC": "Price Johnson Family Collection",
    "SAPL": "San Antonio Public Library",
    "AMSC": "Anne and Mike Stewart Collection",
    "TSU": "Tarleton State University",
    "STPRB": "Texas State Preservation Board",
    "UNTCAS": "UNT College of Arts and Sciences",
    "UNTCOE": "UNT College of Engineering",
    "UNTCPA": "UNT College of Public Affairs and Community Service",
    "STXCL": "South Texas College of Law",
    "CPL": "Carrollton Public Library",
    "CWCM": "Collingsworth County Museum",
    "PCMC": "Private Collection of Mike Cochran",
    "NMPW": "National Museum of the Pacific War/Admiral Nimitz Foundation",
    "SRH": "Sam Rayburn House Museum",
    "TCFA": "Talkington Clement Family Archives",
    "WTM": "Witte Museum",
    "UNTCED": "UNT College of Education",
    "BECA": "Beth-El Congregation Archives",
    "UNTCEDR": "UNT Center for Economic Development and Research",
    "DMA": "Dallas Museum of Art",
    "UTMDAC": "University of  Texas MD Anderson Center",
    "UNTSMHM": "UNT College of Merchandising, Hospitality and Tourism",
    "UTEP": "University of Texas at El Paso",
    "UNTHSC": "UNT Health Science Center",
    "PPHM": "Panhandle-Plains Historical Museum",
    "AMPL": "Amarillo Public Library",
    "FWHC": "The History Center",
    "EFNHM": "Elm Fork Natural Heritage Museum",
    "UNTOHP": "UNT Oral History Program",
    "UNTCOB": "UNT College of Business ",
    "HCLB": "Hutchinson County Library, Borger Branch",
    "HPWML": "Harrie P. Woodson Memorial Library",
    "CTLS": "Central Texas Library System",
    "ARMCM": "Armstrong County  Museum",
    "CHRK": "Cherokeean Herald",
    "DGS": "Dallas Genealogical Society",
    "UNTCOM": "UNT College of Music ",
    "MBIGB": "Museum of the Big Bend",
    "SCPL": "Schulenburg Public Library",
    "UNTCEP": "UNT Center For Environmental Philosophy",
    "UDAL": "University of Dallas",
    "PVAMU": "Prairie View A&M University ",
    "TWSU": "Texas Wesleyan University",
    "RGPL": "Rio Grande City Public Library",
    "UNTIAS": "UNT Institute of Applied Sciences",
    "UNTGSJ": "UNT Frank W. and Sue Mayborn School of Journalism",
    "BSTPL": "Bastrop Public Library",
    "SHML": "Stella Hill Memorial Library",
    "CAL": "Canyon Area Library",
    "MWHA": "Mineral Wells Heritage Association",
    "TAEA": "Texas Art Education Association",
    "EPCHS": "El Paso County Historical Society  ",
    "CPPL": "Cross Plains Public Library",
    "LCHHL": "League City Helen Hall Library",
    "NCWM": "National Cowboy and Western Heritage Museum",
    "SWATER": "Sweetwater/Nolan County City-County Library",
    "UNTHON": "UNT Honors College",
    "PCJW": "Private Collection of Judy Wood and Jim Atkinson",
    "CRPL": "Crosby County Public Library",
    "DPKR": "City of Denton Parks and Recreation",
    "THC": "Texas Historical Commission",
    "BSAM": "Boy Scouts of America National Scouting Museum",
    "PCCW": "Private Collection of Carolyn West",
    "OCHS": "Orange County Historical Society",
    "DISD": "Denton Independent School District",
    "MINML": "Mineola Memorial Library",
    "CASML": "Casey Memorial Library",
    "UNTD": "UNT Dallas",
    "PTBW": "Private Collection of T. Bradford Willis",
    "UNTG": "University of North Texas Galleries",
    "SCHU": "Schreiner University",
    "TYHL": "Tyrrell Historical Library ",
    "TCAF": "Texas Chapter of the American Fisheries Society",
    "GIBBS": "Gibbs Memorial Library",
    "ATLANT": "Atlanta Public Library",
    "CCS": "City of College Station",
    "GCFV": "Grayson County Frontier Village",
    "PCTF": "Private Collection of the Tarver Family",
    "TAMS": "Texas Academy of Mathematics and Science",
    "CCHC": "Cherokee County Historical Commission",
    "PCBARTH": "Private Collection of Marie Bartholomew",
    "CCPL": "Corpus Christi Public Library",
    "UNTDCL": "UNT Dallas College of Law",
    "LAMAR": "Lamar State College - Orange",
    "SDEC": "St. David's Episcopal Church",
    "TDCD": "Travis County District Clerk's Office"
}

def rights_transform(d, p):
    rights = None
    license = None
    statement = None
    for s in iterify(getprop(d, p)):
        try:
            qualifier = s.get("qualifier")
            text = s.get("#text")
        except:
            continue
        
        if qualifier == "license":
             try:
                 license = "License: " + RIGHTS_TERM_LABEL[text]
             except:
                 logger.error("Term %s not in RIGHTS_TERM_LABEL for %s" %
                              (text, d["_id"]))
        elif qualifier == "statement":
            statement = text

    rights = "; ".join(filter(None, [rights, statement]))

    return {"rights": rights} if rights else {}

def identifier_transform(d, p):
    identifier = []
    for s in iterify(getprop(d, p)):
        try:
            qualifier = s.get("qualifier")
            text = s.get("#text")
        except:
            continue
        if qualifier == "license":
            identifier.append("%s: %s" % (qualifier, text))

    # Add rights values as well
    rights = getprop(d, META + "rights", True)
    if rights is not None:
        for s in iterify(rights):
            try:
                qualifier = s.get("qualifier")
                text = s.get("#text")
            except:
                continue
            if qualifier == "statement":
                identifier.append("%s: %s" % (qualifier, text))

    return {"identifier": identifier} if identifier else {}

def spatial_transform(d, p):
    spatial = []
    for s in iterify(getprop(d, p)):
        if "qualifier" in s and s["qualifier"] in ["placeName", "placePoint",
                                                   "placeBox"]:
            spatial.append(s.get("#text"))

    return {"spatial": spatial} if spatial else {}

def publisher_transform(d, p):
    publisher = []
    for s in iterify(getprop(d, p)):
        if "location" in s and "name" in s:
            publisher.append("%s: %s" % (s["location"].strip(),
                                         s["name"].strip()))

    return {"publisher": publisher} if publisher else {}

def description_transform(d, p):
    description = []
    for s in iterify(getprop(d, p)):
        if isinstance(s, basestring):
            description.append(s)
        else:
            description.append(s.get("#text"))
    description = filter(None, description)

    return {"description": description} if description else {}

def creator_transform(d, p):
    creator = []
    for s in iterify(getprop(d, p)):
        if isinstance(s, basestring):
            creator.append(s)
        else:
            creator.append(s.get("name"))
    creator = filter(None, creator)

    return {"creator": creator} if creator else {}

def title_transform(d, p):
    title = []
    for s in iterify(getprop(d, p)):
        if isinstance(s, basestring):
            title.append(s)
        else:
            title.append(s.get("#text"))
    title = filter(None, title)

    return {"title": title} if title else {}

def subject_transform(d, p):
    subject = []
    for s in iterify(getprop(d, p)):
        if isinstance(s, basestring):
            subject.append(s)
        else:
            subject.append(s.get("#text"))
    subject = filter(None, subject)

    return {"subject": " - ".join(subject)} if subject else {}

def date_transform(d, p):
    date = []
    for s in iterify(getprop(d, p)):
        if "qualifier" in s and s["qualifier"] == "creation":
            date.append(s.get("#text"))

    # Get dates from coverage
    coverage = getprop(d, META + "coverage", True)
    if coverage is not None:
        start = None
        end = None
        for s in iterify(coverage):
            if not isinstance(s, basestring):
                qualifier = s.get("qualifier")
                text = s.get("#text")
                if qualifier == "sDate":
                    start = text
                elif qualifier == "eDate":
                    end = text
        if start is not None and end is not None:
            date.append("%s-%s" % (start, end))

    return {"date": date} if date else {}

def url_transform(d, p):
    urls = {}
    for s in iterify(getprop(d, p)):
        try:
            qualifier = s.get("qualifier")
            text = s.get("#text")
        except:
            continue
        if qualifier == "itemURL":
            urls["isShownAt"] = text
        elif qualifier == "thumbnailURL":
            urls["object"] = text

    return urls

def spectype_and_format_transform(d, p):
    spectype_and_format = {}
    spectype = None
    for s in iterify(getprop(d, p)):
        values = s.split("_")
        spectype_and_format["format"] = values[0]
        try:
            if values[1] in ["book", "newspaper", "journal"]:
                spectype = values[1]
            elif values[1] == "leg":
                spectype = "government document"
            elif values[1] == "serial":
                spectype = "journal"
        except:
            pass
    if spectype is not None:
        spectype_and_format["specType"] = spectype.title()

    return spectype_and_format

def type_transform(d, p):
    type = []
    for s in iterify(getprop(d, p)):
        if s == "other":
            pass
        elif s == "audio":
            type.append("sound")
        elif s == "video":
            type.append("moving image")
        elif s == "website":
            type.append("interactive resource")
        else:
            type.append(s)

    return {"type": type} if type else {}

def dataprovider_transform(d, p):
    dataprovider = []
    for s in getprop(d, p):
        if "partner" in s:
            term = s.split(":")[-1]
            try:
                dataprovider.append(DATAPROVIDER_TERM_LABEL[term])
            except:
                logger.debug("TERM %s does not exist %s" % (term, d["_id"]))

    return {"dataProvider": dataprovider} if dataprovider else {}

def contributor_transform(d, p):
    contributor = []
    for s in iterify(getprop(d, p)):
        if isinstance(s, basestring):
            contributor.append(s)
        elif "name" in s:
            contributor.append(s.get("name"))

    if not contributor:
        logger.error("NO CONTRIBUTOR")
    return {"contributor": contributor} if contributor else {}

def relation_transform(d, p):
    relation = []
    for s in iterify(getprop(d, p)):
        if isinstance(s, basestring):
            relation.append(s)
        elif "#text" in s:
            relation.append(s.get("#text"))

    return {"relation": relation} if relation else {}

# Structure mapping the original top level property to a function returning a
# single item dict representing the new property and its value
CHO_TRANSFORMER = {
    "collection"         : lambda d, p: {"collection": getprop(d, p)},
    META + "date"        : date_transform,
    META + "title"       : title_transform,
    META + "rights"      : rights_transform,
    META + "format"      : type_transform,
    META + "creator"     : creator_transform,
    META + "subject"     : subject_transform,
    META + "relation"    : relation_transform,
    META + "language"    : lambda d, p: {"language": getprop(d, p)},
    META + "coverage"    : spatial_transform,
    META + "publisher"   : publisher_transform,
    META + "identifier"  : identifier_transform,
    META + "contributor" : contributor_transform,
    META + "description" : description_transform,
    META + "resourceType": spectype_and_format_transform 
}

AGGREGATION_TRANSFORMER = {
    "id"                 : lambda d, p: {"id": getprop(d, p),
                                         "@id" : "http://dp.la/api/items/"+ 
                                                 getprop(d, p)},
    "_id"                : lambda d, p: {"_id": getprop(d, p)},
    "provider"           : lambda d, p: {"provider": getprop(d, p)},
    "ingestType"         : lambda d, p: {"ingestType": getprop(d, p)},
    "ingestDate"         : lambda d, p: {"ingestDate": getprop(d, p)},
    "originalRecord"     : lambda d, p: {"originalRecord": getprop(d, p)},
    META + "identifier"  : url_transform,
    "originalRecord/header/setSpec": dataprovider_transform
}

@simple_service("POST", "http://purl.org/la/dp/oai_untl_to_dpla",
                "oai_untl_to_dpla", "application/ld+json")
def oaiuntltodpla(body, ctype, geoprop=None):
    """
    Convert output of JSON-ified OAI UNTL format into the DPLA JSON-LD format.

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

    # Remove "untl:" prefix from data keys
    data = remove_key_prefix(data, "untl:")

    # Apply all transformation rules from original document
    for p in CHO_TRANSFORMER:
        if exists(data, p):
            out["sourceResource"].update(CHO_TRANSFORMER[p](data, p))
    for p in AGGREGATION_TRANSFORMER:
        if exists(data, p):
            out.update(AGGREGATION_TRANSFORMER[p](data, p))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
