from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from akara import module_config
from dplaingestion.selector import getprop
from dplaingestion.utilities import iterify
import urllib2
import re


IGNORE = module_config().get('IGNORE')
PENDING = module_config().get('PENDING')

NS_GOOGLE_PREFIX = {
    "chi": "CHI",
    "coo": "CORNELL",
    "hvd": "HARVARD",
    "ien": "NWU",
    "inu": "IND",
    "mdp": "UOM",
    #"njp": "PRNC",
    "nnc1": "COLUMBIA",
    "nyp": "NYPL",
    "pst": "PSU",
    "pur1": "PURD",
    "uc1": "UCAL",
    "ucm": "UCM",
    "umn": "MINN",
    "uva": "UVA",
    "wu": "WISC"
}

def _get_UCAL_prefix(barcode):
    barcode_length = len(barcode)
    if barcode_length == 11 and barcode[0] == "l":
        google_prefix = "UCLA"
    elif barcode_length == 10:
        google_prefix = "UCB"
    elif barcode_length == 14:
        b = barcode[1:5]
        if b == "1822":
            google_prefix = "UCSD"
        elif b == "1970":
            google_prefix = "UCI"
        elif b == "1378":
            google_prefix = "UCSF"
        elif b == "2106":
            google_prefix = "UCSC"
        elif b == "1205":
            google_prefix = "UCSB"
        elif b == "1175":
            google_prefix = "UCD"
        elif b == "1158":
            google_prefix = "UCLA"
        elif b == "1210":
            google_prefix = "UCR"
        else:
            google_prefix = "UCAL"
    else:
        google_prefix = "UCAL"

    return google_prefix

@simple_service('POST', 'http://purl.org/la/dp/hathi_identify_object',
                'hathi_identify_object', 'application/json')
def hathi_identify_object(body, ctype, download="True"):
    """Build the thumbnail URL as follows:

       Base URL: http://books.google.com/books?jscmd=viewapi&bibkeys=<params>
       
       Where params are:
            <google_prefix>:<hathi_id>,ISBN:<isbn>,OCLC<oclc_id>
    """
    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"


    identifiers = getprop(data, "sourceResource/identifier", True)
    if identifiers is None:
        logger.debug("Record %s has no sourceResource/identifier field" %
                     data["_id"])
        return json.dumps(data)

    hathi_id = data["_id"].split("--")[-1]
    oclc_id = None
    isbn = None

    # Get OCLC and ISBN
    for id in iterify(identifiers):
        if "(OC" in id:
            oclc_id = re.sub("\(OCo?LC\)(oc[mn]?)?", "", id).strip()
        elif "ISBN:" in id and isbn is None:
            temp = id.replace("ISBN:", "").strip()
            if temp:
                isbn = temp

    if oclc_id is None:
        logger.debug("Field sourceResource/identifier does not contain " +
                     "OCLC for %s" % data["_id"])
        return json.dumps(data)

    # Get Google prefix from namespace
    datafield = getprop(data, "originalRecord/datafield", True)
    if datafield is None:
        logger.error("Record %s has no originalRecord/datafield" %
                     data["_id"])
        return json.dumps(data)
    ns = []
    for field in datafield:
        if field.get("tag") == "974":
            if "subfield" in field:
                for subfield in field["subfield"]:
                    if subfield.get("code") == "u":
                        ns.append(subfield.get("#text", ""))
    google_prefix = None
    barcode = None
    for ns_str in ns:
        namespace = ns_str.split(".")[0]
        barcode = ns_str.split(".")[-1]
        if namespace.split(".")[0] in NS_GOOGLE_PREFIX:
            google_prefix = NS_GOOGLE_PREFIX[namespace]
            break
    if google_prefix is None:
        logger.error("No Google prefix mappend to any namespace in %s for %s" %
                     (ns, data["_id"]))
        return json.dumps(data)

    if google_prefix == "UCAL":
        google_prefix = _get_UCAL_prefix(barcode)

    url = "http://books.google.com/books?jscmd=viewapi&bibkeys="
    url += "%s:%s" % (google_prefix, hathi_id)
    if oclc_id:
        url += ",OCLC:%s" % oclc_id
    if isbn:
        url += ",ISBN:%s" % isbn

    # Set the user agent since the Dynamic Links feature of the Google Books
    # API supports only the client side
    user_agent = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:20.0) " + \
                 "Gecko/20100101 Firefox/20.0"
    req = urllib2.Request(url, None, {"User-agent": user_agent})
    try:
        s = urllib2.urlopen(req).read()
    except:
        logger.error("Unable to open/read %s for %s" % (url, data["_id"]))
        return json.dumps(data)
    s = s.replace("var _GBSBookInfo = ", "").replace(";", "")
    try:
        j = json.loads(s)
    except:
        logger.error("Unable to parse content from %s as JSON for %s" %
                     (url, data["_id"]))
    if not j:
        logger.error("Resp from %s returned empty _GBSBookInfo for %s" %
                     (url, data["_id"]))
        return json.dumps(data)

    logger.debug("Google URL for %s: %s" % (data["_id"], url))
    logger.debug("Response: %s" % j)
    thumbnail_url = getprop(j, "OCLC:%s/thumbnail_url" % oclc_id, True)
    if thumbnail_url is None:
        msg = ("Resp from %s does not contain 'OCLC:%s/thumbnail_url' for %s" %
               (url, oclc_id, data["_id"]))
        logger.error(msg)
        return json.dumps(data)

    data["object"] = thumbnail_url

    status = IGNORE
    if download == "True":
        status = PENDING

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    return json.dumps(data)
