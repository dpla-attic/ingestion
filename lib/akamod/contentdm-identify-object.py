from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists


@simple_service('POST', 'http://purl.org/la/dp/identify_preview_location',
    'contentdm-identify-object', 'application/json')
def contentdm_identify_object(body, ctype, rights_field, download):
    """
    Responsible for: adding a field to a document with the URL where we
    should expect to the find the thumbnail
    """

    LOG_JSON_ON_ERROR = True

    def log_json():
        if LOG_JSON_ON_ERROR:
            logger.debug(body)

    logger.debug(body)
    data = {}
    try:
        data = json.loads(body)
    except Exception as e:
        msg = "Bad JSON: " + e.args[0]
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

    url = None
    try:
        url = getprop(data, "originalRecord/handle")[1]
        logger.debug("Found URL: " + url)
    except KeyError as e:
        msg = e.args[0]
        logger.error(msg)
        return body

    p = url.split("u?")

    if len(p) != 2:
        logger.error("Bad URL %s. It should have just one 'u?' part." % url)
        log_json()
        return body

    (base_url, rest) = p

    if base_url == "" or rest == "":
        logger.error("Bad URL: %s. There is no 'u?' part." % url)
        log_json()
        return body

    p = rest.split(",")

    if len(p) != 2:
        logger.error("Bad URL %s. Expected two parts at the end, used in " +
            "thumbnail URL for CISOROOT and CISOPTR." % url)
        log_json()
        return body

    # Thumb url field.
    thumb_url = "%scgi-bin/thumbnail.exe?CISOROOT=%s&CISOPTR=%s" % \
        (base_url, p[0], p[1])

    # Gettings the rights field

    rights = None
    try:
        rights = getprop(data, rights_field)
    except KeyError as e:
        msg = e.args[0]
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

    ob = {"@id": thumb_url, "format": "", "rights": rights}

    data["object"] = ob

    status = "ignore"
    if download == "True":
        status = "pending"

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    logger.debug("Thumbnail URL = " + thumb_url)
    return json.dumps(data)
