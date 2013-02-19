from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

IGNORE = module_config().get('IGNORE')
PENDING = module_config().get('PENDING')

@simple_service('POST', 'http://purl.org/la/dp/contentdm_identify_object',
    'contentdm_identify_object', 'application/json')
def contentdm_identify_object(body, ctype, rights_field="aggregatedCHO/rights", download="True"):
    """
    Responsible for: adding a field to a document with the URL where we
    should expect to the find the thumbnail
    """

    LOG_JSON_ON_ERROR = True

    def log_json():
        if LOG_JSON_ON_ERROR:
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

    handle_field = "originalRecord/handle"
    if exists(data, handle_field):
        url = getprop(data, handle_field)[1]
    else:
        msg = "Field %s does not exist" % handle_field
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
    if exists(data, rights_field):
        rights = getprop(data, rights_field)

    data["object"] = {"@id": thumb_url, "format": "", "rights": rights}

    status = IGNORE
    if download == "True":
        status = PENDING

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    return json.dumps(data)
