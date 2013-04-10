import os
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from akara import module_config

IGNORE = module_config().get('IGNORE')
PENDING = module_config().get('PENDING')

@simple_service('POST', 'http://purl.org/la/dp/david_rumsey_identify_object',
    'david_rumsey_identify_object', 'application/json')
def david_rumsey_identify_object(body, ctype, download="True"):
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
        handle = getprop(data, handle_field)
    else:
        msg = "Field %s does not exist" % handle_field
        logger.debug(msg)
        return body

    data["object"] = handle[1]

    status = IGNORE
    if download == "True":
        status = PENDING

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    return json.dumps(data)
