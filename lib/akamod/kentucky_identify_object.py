import os
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from akara import module_config

IGNORE = module_config().get('IGNORE')
PENDING = module_config().get('PENDING')

@simple_service('POST', 'http://purl.org/la/dp/kentucky_identify_object',
    'kentucky_identify_object', 'application/json')
def kentucky_identify_object(body, ctype, download="True"):
    """
    Responsible for: adding a field to a document with the URL where we
    should expect to the find the thumbnail
    """
    data = {}
    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    relation_field = "sourceResource/relation"
    if exists(data, relation_field):
        url = getprop(data, relation_field)
    else:
        logger.debug("Field %s does not exist" % relation_field)
        return body

    base_url, ext = os.path.splitext(url)
    data["object"] = "%s_tb%s" % (base_url, ext)

    status = IGNORE
    if download == "True":
        status = PENDING

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    return json.dumps(data)
