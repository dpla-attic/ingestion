"""
Internet Archive specific module for getting preview url for the document;
"""

__author__ = 'aleksey'

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from akara import module_config

from dplaingestion.selector import getprop, exists


IGNORE = module_config().get('IGNORE')
PENDING = module_config().get('PENDING')

@simple_service('POST', 'http://purl.org/la/dp/ia_identify_object',
                'ia_identify_object', 'application/json')
def ia_identify_object(body, ctype, download="True"):
    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    original_preview_key = "originalRecord/files/gif"
    preview_format = "http://www.archive.org/download/{0}/{1}"

    try:
        preview_url = preview_format.format(getprop(data, "originalRecord/_id"), getprop(data, original_preview_key))
    except KeyError:
        logger.error("Can not build preview url by path \"%s\" for doc [%s]", original_preview_key, data[u"id"])
        return body

    data["object"] = preview_url
    status = IGNORE
    if download == "True":
        status = PENDING

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    return json.dumps(data)


