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

HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'


@simple_service('POST', 'http://purl.org/la/dp/ia_identify_object', 'ia_identify_object', HTTP_TYPE_JSON)
def ia_identify_object(body, ctype, download="True"):

    try:
        assert ctype.lower() == HTTP_TYPE_JSON, "%s is not %s" % (HTTP_HEADER_TYPE, HTTP_TYPE_JSON)
        data = json.loads(body)
    except Exception as e:
        error_text = "Bad JSON: %s: %s" % (e.__class__.__name__, str(e))
        logger.exception(error_text)
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return error_text

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


