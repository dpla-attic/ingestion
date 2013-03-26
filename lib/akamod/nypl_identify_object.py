"""
NYPL specific module for getting preview url for the document;
"""

__author__ = 'aleksey'

import re

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from akara import module_config

from dplaingestion import selector


IGNORE = module_config().get('IGNORE')
PENDING = module_config().get('PENDING')

HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'


@simple_service('POST', 'http://purl.org/la/dp/nypl_identify_object', 'nypl_identify_object', HTTP_TYPE_JSON)
def nypl_identify_object(body, ctype, download="True"):

    try:
        assert ctype.lower() == HTTP_TYPE_JSON, "%s is not %s" % (HTTP_HEADER_TYPE, HTTP_TYPE_JSON)
        data = json.loads(body)
    except Exception as e:
        error_text = "Bad JSON: %s: %s" % (e.__class__.__name__, str(e))
        logger.exception(error_text)
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return error_text

    original_document_key = u"originalRecord"
    original_preview_key = u"_image_id"
    preview_format = "http://images.nypl.org/index.php?id={0}&t=t"

    if original_document_key not in data:
        logger.error("There is no '%s' key in JSON for doc [%s].", original_document_key, data[u'id'])
        return body

    if original_preview_key not in data[original_document_key]:
        logger.error("There is no '%s/%s' key in JSON for doc [%s].", original_document_key, original_preview_key, data[u'id'])
        return body

    preview_url = preview_format.format(data[original_document_key][original_preview_key])
    data["object"] = preview_url

    status = IGNORE
    if download == "True":
        status = PENDING

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    return json.dumps(data)


