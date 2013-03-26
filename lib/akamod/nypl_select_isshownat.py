"""
NYPL specific module for getting full view url for the document;
"""

__author__ = 'aleksey'

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json


HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'


@simple_service('POST', 'http://purl.org/la/dp/nypl_select_isshownat', 'nypl_select_isshownat', HTTP_TYPE_JSON)
def nypl_select_isshownat(body, ctype):

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
    original_preview_key = u"_high_res_link"
    source_key = u"isShownAt"

    if original_document_key not in data:
        logger.error("There is no '%s' key in JSON for doc [%s].", original_document_key, data[u'id'])
        return body

    if original_preview_key not in data[original_document_key]:
        logger.error("There is no '%s/%s' key in JSON for doc [%s].", original_document_key, original_preview_key, data[u'id'])
        return body

    data[source_key] = data[original_document_key][original_preview_key]
    return json.dumps(data)


