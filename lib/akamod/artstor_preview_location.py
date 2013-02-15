"""
Artstor specific module for getting preview url for the document;
"""

__author__ = 'aleksey'

import re

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'


@simple_service('POST', 'http://purl.org/la/dp/artstor_preview_location', 'artstor_preview_location', HTTP_TYPE_JSON)
def artstor_preview_location(body, ctype):

    LOG_JSON_ON_ERROR = True
    def log_json():
        if LOG_JSON_ON_ERROR:
            logger.debug(body)

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
    original_sources_key = u"handle"
    artstor_preview_prefix = "Thumbnail"
    preview_url_key = u"preview_source_url"

    if original_document_key not in data:
        logger.error("There is no '%s' key in JSON for doc [%s].", original_document_key, data[u'id'])
        log_json()
        return body

    if original_sources_key not in data[original_document_key]:
        logger.error("There is no '%s/%s' key in JSON for doc [%s].", original_document_key, original_sources_key, data[u'id'])
        log_json()
        return body

    preview_url = None
    http_re = re.compile("https?://.*$", re.I)
    for s in data[original_document_key][original_sources_key]:
        if s.startswith(artstor_preview_prefix):
            match = re.search(http_re, s)
            if match:
                preview_url = match.group(0)
                break

    if not preview_url:
        logger.error("Can't find url with '%s' prefix in [%s] for fetching document preview url for Artstor.", artstor_preview_prefix, data[original_document_key][original_sources_key])
        log_json()
        return body

    data[preview_url_key] = preview_url
    return json.dumps(data)

