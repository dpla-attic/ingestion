"""
Akara module for extracting document source from dplaSourceRecord;
Assumes that original version of document is stored at dplaSourceRecord key in a json tree.
"""

__author__ = 'alexey'

import re

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'

@simple_service('POST', 'http://purl.org/la/dp/artstor_select_isshownat', 'artstor_select_isshownat', HTTP_TYPE_JSON)
def filter_empty_values_endpoint(body, ctype):

    LOG_JSON_ON_ERROR = True
    def log_json():
        if LOG_JSON_ON_ERROR:
            logger.debug(body)

    try:
        assert ctype == HTTP_TYPE_JSON, "%s is not %s" % (HTTP_HEADER_TYPE, HTTP_TYPE_JSON)
        data = json.loads(body)
    except Exception as e:
        error_text = "Bad JSON: %s: %s" % (e.__class__.__name__, str(e))
        logger.exception(error_text)
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return error_text

    originalDocumentKey = u"dplaSourceRecord"
    originalSourcesKey = u"handle"
    artstorSourcePrefix = "Image View"
    sourceKey = u"source"

    if originalDocumentKey not in data:
        logger.error("There is no '%s' key in JSON for doc [%s].", originalDocumentKey, data[u'id'])
        log_json()
        return body

    if originalSourcesKey not in data[originalDocumentKey]:
        logger.error("There is no '%s/%s' key in JSON for doc [%s].", originalDocumentKey, originalSourcesKey, data[u'id'])
        log_json()
        return body

    source = None
    for s in data[originalDocumentKey][originalSourcesKey]:
        if s.startswith(artstorSourcePrefix):
            match = re.search("http://.*$", s, re.I)
            if match:
                source = match.group(0)
                break

    if not source:
        logger.error("Can't find url with '%s' prefix in [%s] for fetching document source for Artstor.", artstorSourcePrefix, data[originalDocumentKey][originalSourcesKey])
        log_json()
        return body

    data[sourceKey] = source
    return json.dumps(data)

