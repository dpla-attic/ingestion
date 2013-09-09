"""
NYPL specific module for getting full view url for the document;
"""

__author__ = 'aleksey'

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

@simple_service('POST', 'http://purl.org/la/dp/nypl_select_hasview',
                'nypl_select_hasview', 'application/json')
def nypl_select_hasview(body, ctype):

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    original_document_key = u"originalRecord"
    original_preview_key = u"tmp_high_res_link"
    source_key = u"hasView"

    if original_document_key not in data:
        logger.error("There is no '%s' key in JSON for doc [%s].", original_document_key, data[u'id'])
        return body

    if original_preview_key not in data[original_document_key]:
        logger.error("There is no '%s/%s' key in JSON for doc [%s].", original_document_key, original_preview_key, data[u'id'])
        return body

    data[source_key] = {"@id": data[original_document_key][original_preview_key], "format": None}
    return json.dumps(data)


