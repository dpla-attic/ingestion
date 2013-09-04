"""
NYPL specific module for getting preview url for the document;
"""

__author__ = 'aleksey'

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from akara import module_config


IGNORE = module_config().get('IGNORE')
PENDING = module_config().get('PENDING')

@simple_service('POST', 'http://purl.org/la/dp/nypl_identify_object',
                'nypl_identify_object', 'application/json')
def nypl_identify_object(body, ctype, download="True"):

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    original_document_key = u"originalRecord"
    original_preview_key = u"tmp_image_id"
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


