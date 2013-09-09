"""
Georgia specific module for getting preview url for the document;
"""

__author__ = 'aleksey'

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from akara import module_config

from dplaingestion import selector


IGNORE = module_config().get('IGNORE')
PENDING = module_config().get('PENDING')

@simple_service('POST', 'http://purl.org/la/dp/georgia_identify_object',
                'georgia_identify_object', 'application/json')
def georgia_identify_object(body, ctype, download="True"):

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    original_document_key = u"originalRecord"
    original_sources_key = u"id"
    preview_url_pattern = "http://dlg.galileo.usg.edu/%(repo)s/%(coll)s/do-th:%(item)s"

    if original_document_key not in data:
        logger.error("There is no '%s' key in JSON for doc [%s].", original_document_key, data[u'id'])
        return body

    if original_sources_key not in data[original_document_key]:
        logger.error("There is no '%s/%s' key in JSON for doc [%s].", original_document_key, original_sources_key, data[u'id'])
        return body

    _id = data[original_document_key][original_sources_key]
    _id_head, sep, _item_id_tuple = _id.rpartition(":")

    if not _item_id_tuple:
        logger.error("Can not get item id tuple from the [%s] identifier.", _id)
        return body

    try:
        repo, coll, item = _item_id_tuple.split("_", 2)
    except ValueError:
        logger.error("Can not fetch \"repo, coll, item\" values from [%s], splitting by \"_\"", _item_id_tuple)
        return body

    preview_url = preview_url_pattern % {"repo": repo, "coll": coll, "item": item}

    data["object"] = preview_url

    status = IGNORE
    if download == "True":
        status = PENDING

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    return json.dumps(data)


