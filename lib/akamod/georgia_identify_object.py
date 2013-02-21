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

HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'


@simple_service('POST', 'http://purl.org/la/dp/georgia_identify_object', 'georgia_identify_object', HTTP_TYPE_JSON)
def georgia_identify_object(body, ctype, download="True"):

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
    original_sources_key = u"id"
    preview_url_pattern = "http://dlg.galileo.usg.edu/%(repo)s/%(coll)s/do-th:%(item)s"

    if original_document_key not in data:
        logger.error("There is no '%s' key in JSON for doc [%s].", original_document_key, data[u'id'])
        log_json()
        return body

    if original_sources_key not in data[original_document_key]:
        logger.error("There is no '%s/%s' key in JSON for doc [%s].", original_document_key, original_sources_key, data[u'id'])
        log_json()
        return body

    _id = data[original_document_key][original_sources_key]
    _id_head, sep, _item_id_tuple = _id.rpartition(":")

    if not _item_id_tuple:
        logger.error("Can not get item id tuple from the [%s] identifier.", _id)
        log_json()
        return body

    try:
        repo, coll, item = _item_id_tuple.split("_", 3)
    except ValueError:
        logger.error("Can not fetch \"repo, coll, item\" values from [%s], splitting by \"_\"", _item_id_tuple)
        log_json()
        return body

    preview_url = preview_url_pattern % {"repo": repo, "coll": coll, "item": item}

    data["object"] = {"@id": preview_url,
                      "format": None,
                      "rights": selector.getprop(data, "aggregatedCHO/rights", keyErrorAsNone=True)}

    status = IGNORE
    if download == "True":
        status = PENDING

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    return json.dumps(data)


