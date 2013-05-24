"""
NYPL specific module for setting title for given collection;
"""

__author__ = 'aleksey'

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json, httplib2
import xmltodict


HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'


@simple_service('POST', 'http://purl.org/la/dp/nypl-coll-title', 'nypl-coll-title', HTTP_TYPE_JSON)
def nypl_identify_object(body, ctype, list_sets=None):

    try:
        assert ctype.lower() == HTTP_TYPE_JSON, "%s is not %s" % (HTTP_HEADER_TYPE, HTTP_TYPE_JSON)
        data = json.loads(body)
    except Exception as e:
        error_text = "Bad JSON: %s: %s" % (e.__class__.__name__, str(e))
        logger.exception(error_text)
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return error_text

    H = httplib2.Http('/tmp/.cache')
    H.force_exception_as_status_code = True
    resp, content = H.request(list_sets)
    if not resp[u'status'].startswith('2'):
        logger.error('  HTTP error (' + resp[u'status'] + ') resolving URL: ' + list_sets)
        return body
    content_dict = xmltodict.parse(content, xml_attribs=True, attr_prefix='', force_cdata=False, ignore_whitespace_cdata=True)
    sets = content_dict["nyplAPI"]["response"]

    for r in sets:
        if "collection" == r:
            for coll_dict in sets[r]:
                if "uuid" in coll_dict and "title" in coll_dict and (coll_dict["uuid"] == data["title"] or coll_dict["uuid"] in data["@id"]):
                    data["title"] = coll_dict["title"]

    return json.dumps(data)


