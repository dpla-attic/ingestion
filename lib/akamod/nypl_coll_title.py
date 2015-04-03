"""
NYPL specific module for setting title for given collection
"""

from akara import logger
from akara import response
from akara import module_config
from akara.services import simple_service
from amara.thirdparty import json, httplib2
import xmltodict

CACHE_DIR = module_config().get('CACHE_DIR')

@simple_service('POST', 'http://purl.org/la/dp/nypl-coll-title',
                'nypl-coll-title', 'application/json')
def nypl_identify_object(body, ctype, list_sets=None):

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    H = httplib2.Http(CACHE_DIR)
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


