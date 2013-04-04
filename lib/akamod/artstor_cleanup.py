"""
Artstor specific module for cleaning data;
"""

__author__ = 'aleksey'

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

from dplaingestion.selector import getprop, setprop, exists


HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'


@simple_service('POST', 'http://purl.org/la/dp/artstor_cleanup', 'artstor_cleanup', HTTP_TYPE_JSON)
def artstor_cleanup(body, ctype):

    try:
        assert ctype.lower() == HTTP_TYPE_JSON, "%s is not %s" % (HTTP_HEADER_TYPE, HTTP_TYPE_JSON)
        data = json.loads(body)
    except Exception as e:
        error_text = "Bad JSON: %s: %s" % (e.__class__.__name__, str(e))
        logger.exception(error_text)
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return error_text


    data_provider_key = u"dataProvider"
    if exists(data, data_provider_key):
            item = getprop(data, data_provider_key)
            if isinstance(item, basestring):
                cleaned_data_provider = item.replace("Repository:", "").lstrip()
                setprop(data, data_provider_key, cleaned_data_provider)

    return json.dumps(data)


