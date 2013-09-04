"""
Artstor specific module for cleaning data;
"""

__author__ = 'aleksey'

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

from dplaingestion.selector import getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/artstor_cleanup',
                'artstor_cleanup', 'application/json')
def artstor_cleanup(body, ctype):

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    data_provider_key = u"dataProvider"
    if exists(data, data_provider_key):
            item = getprop(data, data_provider_key)
            if isinstance(item, basestring):
                cleaned_data_provider = item.replace("Repository:", "").lstrip()
                setprop(data, data_provider_key, cleaned_data_provider)

    return json.dumps(data)


