from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import setprop, delprop, exists

@simple_service('POST', 'http://purl.org/la/dp/sets_prop', 'sets_prop',
    'application/json')
def setsprop(body,ctype,prop=None,value=None):
    """Sets/unsets the value of prop.

    Keyword arguments:
    body -- the content to load
    ctype -- the type of content
    prop -- the prop to set/unset
    value -- the value to set prop to (None unsets prop)
    
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop):
        setprop(data, prop, value) if value else delprop(data, prop)

    return json.dumps(data)
