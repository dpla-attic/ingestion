from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import setprop, delprop, exists

@simple_service('POST', 'http://purl.org/la/dp/set_prop', 'set_prop',
    'application/json')
def set_prop(body, ctype, prop=None, value=None, condition_prop=None):
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

    # If there is no condition_prop, set the prop, creating it if it does not
    # exist. If there is a condition_prop, only set the prop if the
    # condition_prop exists.
    if not condition_prop or exists(data, condition_prop):
        if value:
            setprop(data, prop, value)
        else:
            # Check if prop exists to avoid key error
            if exists(data, prop):
                delprop(data, prop)

    return json.dumps(data)
