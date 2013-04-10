from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, delprop, exists

@simple_service('POST', 'http://purl.org/la/dp/set_prop', 'set_prop',
    'application/json')
def set_prop(body, ctype, prop=None, value=None, condition_prop=None,
             condition_value=None):
    """Sets the value of prop.

    Keyword arguments:
    body -- the content to load
    ctype -- the type of content
    prop -- the prop to set
    value -- the value to set prop to
    condition_prop -- (optional) the field that must exist to set the prop
    
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if not value:
        logger.error("No value was supplied to set_prop.")
    else:
        # If there is no condition_prop, set the prop, creating it if it does
        #not exist. If there is a condition_prop, only set the prop if the
        # condition_prop exists.
        if not condition_prop or exists(data, condition_prop):
            setprop(data, prop, value)

    return json.dumps(data)

@simple_service('POST', 'http://purl.org/la/dp/unset_prop', 'unset_prop',
    'application/json')
def unset_prop(body, ctype, prop=None, condition=None):
    """Unsets the value of prop.

    Keyword arguments:
    body -- the content to load
    ctype -- the type of content
    prop -- the prop to unset
    condition -- (optional) 
    
    """

    CONDITIONS = {
        "is_digit":   lambda v: v.isdigit()
    }

    def condition_met(v, condition):
        return CONDITIONS[condition](v)

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    # Check if prop exists to avoid key error
    if exists(data, prop):
        if not condition:
            delprop(data, prop)
        else:
            try:
                if condition_met(getprop(data, prop), condition):
                    delprop(data, prop)
            except KeyError:
                logger.error("CONDITIONS does not contain %s" % condition)
                

    return json.dumps(data)
