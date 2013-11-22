from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, delprop, exists

@simple_service('POST', 'http://purl.org/la/dp/set_prop', 'set_prop',
    'application/json')
def set_prop(body, ctype, prop=None, value=None, condition_prop=None,
             condition_value=None, _dict=None):
    """Sets the value of prop.

    Keyword arguments:
    body -- the content to load
    ctype -- the type of content
    prop -- the prop to set
    value -- the value to set prop to
    condition_prop -- (optional) the field that must exist to set the prop
    condition_value -- (optional, if condition_prop set) the value that
                       condition_prop must have to set the prop
    
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
        if _dict:
            try:
                value = json.loads(value)
            except Exception, e:
                logger.error("Unable to parse set_prop value: %s" % e)

        def _set_prop():
            """Returns true if

               1. The condition_prop is not set OR
               2. The condition_prop is set and exists and the condition_value
                  is None OR
               3. The condition_prop is set and exists, the condition_value is
                  set, and the value of condition_prop equals condition_value
            """
            return (not condition_prop or
                    (exists(data, condition_prop) and
                     (not condition_value or
                      getprop(data, condition_prop) == condition_value)))

        if _set_prop():
            setprop(data, prop, value)

    return json.dumps(data)

@simple_service('POST', 'http://purl.org/la/dp/unset_prop', 'unset_prop',
    'application/json')
def unset_prop(body, ctype, prop=None, condition=None, condition_prop=None):
    """Unsets the value of prop.

    Keyword arguments:
    body -- the content to load
    ctype -- the type of content
    prop -- the prop to unset
    condition -- the condition to be met (uses prop by default) 
    condition_prop -- the prop(s) to use in the condition (comma-separated if
                      multiple props)
    
    """

    CONDITIONS = {
        "is_digit": lambda v: v[0].isdigit(),
        "mwdl_exclude": lambda v: (v[0] == "collections" or
                                   v[0] == "findingAids"),
        "hathi_exclude": lambda v: ("University of Minnesota" in v[0] and
                                    v[1] == "image")
    }

    def condition_met(condition_prop, condition):
        values = []
        props = condition_prop.split(",")
        for p in props:
            values.append(getprop(data, p, True))

        return CONDITIONS[condition](values)

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
            if not condition_prop:
                condition_prop = prop
            try:
                if condition_met(condition_prop, condition):
                    logger.debug("Unsetting prop %s for doc with id %s" % 
                                 (prop, data["_id"]))
                    delprop(data, prop)
            except KeyError:
                logger.error("CONDITIONS does not contain %s" % condition)
                

    return json.dumps(data)
