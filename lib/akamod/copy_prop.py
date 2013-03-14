from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import delprop, getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/copy_prop', 'copy_prop',
    'application/json')
def copyprop(body,ctype,prop=None,to_prop=None,create=False,key=None,
    remove=None,no_replace=None):
    """Copies value in one prop to another prop.

    Keyword arguments:
    body -- the content to load
    ctype -- the type of content
    prop -- the prop to copy from (default None)
    to_prop -- the prop to copy into (default None)
    create -- creates to_prop if True (default False)
    key -- the key to use if to_prop is a dict (default None)
    remove  -- removes prop if True (default False)
    no_replace -- creates list of to_prop string and appends prop if True
    
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop) and create and not exists(data, to_prop):
        setprop(data, to_prop, "")

    if exists(data, prop) and exists(data, to_prop):
        val = getprop(data, prop)
        to_element = getprop(data, to_prop)

        if isinstance(to_element, basestring):
            if no_replace:
                el = [to_element] if to_element else []
                el.append(val)
                # Flatten
                val = [e for s in el for e in (s if not isinstance(s, basestring) else [s])]
            setprop(data, to_prop, val)
        else:
            # If key is set, assume to_element is dict or list of dicts
            if key:
                if not isinstance(to_element, list):
                    to_element = [to_element]
                for dict in to_element:
                    if exists(dict, key):
                        setprop(dict, key, val)
                    else:
                        msg = "Key %s does not exist in %s" % (key, to_prop)
                        logger.debug(msg)
            else:
                # Handle case where to_element is a list
                if isinstance(to_element, list):
                    if isinstance(val, list):
                        to_element = to_element + val
                    else:
                        to_element.append(val)
                    setprop(data, to_prop, to_element) 
                else:
                    # to_prop is dictionary but no key was passed.
                    msg = "%s is a dictionary but no key was passed" % to_prop
                    logger.warn(msg)
                    setprop(data, to_prop, val)

        if remove:
            delprop(data, prop)

    return json.dumps(data)
