from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/copy_prop', 'copy_prop', 'application/json')
def copyprop(body,ctype,prop=None,to_prop=None,create=False,key=None):
    """ Copies value in prop to to/to[key]/to[i][key],
        whichever applies. If to does not exist but create is true,
        to is created.
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
            setprop(data, to_prop, val)
        else:
            if not isinstance(to_element, list):
                to_element = [to_element]
            for i in range(0,len(to_element)):
                if key:
                    if exists(to_element[i], key):
                        setprop(to_element[i], key, val)
                    else:
                        msg = "Key %s does not exist in %s" % (key, to_prop)
                        logger.error(msg)
                else:
                    setprop(data, to_element[i], val)

    return json.dumps(data)
