from akara import logger
from akara import module_config
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, delprop, exists

def convert_field(value):
    import re
    return re.sub(r"(--.*)", '', value)

def convert(data, prop):

    value = getprop(data, prop, True)
    if not value:
        return

    if isinstance(value, basestring):
        if value == "creator":
            delprop(data, prop)
        else:
            v = convert_field(value)
            setprop(data, prop, v)

    elif isinstance(value, list):
        values = []
        for item in value:
            if item == "creator":
                continue
            v = convert_field(item)
            values.append(v)
        setprop(data, prop, values)


@simple_service('POST', 'http://purl.org/la/dp/mwdl_cleanup_field', 'mwdl_cleanup_field', 'application/json')
def lookup(body, ctype, prop="sourceResource/creator"):
    """ Performs cleaning of the given field.
    """

    # Parse incoming JSON
    data = {}
    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    convert(data, prop)

    return json.dumps(data)
