from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/replace_substring',
                'replace_substring', 'application/json')
def replace_substring(body, ctype, prop=None, old=None, new=None):
    """Replaces a substring in prop

    Keyword arguments:
    body -- the content to load
    ctype -- the type of content
    prop -- the prop to apply replacing
    old -- the substring to replace
    new -- the substring to replaced old with
    
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if not old or not new:
        logger.error("No old or new parameters were provided")
    else:
        if exists(data, prop):
            v = getprop(data, prop)
            setprop(data, prop, v.replace(old, new))

    return json.dumps(data)

