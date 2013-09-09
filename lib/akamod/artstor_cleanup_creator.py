from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

from dplaingestion.selector import getprop, setprop, exists
import re

CLEANUP = (
    "And", "Artist:", "Author:", "Binder:", "Drawn by", "Illuminator:", "Or",
    "Scribe:", "Resolve"
    )

@simple_service('POST', 'http://purl.org/la/dp/artstor_cleanup_creator',
                'artstor_cleanup_creator', 'application/json')
def artstor_cleanup_creator(body, ctype, prop="sourceResource/creator"):
    """
    Service that accepst a JSON document and removes cleans the
    sourceResource/creator field by removing the values in REGEXES if the
    field value begins with them
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop):
        item = getprop(data, prop)
        if not isinstance(item, list):
            item = [item]
        for i in range(len(item)):
            for s in CLEANUP:
                item[i] = re.sub(r"(?i)^{0}".format(s), "", item[i].strip()).lstrip()
            
        setprop(data, prop, item[0] if len(item) == 1 else item)

    return json.dumps(data)
