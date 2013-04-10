from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
import re

@simple_service('POST', 'http://purl.org/la/dp/dedup_value', 'dedup_value',
                'application/json')
def dedup_value(body, ctype, action="dedup_value", prop=None):
    '''
    Service that accepts a JSON document and enriches the prop field of that document by:

    a) Removing duplicates
    '''

    if prop is None:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        msg = "Prop param is None"
        logger.error(msg)
        return msg

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    for p in prop.split(","):
        v = getprop(data, p)
        if isinstance(v, list):
            # Remove whitespace, periods, parens
            clone = [re.sub("[ \.\(\)]", "", s).lower() for s in v]
            # Get index of unique values
            index = list(set([clone.index(s) for s in list(set(clone))]))
            
            setprop(data, p, [v[i] for i in index])

    return json.dumps(data)
