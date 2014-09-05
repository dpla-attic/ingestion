"""
Pipeline module to recursively strip all strings found in the posted object
"""

import json
from akara import response
from akara.services import simple_service


@simple_service('POST',
                'http://purl.org/la/dp/strip',
                'strip',
                'application/json')
def strip(body, ctype_ignored):
    """Strip whitespace from any strings found within the given object"""
    try:
        d = json.loads(body)
        return json.dumps(_strip_r(d))
    except Exception as e:
        response.code = 500
        response.add_header('Content-Type', 'text/plain')
        if type(e) == ValueError:
            return 'Unable to parse request body as JSON'
        elif type(e) == KeyError:
            return 'No originalRecord.source for determining object'
        else:
            return e.message

def _strip_r(thing):
    """Recursively strip strings"""
    if isinstance(thing, basestring):
        return thing.strip()
    if type(thing) == dict:
        items = thing.iteritems()
    elif type(thing) == list:
        items = enumerate(thing)
    else:
        return thing
    for key, value in items:
        thing[key] = _strip_r(value)
    return thing
