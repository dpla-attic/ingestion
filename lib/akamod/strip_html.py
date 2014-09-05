"""
Pipeline module to recursively strip HTML tags from all strings found in the
posted object
"""

import json
from markupsafe import Markup
from akara import response
from akara.services import simple_service


@simple_service('POST',
                'http://purl.org/la/dp/strip_html',
                'strip_html',
                'application/json')
def strip_html(body, ctype_ignored):
    """Strip HTML tags and whitespace from strings within the given object

    Remove HTML tags and convert HTML entities to UTF-8 characters.
    Compacts consecutive whitespace to single space characters, and strips
    whitespace from ends of strings.
    """
    try:
        d = json.loads(body)
        return json.dumps(_strip_r(d))
    except Exception as e:
        response.code = 500
        response.add_header('Content-Type', 'text/plain')
        if type(e) == ValueError:
            return 'Unable to parse request body as JSON'
        else:
            return e.message

def _strip_r(thing):
    """Recursively strip HTML tags and whitespace from strings"""
    if isinstance(thing, basestring):
        return Markup(thing).striptags()
    if type(thing) == dict:
        items = thing.iteritems()
    elif type(thing) == list:
        items = enumerate(thing)
    else:
        return thing
    for key, value in items:
        thing[key] = _strip_r(value)
    return thing
