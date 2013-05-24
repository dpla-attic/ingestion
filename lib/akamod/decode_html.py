from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import exists, getprop, setprop
import re

@simple_service('POST', 'http://purl.org/la/dp/decode_html', 'decode_html',
                'application/json')
def decode_html(body, ctype, prop=None):
    """Decodes any encoded html in the prop

    Keyword arguments:
    body -- the content to load
    ctype -- the type of content
    prop -- the prop to decode
    
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    REGEX = ('&quot;', '"'), ('&amp;', '&'), ('&lt;', '<'), ('&gt;', '>')

    if prop and exists(data, prop):
        decoded = []
        v = getprop(data, prop)
        if not isinstance(v, list):
            v = [v]
        for s in v:
            if isinstance(s, basestring):
                for p, r in REGEX:
                    s = re.sub(p, r, s)
            decoded.append(s)

        setprop(data, prop, decoded)
                

    return json.dumps(data)
