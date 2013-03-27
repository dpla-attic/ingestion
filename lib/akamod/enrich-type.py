from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import delprop, getprop, setprop, exists
import re

@simple_service('POST', 'http://purl.org/la/dp/enrich-type', 'enrich-type',
                'application/json')
def enrichtype(body,ctype,action="enrich-type", prop="sourceResource/type",
               format_field="sourceResource/format"):
    """   
    Service that accepts a JSON document and enriches the "type" field of that
    document by: 

    a) making the type lowercase
    b) converting "image" to "still image"
      (TODO: Amy to confirm that this is ok)
    c) applying a set of regexps to do data cleanup (remove plural forms)
    d) moving all items that are not standard DC types to the
       sourceResource/format
       (http://dublincore.org/documents/resource-typelist/)
    
    By default works on the 'type' field, but can be overridden by passing the
    name of the field to use as a parameter
    """

    REGEXPS = ('images','image'), ('still image','image')
    DC_TYPES = ['collection', 'dataset', 'event', 'image', 'still image',
                'interactive resource', 'moving image',
                'physical object', 'service', 'software', 'sound',
                'text']

    def cleanup(s):
        s = s.lower().strip()
        for pattern, replace in REGEXPS:
            s = re.sub(pattern, replace, s)
        return s

    def is_dc_type(s):
        return s in DC_TYPES

    try :
        data = json.loads(body)
    except Exception:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    if exists(data,prop):
        v = getprop(data, prop)
        dctype = []
        f = getprop(data, format_field) if exists(data, format_field) else []
        if not isinstance(f, list):
            f = [f]

        for s in (v if not isinstance(v,basestring) else [v]):
            if is_dc_type(cleanup(s)):
                dctype.append(cleanup(s))
            else:
                f.append(s)

        if dctype:
            if len(dctype) == 1:
                dctype = dctype[0]
            setprop(data, prop, dctype)
        else:
            delprop(data, prop)

        if f:
            setprop(data, format_field, f)

    return json.dumps(data)
