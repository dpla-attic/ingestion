from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
import re

@simple_service('POST', 'http://purl.org/la/dp/enrich-type', 'enrich-type', 'application/json')
def enrichtype(body,ctype,action="enrich-type", prop="aggregatedCHO/type", alternate="aggregatedCHO/physicalMedium"):
    """   
    Service that accepts a JSON document and enriches the "type" field of that document
    by: 

    a) making the type lowercase
    b) converting "image" to "still image" (TODO: Amy to confirm that this is ok)
    c) applying a set of regexps to do data cleanup (remove plural forms)
    d) moving all items that are not standard DC types to the physical format field (http://dublincore.org/documents/resource-typelist/)
    
    By default works on the 'type' field, but can be overridden by passing the name of the field to use
    as a parameter
    """

    REGEXPS = ('images','image'), ('still image','image')
    DC_TYPES = ['collection', 'dataset', 'event', 'image', 'still image', 'interactive resource', 'model', 'party', 'physical object',
                'place', 'service', 'software', 'sound', 'text']

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
        v = getprop(data,prop)
        dctype = []
        physicalFormat = getprop(data,alternate) if exists(data,alternate) else []

        for s in (v if not isinstance(v,basestring) else [v]):
            dctype.append(cleanup(s)) if is_dc_type(cleanup(s)) else physicalFormat.append(s)

        if dctype:
            setprop(data,prop,dctype[0]) if len(dctype) == 1 else setprop(data,prop,dctype)
        else:
            setprop(data,prop,None)
        if physicalFormat:
            setprop(data,alternate,physicalFormat[0]) if len(physicalFormat) == 1 else setprop(data,alternate,physicalFormat)

    return json.dumps(data)
