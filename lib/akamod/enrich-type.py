from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
import re

@simple_service('POST', 'http://purl.org/la/dp/enrich-type', 'enrich-type', 'application/json')
def enrichtype(body,ctype,action="enrich-type",prop="type", alternate="TBD_physicalformat"):
    '''   
    Service that accepts a JSON document and enriches the "type" field of that document
    by: 

    a) making the type lowercase
    b) converting "image" to "still image" (TODO: Amy to confirm that this is ok)
    c) applying a set of regexps to do data cleanup (remove plural forms)
    d) moving all items that are not standard DC types to the physical format field (http://dublincore.org/documents/resource-typelist/)
    
    By default works on the 'type' field, but can be overridden by passing the name of the field to use
    as a parameter
    '''   

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
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    if prop in data:
        dctype = []
        physicalFormat = []
        for s in (data[prop] if not isinstance(data[prop],basestring) else [data[prop]]):
            dctype.append(cleanup(s)) if is_dc_type(cleanup(s)) else physicalFormat.append(s)

        if len(dctype) > 0:
            data[prop] = dctype[0] if len(dctype) == 1 else dctype
        else:
            del data[prop]
        if len(physicalFormat) > 0:
            data[alternate] = physicalFormat[0] if len(physicalFormat) == 1 else physicalFormat

    return json.dumps(data)
