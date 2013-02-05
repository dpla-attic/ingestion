from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
import re

@simple_service('POST', 'http://purl.org/la/dp/enrich-format', 'enrich-format', 'application/json')
def enrichformat(body,ctype,action="enrich-format",prop="format",alternate="TBD_physicalformat"):
    """
    Service that accepts a JSON document and enriches the "format" field of that document
    by: 

    a) setting the format to be all lowercase
    b) running through a set of cleanup regex's (e.g. image/jpg -> image/jpeg)
    c) checking to see if the field is a valid IMT, and moving it to a separatee field if not
       See http://www.iana.org/assignments/media-types for list of valid media-types. We do not
       require that a subtype be defined. 
    d) Remove any extra text after the IMT   
    
    By default works on the 'format' field, but can be overridden by passing the name of the field to use
    as the 'prop' parameter. Non-IMT's are moved the field defined by the 'alternate' parameter.
    """

    REGEXPS = ('image/jpg','image/jpeg'),('image/jp$', 'image/jpeg'), ('img/jpg', 'image/jpeg'), ('\W$','')
    IMT_TYPES = ['application','audio','image','message','model','multipart','text','video']

    def cleanup(s):
        s = s.lower().strip()
        for pattern, replace in REGEXPS:
            s = re.sub(pattern, replace, s)
            s = re.sub(r"^([a-z0-9/]+)\s.*",r"\1",s)
        return s

    def is_imt(s):
        imt_regexes = [re.compile('^' + x + '(/|\Z)') for x in IMT_TYPES]
        return any(regex.match(s) for regex in imt_regexes)

    try :
        data = json.loads(body)
    except Exception:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    if prop in data:
        format = []
        physicalFormat = list(data[alternate]) if alternate in data else []
        for s in (data[prop] if not isinstance(data[prop],basestring) else [data[prop]]):
            format.append(cleanup(s)) if is_imt(cleanup(s)) else physicalFormat.append(s)

        if format:
            data[prop] = format[0] if len(format) == 1 else format
        else:
            del data[prop]
        if physicalFormat:
            data[alternate] = physicalFormat[0] if len(physicalFormat) == 1 else physicalFormat

    return json.dumps(data)
