import hashlib
from amara.thirdparty import json
from amara.lib.iri import is_absolute
from akara.services import simple_service
from akara.util import copy_headers_to_dict
from akara import request, response
from akara import logger
from dplaingestion.selector import getprop, setprop, exists

COUCH_ID_BUILDER = lambda src, lname: "--".join((src,lname))
COUCH_REC_ID_BUILDER = lambda src, id_handle: COUCH_ID_BUILDER(src,
                                                               CLEAN_ID(id_handle))
CLEAN_ID = lambda id_handle: id_handle.strip().replace(" ","__")

@simple_service('POST', 'http://purl.org/la/dp/select-id', 'select-id',
                'application/json')
def selid(body, ctype, prop='handle', use_source='yes'):
    '''   
    Service that accepts a JSON document and adds or sets the "id" property to
    the value of the property named by the "prop" paramater
    '''   
    
    if not prop:
        # Remove this document
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "No id property has been selected"

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    request_headers = copy_headers_to_dict(request.environ)
    source_name = request_headers.get('Source')

    id = None
    if exists(data,prop):
        v = getprop(data,prop)
        if isinstance(v,basestring):
            id = v
        else:
            if v:
                for h in (v if isinstance(v, list) else [v]):
                    if is_absolute(h):
                        id = h
                if not id:
                    id = v[0]

    if not id:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "No id property was found"

    '''
    If the useSource parameter is True (default) than prepend it to
    the id and use that value when hashing for the DPLA id
    '''
    if use_source == 'yes':
        data[u'_id'] = COUCH_REC_ID_BUILDER(source_name, id)
    else:
        data[u'_id'] = CLEAN_ID(id)

    data[u'id'] = hashlib.md5(data[u'_id']).hexdigest()

    return json.dumps(data)
