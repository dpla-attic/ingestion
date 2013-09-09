import hashlib
from amara.thirdparty import json
from amara.lib.iri import is_absolute
from akara.services import simple_service
from akara.util import copy_headers_to_dict
from akara import request, response
from akara import logger
from dplaingestion.selector import getprop, setprop, exists

COUCH_ID_BUILDER = lambda src, lname: "--".join((src,lname))
COUCH_REC_ID_BUILDER = lambda src, id_handle: COUCH_ID_BUILDER(src,id_handle.strip().replace(" ","__"))

@simple_service('POST', 'http://purl.org/la/dp/edan_select_id', 'edan_select_id', 'application/json')
def selid(body,ctype,prop='descriptiveNonRepeating/record_link', alternative_prop='descriptiveNonRepeating/record_ID'):
    '''   
    Service that accepts a JSON document and adds or sets the "id" property to the
    value of the property named by the "prop" paramater
    '''   
    tmpl="http://collections.si.edu/search/results.htm?q=record_ID%%3A%s&repo=DPLA"
    
    if prop:
        try :
            data = json.loads(body)
        except:
            response.code = 500
            response.add_header('content-type','text/plain')
            return "Unable to parse body as JSON"

        request_headers = copy_headers_to_dict(request.environ)
        source_name = request_headers.get('Source')

        id = None

        if exists(data, prop) or exists(data, alternative_prop):
            v = getprop(data,prop, True)
            if not v:
                v = getprop(data, alternative_prop)
                v = tmpl % v
            if isinstance(v,basestring):
                id = v
            else:
                if v:
                    for h in v:
                        if is_absolute(h):
                            id = h
                    if not id:
                        id = v[0]

        if not id:
            response.code = 500
            response.add_header('content-type','text/plain')
            return "No id property was found"

        data[u'_id'] = COUCH_REC_ID_BUILDER(source_name, id)
        data[u'id']  = hashlib.md5(data[u'_id']).hexdigest()
    else:
        logger.error("Prop param in None in %s" % __name__)

    return json.dumps(data)
