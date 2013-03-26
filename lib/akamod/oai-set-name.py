import sys
from akara.services import simple_service
from akara import request, response
from akara import logger
from amara.lib.iri import is_absolute
from amara.thirdparty import json, httplib2

@simple_service('POST', 'http://purl.org/la/dp/oai-set-name', 'oai-set-name', 'application/json')
def oaisetname(body,ctype,sets_service=None):
    '''   
    Service that accepts a JSON document and sets the "name" property based on looking up
    the set in the HTTP_CONTEXT using the service passed in the 'sets_service' parameter.
    Assumes that the set_service returns a JSON array of two-element arrays, where the first
    element is the id and the second element the complete name.
    '''   
    
    if not sets_service:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "No set service has been selected"

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    if not is_absolute(sets_service):
        prefix = request.environ['wsgi.url_scheme'] + '://' 
        prefix += request.environ['HTTP_HOST'] if request.environ.get('HTTP_HOST') else request.environ['SERVER_NAME']
        sets_service = prefix + sets_service
        
    H = httplib2.Http('/tmp/.cache')
    H.force_exception_as_status_code = True
    resp, content = H.request(sets_service)
    if not resp[u'status'].startswith('2'):
         print >> sys.stderr, '  HTTP error ('+resp[u'status']+') resolving URL: '+sets_service

    try :
        sets = json.loads(content)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse sets service result as JSON: " + repr(content)

    for s in sets:
        if s['setSpec'] in data[u'_id']:
            data[u'title'] = s['setName']
            if s['setDescription']:
                data[u'description'] = s['setDescription']
                break

    return json.dumps(data)
