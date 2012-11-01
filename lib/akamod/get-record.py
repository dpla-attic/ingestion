import sys
from akara.services import simple_service
from akara import response
from amara.thirdparty import json, httplib2

@simple_service('POST', 'http://purl.org/la/dp/get-record', 'get-record', 'application/json')
def getrecord(body,ctype,couch=None):
    '''
    Retrieve a record from CouchDB based on an id. Can be used in the pipeline after submitting
    to CouchDB.
    '''

    if not couch:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "CouchDB url not provided"

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    H = httplib2.Http('/tmp/.pollcache')
    H.force_exception_as_status_code = True
    resp, content = H.request(couch+data['id'])
    if not (resp[u'status'].startswith('2') or resp[u'status'] == '304'):
         print >> sys.stderr, '  HTTP error ('+resp[u'status']+') resolving URL: '+couch+data['id']

    return content
