from akara import response
from akara.services import simple_service
from amara.thirdparty import json

@simple_service('POST', 'http://purl.org/la/dp/shred', 'shred', 'application/json')
def shred(body,ctype,prop=None):
    '''   
    Service that accepts a JSON document and "unshreds" the value of the
    field named by the "prop" parameter
    '''   
    
    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    data[prop] = [ s.strip() for s in data[prop].split(',') ]

    return json.dumps(data)
