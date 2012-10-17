from akara import response
from akara.services import simple_service
from amara.thirdparty import json

@simple_service('POST', 'http://purl.org/la/dp/unshred', 'unshred', 'application/json')
def unshred(body,ctype,prop=None):
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

    if prop in data:
        data[prop] = ','.join(data[prop])

    return json.dumps(data)
