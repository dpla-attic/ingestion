from akara import response
from akara.services import simple_service
from amara.thirdparty import json

@simple_service('POST', 'http://purl.org/la/dp/select-id', 'select-id', 'application/json')
def selid(body,ctype,prop=None):
    '''   
    Service that accepts a JSON document and adds or sets the "id" property to the
    value of the property named by the "prop" paramater
    '''   
    
    if not prop:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "No id property has been selected"

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    data[u'id'] = data.get(prop,None)

    return json.dumps(data)
