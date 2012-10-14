from akara import logger

from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from zen.akamod import geolookup_service

geolookup = geolookup_service()

@simple_service('POST', 'http://purl.org/la/dp/geocode', 'geocode', 'application/json')
def geocode(body,ctype,prop=None):
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

    
    data[prop] = geolookup(data[prop])[data[prop]]

    return json.dumps(data)
