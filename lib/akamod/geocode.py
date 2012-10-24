from akara import logger

from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from zen.akamod import geolookup_service

GEOLOOKUP = geolookup_service()

def lookup_place(p):
    lu = GEOLOOKUP(p)
    if p in lu:
        return lu[p]
    else:
        return ""

@simple_service('POST', 'http://purl.org/la/dp/geocode', 'geocode', 'application/json')
def geocode(body,ctype,prop=None,newprop=None):
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

    if prop not in data:
        return json.dumps(data) # graceful abort

    if not newprop:
        newprop = prop

    if hasattr(data[prop],'__iter__'): # Handle strings and iterables
        data[newprop] = [ lookup_place(place) for place in data[prop] ]
    else:
        data[newprop] = lookup_place(data[prop])

    return json.dumps(data)
