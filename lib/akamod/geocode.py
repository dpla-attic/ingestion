from akara import logger

from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from zen.akamod import geolookup_service
from dplaingestion.selector import getprop, setprop, exists

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
    Service that annotates an inbound JSON document with the lat/long for a
    given property of that document. If newprop is not specified, the property
    is overwritten with the lat/long, otherwise a new property is added. Both
    strings and list of strings are supported.
    '''   

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    if exists(data,prop):
        if not newprop:
            newprop = prop

        val = getprop(data,prop)
        if isinstance(val,list):
            setprop(data,newprop,[ lookup_place(place) for place in val ])
        else:
            setprop(data,newprop,lookup_place(val))

    return json.dumps(data)
