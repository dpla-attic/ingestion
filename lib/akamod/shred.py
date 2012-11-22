from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

@simple_service('POST', 'http://purl.org/la/dp/shred', 'shred', 'application/json')
def shred(body,ctype,action="shred",prop=None,delim=','):
    '''   
    Service that accepts a JSON document and "shreds" or "unshreds" the value
    of the field(s) named by the "prop" parameter

    "prop" can include multiple property names, delimited by a comma (the delim
    property is used only for the fields to be shredded/unshredded). This requires
    that the fields share a common delimiter however.
    '''   
    
    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    for p in prop.split('.'):
        if p in data:
            if action == "shred":
                if type(data[p]) == list:
                    data[p] = delim.join(data[p])
                if delim not in data[p]: continue
                data[p] = [ s.strip() for s in data[p].split(delim) ]
            elif action == "unshred":
                if type(data[p]) == list:
                    data[p] = delim.join(data[p])

    return json.dumps(data)
