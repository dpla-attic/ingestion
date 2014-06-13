from akara import response
from amara.thirdparty import json
from akara.services import simple_service
from dplaingestion.selector import setprop

@simple_service('POST', 'http://purl.org/la/dp/set_context',
                'set_context', 'application/json')
def setcontext(body, ctype, prop="@context"):
    """   
    Service that accepts a JSON document and sets the "@context" field of that
    document.
    """

    try :
        data = json.loads(body)
    except Exception:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    item_context = {
        "@id": "http://dp.la/api/items/%s" % data["id"],
        "@context": "http://dp.la/api/items/context",
        "aggregatedCHO": "#sourceResource",
        "@type": "ore:Aggregation",
        "sourceResource": {
           "@id": "http://dp.la/api/items/%s#sourceResource" % data["id"]
        }
    }

    collection_context = {
        "@context": "http://dp.la/api/collections/context",
        "@id": "http://dp.la/api/collections/%s" % data["id"],
        "@type": "dcmitype:Collection" 
    }

    if data["ingestType"] == "item":
        setprop(data, prop, item_context)
    else:
        setprop(data, prop, collection_context)

    return json.dumps(data)
