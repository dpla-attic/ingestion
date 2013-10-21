from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop
from dplaingestion.utilities import iterify

@simple_service('POST', 'http://purl.org/la/dp/usc_set_dataprovider',
                'usc_set_dataprovider', 'application/json')
def uscsetdataprovider(body, ctype, prop="dataProvider"):
    """   
    Service that accepts a JSON document and sets the "dataProvider"
    field of that document to:

    1. The first value of the originalRecord/source field (placed in
       dataProvider in the oai-to-dpla module) for the chs set (setSpec
       p15799coll65)
    2. The string "University of Southern California. Libraries" for all
       other sets

    For primary use with USC documents
    """

    try :
        data = json.loads(body)
    except Exception:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"


    data_provider = getprop(data, "dataProvider", True)
    if getprop(data, "originalRecord/setSpec") == "p15799coll65":
        setprop(data, "dataProvider", data_provider[0])
    else:
        setprop(data, "dataProvider",
                "University of Southern California. Libraries")

    return json.dumps(data)
