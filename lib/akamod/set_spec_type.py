from akara import response
from amara.thirdparty import json
from akara.services import simple_service
from dplaingestion.utilities import iterify
from dplaingestion.selector import getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/set_spec_type',
                'set_spec_type', 'application/json')
def setspectype(body, ctype, prop="sourceResource/type"):
    """   
    Service that accepts a JSON document and sets the "sourceResource/specType"
    field of that document from the prop field
    """

    try :
        data = json.loads(body)
    except Exception:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    TYPE_TO_SPEC_TYPE = {
        "book": "Book",
        "government": "Government Document",
        "periodical": "Serial"
    }

    if exists(data, prop):
        spec_type = []
        for s in iterify(getprop(data, prop)):
            for k, v in TYPE_TO_SPEC_TYPE.items():
                if k in s.lower() and v not in spec_type:
                    spec_type.append(v)

            if spec_type:
                setprop(data, "sourceResource/specType", spec_type)

    return json.dumps(data)
