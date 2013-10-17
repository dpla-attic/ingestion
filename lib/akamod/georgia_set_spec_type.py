from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop
from dplaingestion.utilities import iterify

@simple_service('POST', 'http://purl.org/la/dp/georgia_set_spec_type',
                'georgia_set_spec_type', 'application/json')
def georgiasetspectype(body, ctype):
    """   
    Service that accepts a JSON document and sets the "sourceResource/specType"
    field of that document from the "sourceResource/type" field

    For primary use with DLG documents
    """

    try :
        data = json.loads(body)
    except Exception:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    TYPE_TO_SPEC_TYPE = {
        "books": "Book",
        "government": "Government Document",
        "periodicals": "Serial"
    }

    type = getprop(data, "sourceResource/type", True)
    if type:
        spec_type = []
        for s in iterify(type):
            for k, v in TYPE_TO_SPEC_TYPE.items():
                if k in s.lower() and v not in spec_type:
                    spec_type.append(v)

        if spec_type:
            setprop(data, "sourceResource/specType", spec_type)

    return json.dumps(data)
