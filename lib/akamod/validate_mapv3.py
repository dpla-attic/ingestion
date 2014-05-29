from amara.thirdparty import json
from akara.services import simple_service
from akara import response
from akara import logger
from dplaingestion.selector import getprop, delprop
from dplaingestion.mapv3 import MAPV3
from jsonschema import validate 
from jsonschema.exceptions import ValidationError

@simple_service("POST", "http://purl.org/la/dp/validate_mapv3",
                "validate_mapv3", "application/json")
def validatemapv3(body, ctype):
    """
    Service that accepts a JSON document and validates it against the
    DPLA Metadata Application Profile v3 JSON Schema.
    """

    # TODO: Send GET request to API once schema endpoint is created

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header("content-type", "text/plain")
        return "Unable to parse body as JSON"

    try:
        validate(data, MAPV3)
        valid = True
    except ValidationError as err:
        valid = False
    finally:
        if "admin" in data:
            data["admin"]["valid_after_enrich"] = valid
        else:
            data["admin"] = {"valid_after_enrich": valid}
        print data 
        return json.dumps(data)
