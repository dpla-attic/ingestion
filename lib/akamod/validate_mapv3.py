from amara.thirdparty import json
from akara.services import simple_service
from akara import response
from akara import logger
from dplaingestion.selector import getprop, delprop
from dplaingestion.mapv3 import MAPV3_SCHEMAS
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
        id_for_msg = data.get('_id', '[no id]')
    except:
        response.code = 500
        response.add_header("content-type", "text/plain")
        return "Unable to parse body as JSON"

    valid = None
    validation_message = None

    try:
        ingest_type = data.get('ingestType', None)
        if ingest_type is not None:
            validate(data, MAPV3_SCHEMAS[ingest_type])
            valid = True
        else:
            logger.warning('Could not get ingestType for record with id %s; refusing to validate.' % id_for_msg)
    except ValidationError as err:
        valid = False
        logger.warning('Could not validate %s record with id %s: %s' % (ingest_type, id_for_msg, err.message))
        validation_message = err.message

    if "admin" in data:
        data["admin"]["valid_after_enrich"] = valid
        data["admin"]["validation_message"] = validation_message
    else:
        data["admin"] = {
            "valid_after_enrich": valid,
            "validation_message": validation_message
        }
    return json.dumps(data)
