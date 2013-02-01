from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

@simple_service('POST', 'http://purl.org/la/dp/lookup', 'lookup', 'application/json')
def lookup(body, ctype, input_field, output_field, substitution):
    """
    Performs simple lookup.
    """

    logger.debug("BODY  : [%s]"%body)
    logger.debug("INPUT : [%s]"%input_field)
    logger.debug("OUTPUT: [%s]"%output_field)

    LOG_JSON_ON_ERROR = True
    def log_json():
        if LOG_JSON_ON_ERROR:
            logger.debug(body)

    data = {}
    try:
        data = json.loads(body)
    except Exception as e:
        msg = "Bad JSON: " + e.args[0]
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

    if not output_field:
        msg = "There is not provided output field name."
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

    key = substitution.upper()
    if not key in globals():
        msg = "Missing substitution dictionary"
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

    if not input_field in data:
        logger.error("Missing input key in provided JSON.")
        return json.dumps(data)

    d = globals()[key]
    value = data[input_field]
    logger.debug("Changing value of ['{0}':'{1}'] to {2}".format(input_field, value, d[value]))

    data[output_field] = d[value]

    return json.dumps(data)


TEST_SUBSTITUTE = {
    "bbb" : "BBB"
        }
