from akara import logger, response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.create_mapper import create_mapper

@simple_service('POST', 'http://purl.org/la/dp/dpla_mapper', 'dpla_mapper',
                'application/json')
def dpla_mapper(body, ctype, type=None):
    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if not type:
        logger.error("No type was supplied to dpla_mapper.")
    else:
        mapper = create_mapper(type, data)
        mapper.map()
        data = mapper.mapped_data

    return json.dumps(data)
