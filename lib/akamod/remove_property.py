from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import delprop


@simple_service(
    'POST',
    'http://purl.org/la/dp/remove_property',
    'remove_property',
    'application/json')

def remove_property(
        body,
        ctype,
        prop,
        action="remove_property",
        ):

    try:
        data = json.loads(body)
    except Exception, err:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON: " + str(err)

    delprop(data, prop, True)

    return json.dumps(data)
