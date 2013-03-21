from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, delprop, exists

@simple_service('POST', 'http://purl.org/la/dp/uiuc_publisher_to_dataprovider',
                'uiuc_publisher_to_dataprovider', 'application/json')
def uiuc_publisher_to_dataprovider(body, ctype,
                                   publisher_field="sourceResource/publisher"):
    """
    Copies the publisher field to dataProvider if dataProvider is not set, then
    removes publisher.
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if not exists(data, "dataProvider") and  exists(data, publisher_field):
        publisher = getprop(data, publisher_field)

        setprop(data, "dataProvider", publisher)
        delprop(data, publisher_field)

    return json.dumps(data)
