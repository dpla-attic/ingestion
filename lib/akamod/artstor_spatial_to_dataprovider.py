from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, delprop, exists

@simple_service('POST',
                'http://purl.org/la/dp/artstor_spatial_to_dataprovider',
                'artstor_spatial_to_dataprovider', 'application/json')
def artstor_spatial_to_dataprovider(body, ctype,
                                    prop="sourceResource/spatial"):
    """ Splits spatial on semicolon and copies the first value to dataProvider
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop):
        v = getprop(data, prop)
        if isinstance(v, list):
            v = v[0]
        if isinstance(v, basestring):
            v = v.split(";")[0]    
            setprop(data, "dataProvider", v)
        delprop(data, prop)

    return json.dumps(data)
