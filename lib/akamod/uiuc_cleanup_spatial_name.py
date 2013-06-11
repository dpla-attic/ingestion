from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
import re

@simple_service('POST', 'http://purl.org/la/dp/uiuc_cleanup_spatial_name',
                'uiuc_cleanup_spatial_name', 'application/json')
def uiuc_cleanup_spatial_name(body, ctype, action="uiuc_cleanup_spatial_name",
                              prop="sourceResource/spatial", key="name"):
    '''
    Service that accepts a JSON document and cleans the each
    sourceResource/spatial/name value by:

    a) splitting on "Victorian,?", "Modern", "Contemporary", "A.D."
    b) stripping the temporal and spatial data
    c) moving the temporal data to the sourceResource/temporal field
    '''

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    REGEXPS = ["(Victorian,?)", "(Contemporary,?)", "(Modern,?)", "(A\.D\.,?)"]

    temporal_field = "sourceResource/temporal"

    if exists(data, prop):
        t = getprop(data, temporal_field, True)
        if not t:
            t = []
        v = getprop(data, prop)
        for d in (v if isinstance(v, list) else [v]):
            if key in d:
                for pattern in REGEXPS:
                    if re.search(pattern, d[key]):
                        s = re.split(pattern, d[key])
                        temporal = "%s%s".strip() % (s[0], s[1])
                        t.append(re.sub(",$", "", temporal))
                        d[key] = s[-1]
                d[key] = d[key].strip()
        setprop(data, prop, v)
        if t:
            setprop(data, temporal_field, t)

    return json.dumps(data)
