import re

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

from dplaingestion import selector

@simple_service('POST', 'http://purl.org/la/dp/spatial_dates_to_temporal', 'spatial_dates_to_temporal', 'application/json')
def spatialdatestotemporal(body,ctype,action="spatial_dates_to_temporal",prop="aggregatedCHO/spatial"):
    """
    Service that accepts a JSON document and moves any dates found in the spatial field to the
    temporal field.
    """

    REGEXPS = [" *\d{4}", "( *\d{1,4} *[-/]){2} *\d{1,4}"]
    SOURCE_RESOURCES_KEY = "aggregatedCHO"

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if SOURCE_RESOURCES_KEY in data and selector.exists(data, prop):
        sp = []
        if "temporal" in data[SOURCE_RESOURCES_KEY]:
            temp = data[SOURCE_RESOURCES_KEY]["temporal"]
        else:
            temp = [] 
        for d in selector.getprop(data, prop):
            for pattern in REGEXPS:
                m = re.match(pattern, d["name"])
                if m:
                    # Append to data["temporal")
                    temp.append(d)
                    break
            if d not in temp:
                # Append to sp, which will rewrite data[prop]
                sp.append(d)

        if temp:
            data[SOURCE_RESOURCES_KEY]["temporal"] = temp
        selector.setprop(data, prop, sp)

    return json.dumps(data)
