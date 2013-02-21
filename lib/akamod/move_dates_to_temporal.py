from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, delprop, exists
import re

@simple_service('POST', 'http://purl.org/la/dp/move_dates_to_temporal', 'move_dates_to_temporal', 'application/json')
def movedatestotemporal(body,ctype,action="move_dates_to_temporal",prop=None):
    """
    Service that accepts a JSON document and moves any dates found in the prop field to the
    temporal field.
    """

    if not prop:
        logger.error("No prop supplied")
        return body

    REGSUB = ("\(", ""), ("\)", "")
    REGSEARCH = ["(\( *)?(\d{1,4} *[-/] *\d{1,4} *[-/] *\d{1,4})( *\))?", "(\( *)?(\d{4} *[-/] *\d{4})( *\))?", "(\( *)?(\d{4})( *\))?"]

    def cleanup(s):
        for p,r in REGSUB:
            s = re.sub(p,r,s)
        return s.strip()

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop):
        p = []
        temporal_field = "aggregatedCHO/temporal"
        temporal = getprop(data, temporal_field) if exists(data, temporal_field) else []

        for d in getprop(data, prop):
            for regsearch in REGSEARCH:
                pattern = re.compile(regsearch)
                for match in pattern.findall(d["name"]):
                    m = "".join(match)
                    #TODO (\( *)? matches 0 and produces '' in m
                    if m:
                        d["name"] = re.sub(re.escape(m),"",d["name"])
                        temporal.append({"name": cleanup(m)})
            if d["name"].strip():
                # Append to p, which will overwrite data[prop]
                p.append(d)

        if temporal:
            setprop(data, temporal_field, temporal)
        if p:
            setprop(data, prop, p)
        else:
            delprop(data, prop)

    return json.dumps(data)
