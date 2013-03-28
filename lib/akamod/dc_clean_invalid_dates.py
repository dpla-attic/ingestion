from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists, delprop
import re


def check_date_dict(v):

    if not isinstance(v, dict):
        return False

    return 'begin' in v and v["begin"] and 'end' in v and v["end"]


def convert(data, prop):
    value = getprop(data, prop)

    if isinstance(value, list):
        values = []
        for v in value:
            if check_date_dict(v):
                values.append(v)

        if values:
            setprop(data, prop, values)
        else:
            delprop(data, prop)

    elif not check_date_dict(value):
        delprop(data, prop)


@simple_service('POST', 'http://purl.org/la/dp/dc_clean_invalid_dates', 'dc_clean_invalid_dates', 'application/json')
def dc_clean_invalid_dates(body, ctype, action="cleanup_value", prop="sourceResource/date"):

    if prop is None:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        msg = "Prop param is None"
        logger.error(msg)
        return msg

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    for p in prop.split(","):
        convert(data, p)

    return json.dumps(data)
