from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from dplaingestion.utilities import iterify
import re

@simple_service('POST', 'http://purl.org/la/dp/scdl_format_to_type',
                'scdl_format_to_type', 'application/json')
def scdlformattotype(body, ctype, action="scdl_format_to_type",
                       prop="sourceResource/format",
                       type_field="sourceResource/type"):
    """
    Service that accepts a JSON document and sets the sourceResource/type field
    based on the value of the sourceResource/format field. 
    """

    FORMAT_TO_TYPE = {
        "mp3": "sound",
        "art": "image",
        "map": "image",
        "image": "image",
        "video": "image",
        "object": "image",
        "visual": "image",
        "text": "text",
        "paper": "text",
        "edition": "text",
        "pamphlet": "text",
        "newspaper": "text",
        "manuscript": "text",
        "typescript": "text",
        "scrapbook": ["image", "text"],
        "miscellaneous": ["image", "text"],
    }

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop):
        types = iterify(data["sourceResource"].get("type", []))
        for format_value in iterify(getprop(data, prop)):
            for f, t in FORMAT_TO_TYPE.items():
                if re.search("\\b%s(s)?\\b" % f, format_value, re.I):
                    types.extend(iterify(t))

        if types:
            setprop(data, type_field, list(set(types)))

    return json.dumps(data)
