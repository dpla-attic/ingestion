from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
import re

@simple_service("POST", "http://purl.org/la/dp/set_type_from_physical_format",
                "set_type_from_physical_format", "application/json")
def set_type_from_physical_format(body, ctype, prop="sourceResource/type"):
    """ 
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header("content-type", "text/plain")
        return "Unable to parse body as JSON"

    TYPES = {
        "interactive resource": ["online exhibit"],
        "collection":           ["online collection", "finding aid"],
        "sound":                ["sound recording", "audio",
                                 "oral history recording"],
        "text":                 ["publication", "journal", "magazine", "book",
                                 "letter", "transcript"],
        "image":                ["photograph", "etching", "image", "print",
                                 "illumination", "still image", "painting",
                                 "drawing"],
        "physical object":      ["container", "jewelry", "frame", "object",
                                 "textile", "costume", "statue", "furniture",
                                 "sculpture", "furnishing"]
    }

    if not exists(data, prop) and exists(data, "sourceResource/format"):
        type = []
        format = getprop(data, "sourceResource/format")
        for f in (format if isinstance(format, list) else [format]):
            f = re.sub("s$", "", f).lower()
            for k, v in TYPES.items():
                if set([f]) & set(v) and k not in type:
                    type.append(k)

        if type:
            setprop(data, prop, type)
                

    return json.dumps(data)

