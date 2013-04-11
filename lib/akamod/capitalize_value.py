from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

from dplaingestion.selector import getprop, setprop, exists


# Below fields should have removed do at the end.
DEFAULT_PROP = [
    "sourceResource/language",
    "sourceResource/title",
    "sourceResource/rights",
    "sourceResource/creator",
    "sourceResource/relation",
    "sourceResource/publisher",
    "sourceResource/subject",
    "sourceResource/description",
    "sourceResource/collection/title",
    "sourceResource/contributor",
    "sourceResource/extent",
    "sourceResource/format",
    # "sourceResource/spatial/currentLocation",  # State Located In
    # "sourceResource/spatial",  # place name?
    "dataProvider",
    "provider/name"
]


def capitalize(data, prop):
    """
    Capitalizes the value of the related property path.
    Modifies given dictionary (data argument).
    """
    def str_capitalize(s):
        """
        Changes the first letter of the string into uppercase.
        python "aaa".capitalize() can be used, other words first letters
        into lowercase.
        """
        if s:
            return s[0].upper() + s[1:]
        return s

    if exists(data, prop):
        v = getprop(data, prop, keyErrorAsNone=True)
        if v:
            if isinstance(v, basestring):
                setprop(data, prop, str_capitalize(v))
            elif isinstance(v, list):
                new_v = []
                for s in v:
                    if isinstance(s, basestring):
                        new_v.append(str_capitalize(s))
                    else:
                        new_v.append(s)
                setprop(data, prop, new_v)


@simple_service('POST', 'http://purl.org/la/dp/capitalize_value', 'capitalize_value', 'application/json')
def capitalize_value(body, ctype, prop=",".join(DEFAULT_PROP)):
    """
    Service that accepts a JSON document and capitalizes the prop field of that document
    """

    if prop is None:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        msg = "Prop param is None"
        logger.error(msg)
        return msg

    try:
        data = json.loads(body)
    except Exception as e:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON\n" + str(e)

    for p in prop.split(","):
        if p:
            capitalize(data, p)

    return json.dumps(data)
