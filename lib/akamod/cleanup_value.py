from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
import re


def convert(data, prop):
    """Converts a property.

    Arguments:
        data - dictionary with JSON
        prop - property name

    Returns:
        Nothing, the dictionary is changed in place.
    """
    if exists(data, prop):
        v = getprop(data, prop)
        if isinstance(v, basestring):
            setprop(data, prop, cleanup(v))
        elif isinstance(v, list):
            temp = []
            for val in v:
                temp.append(cleanup(val))
            setprop(data, prop, temp)


def cleanup(value):
    """ Performs a cleanup of value using a bunch of regexps.

    Arguments:
        value - string for convertion

    Returns:
        Converted string.
    """
    TAGS_FOR_STRIPPING = '[\.\' ";]*' # Tags for stripping at beginning and at the end.
    REGEXPS = (' *-- *', '--'), \
              ('^' + TAGS_FOR_STRIPPING, ''), \
              (TAGS_FOR_STRIPPING + '$', '')

    value = value.strip()
    for pattern, replace in REGEXPS:
        value = re.sub(pattern, replace, value)
    return value


@simple_service('POST', 'http://purl.org/la/dp/cleanup_value', 'cleanup_value', 'application/json')
def cleanup_value(body, ctype, action="cleanup_value", prop=None):
    '''
    Service that accepts a JSON document and enriches the prop field of that document by:

    a) applying a set of regexps to do data cleanup
    '''

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
