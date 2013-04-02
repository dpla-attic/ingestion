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
            setprop(data, prop, cleanup(v, prop))
        elif isinstance(v, list):
            temp = []
            for val in v:
                temp.append(cleanup(val, prop))
            setprop(data, prop, temp)


def cleanup(value, prop):
    """ Performs a cleanup of value using a bunch of regexps.

    Arguments:
        value - string for convertion

    Returns:
        Converted string.
    """
    # Do not remove double quotes from title
    dquote = '' if prop == "sourceResource/title" else '"'

    # Remove dot at the end if field name is not in the
    # DONT_STRIP_DOT_END table.
    with_dot = '' if prop in DONT_STRIP_DOT_END else "\."
    # Tags for stripping at beginning and at the end.

    TAGS_FOR_STRIPPING = '[%s\' \r\t\n;,%s]*'

    TAGS_FOR_STRIPPING_AT_BEGIN = TAGS_FOR_STRIPPING % ("\.", dquote)
    TAGS_FOR_STRIPPING_AT_END = TAGS_FOR_STRIPPING % (with_dot, dquote)

    REGEXPS = (' *-- *', '--'), \
              ('[\t ]{2,}', ' '), \
              ('^' + TAGS_FOR_STRIPPING_AT_BEGIN, ''), \
              (TAGS_FOR_STRIPPING_AT_END + '$', '')

    if isinstance(value, basestring):
        value = value.strip()
        for pattern, replace in REGEXPS:
            value = re.sub(pattern, replace, value)

    return value

"""
Fields which should not be changed:
-- format (there are often dimensions in this field)
-- extent (for the same reason)
-- descriptions (full text, includes sentences)
-- rights (full text, includes sentences)
-- place (may end in an abbreviated state name)

"""
DONT_STRIP_DOT_END = [
    "hasView/format",
    "sourceResource/format",
    "sourceResource/extent",
    "sourceResource/description",
    "sourceResource/rights",
    "sourceResource/place",
]

# Below fields should have removed do at the end.
DEFAULT_PROP = [
    "sourceResource/language",
    "sourceResource/title",
    "sourceResource/creator",
    "sourceResource/relation",
    "sourceResource/publisher",
    "sourceResource/subject",
    "sourceResource/date",
    "sourceResource/description",
    "sourceResource/collection/title",
    "sourceResource/collection/description"
]


@simple_service('POST', 'http://purl.org/la/dp/cleanup_value', 'cleanup_value', 'application/json')
def cleanup_value(body, ctype, action="cleanup_value", prop=",".join(DEFAULT_PROP + DONT_STRIP_DOT_END)):
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
