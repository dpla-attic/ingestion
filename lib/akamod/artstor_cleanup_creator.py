from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

from dplaingestion.selector import getprop, setprop, exists
import re


HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'

CLEANUP = (
    "And", "Artist:", "Author:", "Binder:", "Drawn by", "Illuminator:", "Or",
    "Scribe:", "Resolve"
    )

@simple_service('POST', 'http://purl.org/la/dp/artstor_cleanup_creator',
                'artstor_cleanup_creator', HTTP_TYPE_JSON)
def artstor_cleanup_creator(body, ctype, prop="sourceResource/creator"):
    """
    Service that accepst a JSON document and removes cleans the
    sourceResource/creator field by removing the values in REGEXES if the
    field value begins with them
    """

    try:
        assert ctype.lower() == HTTP_TYPE_JSON, "%s is not %s" % (HTTP_HEADER_TYPE, HTTP_TYPE_JSON)
        data = json.loads(body)
    except Exception as e:
        error_text = "Bad JSON: %s: %s" % (e.__class__.__name__, str(e))
        logger.exception(error_text)
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return error_text

    if exists(data, prop):
        item = getprop(data, prop)
        if not isinstance(item, list):
            item = [item]
        for i in range(len(item)):
            for s in CLEANUP:
                item[i] = re.sub(r"(?i)^{0}".format(s), "", item[i].strip()).lstrip()
            
        setprop(data, prop, item[0] if len(item) == 1 else item)

    return json.dumps(data)
