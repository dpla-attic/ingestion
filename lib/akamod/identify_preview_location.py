from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

@simple_service('POST', 'http://purl.org/la/dp/identify_preview_location', 'identify_preview_location', 'application/json')
def identify_preview_location(body, ctype):
    """
    Responsible for: adding a field to a document with the URL where we should 
    expect to the find the thumbnail
    """

    LOG_JSON_ON_ERROR = True
    def log_json():
        if LOG_JSON_ON_ERROR:
            logger.debug(body)

    
    data = {}
    try:
        data = json.loads(body)
    except Exception as e:
        msg = "Bad JSON: " + e.args[0]
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

    if not data.has_key(u"object"):
        logger.error("There is no 'object' key in JSON for doc [%s]." % data[u'id'])
        log_json()
        return body

    url = data[u'object']
    logger.debug("object = " + url)
    URL_FIELD_NAME = u"preview_source_url"
    p = url.split("u?")

    if len(p) != 2:
        logger.error("Bad URL %s. It should have just one 'u?' part." % url)
        log_json()
        return body

    (base_url, rest) = p

    if base_url == "" or rest == "":
        logger.error("Bad URL: %s. There is no 'u?' part." % url)
        log_json()
        return body

    p = rest.split(",")

    if len(p) != 2:
        logger.error("Bad URL %s. Expected two parts at the end, used in thumbnail URL for CISOROOT and CISOPTR." %url)
        log_json()
        return body

    thumb_url = "%scgi-bin/thumbnail.exe?CISOROOT=%s&CISOPTR=%s" % (base_url, p[0], p[1])
    data[URL_FIELD_NAME] = thumb_url

    logger.debug("Thumbnail URL = " + thumb_url)
    return json.dumps(data)
        





