from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from akara import module_config
from amara.lib.iri import is_absolute

IGNORE = module_config().get('IGNORE')
PENDING = module_config().get('PENDING')

@simple_service('POST', 'http://purl.org/la/dp/contentdm_identify_object',
    'contentdm_identify_object', 'application/json')
def contentdm_identify_object(body, ctype, download="True"):
    """
    Responsible for: adding a field to a document with the URL where we
    should expect to the find the thumbnail.

    There are two methods of creating the thumbnail URL:
    1. Replacing "cdm/ref" with "utils/getthumbail" in the handle field
       Example:
           handle: http://test.provider/cdm/ref/collection/1/id/1
           thumbnail: http://test.provider/utils/getthumbnail/collection/1/id/1

    2. Splitting the handle field on "u?" and using the parts to compose the
       thumbnail URL.
       Example:
            handle: http://test.provider/u?/ctm,101
            thumbnail: http://test.provider/cgi-bin/thumbnail.exe?CISOROOT=/ctm&CISOPTR=101"
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    handle_field = "originalRecord/handle"
    if exists(data, handle_field):
        url = None
        handle = getprop(data, handle_field)
        for h in (handle if not isinstance(handle, basestring) else [handle]):
            if is_absolute(h):
                url = h
                break
        if not url:
            logger.error("There is no URL in %s." % handle_field)
            return body
    else:
        logger.error("Field %s does not exist" % handle_field)
        return body

    if "cdm/ref" in url:
        object = url.replace("cdm/ref", "utils/getthumbnail")
    else:
        p = url.split("u?")
        if len(p) != 2:
            logger.error("Bad URL %s. It should have just one 'u?' part." %
                         url)
            log_json()
            return body

        (base_url, rest) = p

        if base_url == "" or rest == "":
            logger.error("Bad URL: %s. There is no 'u?' part." % url)
            log_json()
            return body

        p = rest.split(",")

        if len(p) != 2:
            logger.error("Bad URL %s. Expected two parts at the end, used " +
                         "in thumbnail URL for CISOROOT and CISOPTR." % url)
            log_json()
            return body

        # Thumb url field.
        object = "%scgi-bin/thumbnail.exe?CISOROOT=%s&CISOPTR=%s" % \
                 (base_url, p[0], p[1])

    data["object"] = object

    status = IGNORE
    if download == "True":
        status = PENDING

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    return json.dumps(data)
