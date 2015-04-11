from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists, delprop
from akara import module_config
from amara.lib.iri import is_absolute

@simple_service('POST', 'http://purl.org/la/dp/cdl_identify_object',
    'cdl_identify_object', 'application/json')
def cdl_identify_object(body, ctype):
    """
    Responsible for: adding a field to a document with the URL where we
    should expect to the find the thumbnail.
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    #handle_field = "originalRecord/handle"
    url = None
    if exists(data, "object"):
        handle = getprop(data, "object")
        for h in (handle if not isinstance(handle, basestring) else [handle]):
            if is_absolute(h):
                url = h
                break
    if exists(data, "originalRecord/doc/isShownBy"):
        handle = getprop(data, "originalRecord/doc/isShownBy")
        for h in (handle if not isinstance(handle, basestring) else [handle]):
            if is_absolute(h):
                url = h
                break

    if url:
        if 'content.cdlib.org' in url:
            base_url, obj_id, object_type = url.rsplit("/", 2)
            is_shown_at = getprop(data, "isShownAt")
            is_shown_at_base, is_shown_at_id = is_shown_at.rsplit("/", 1)
            if obj_id != is_shown_at_id:
                logger.warn("Object url for %s has ARK value (%s) that does not match isShownAt (%s)" % (data["_id"], obj_id, is_shown_at_id))
                obj_id = is_shown_at_id
            url = "/".join([base_url, obj_id, object_type])
            if object_type == "hi-res":
                setprop(data, "hasView", {"@id": url})
                url = url.replace('hi-res', 'thumbnail')

        setprop(data, "object", url)
    else:
        logger.warn("No url found for object in id %s" % data["_id"])
        delprop(data, "object", True)
    return json.dumps(data)
