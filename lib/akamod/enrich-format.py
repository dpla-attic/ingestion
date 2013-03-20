from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import delprop, getprop, setprop, exists
import re
import os
from amara.lib.iri import is_absolute

@simple_service('POST', 'http://purl.org/la/dp/enrich-format', 'enrich-format',
                'application/json')
def enrichformat(body, ctype, action="enrich-format",
                 prop="sourceResource/format",
                 type_field="sourceResource/type"):
    """
    Service that accepts a JSON document and enriches the "format" field of
    that document by: 

    a) Setting the format to be all lowercase
    b) Running through a set of cleanup regex's (e.g. image/jpg -> image/jpeg)
    c) Checking to see if the field is a valid IMT
       See http://www.iana.org/assignments/media-types for list of valid
       media-types. We require that a subtype is defined.
    d) Removing any extra text after the IMT
    e) Moving valid IMT values to hasView/format if hasView exists and
       its format is not set
    f) Setting type field from format field, if it is not set. The format field
       is taken if it is a string, or the first element if it is a list. It is
        then split and the first part of IMT is taken.

    By default works on the 'sourceResource/format' field but can be overridden
    by passing the name of the field to use as the 'prop' parameter.
    """

    FORMAT_2_TYPE_MAPPINGS = {
            "audio": "sound",
            "image": "image",
            "video": "moving image",
            "text": "text"
    }

    REGEXPS = ('audio/mp3', 'audio/mpeg'), ('images/jpeg', 'image/jpeg'),\
              ('image/jpg','image/jpeg'),('image/jp$', 'image/jpeg'),\
              ('img/jpg', 'image/jpeg'), ('^jpeg$','image/jpeg'),\
              ('^jpg$', 'image/jpeg'), ('\W$','')
    IMT_TYPES = ['application', 'audio', 'image', 'message', 'model',
                 'multipart', 'text', 'video']

    def get_ext(s):
        ext = os.path.splitext(s)[1].split('.')

        return ext[1] if len(ext) == 2 else ""
        
    def cleanup(s):
        s = s.lower().strip()
        for pattern, replace in REGEXPS:
            s = re.sub(pattern, replace, s)
            s = re.sub(r"^([a-z0-9/]+)\s.*",r"\1", s)
        return s

    def is_imt(s):
        logger.debug("Checking: " + s)
        imt_regexes = [re.compile('^' + x + '(/)') for x in IMT_TYPES]
        return any(regex.match(s) for regex in imt_regexes)

    try :
        data = json.loads(body)
    except Exception:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop):
        v = getprop(data, prop)
        format = []
        hasview_format = []

        for s in (v if not isinstance(v,basestring) else [v]):
            if is_absolute(s):
                s = get_ext(s)
            cleaned = cleanup(s)
            if is_imt(cleaned):
                if exists(data, "hasView") and not \
                   exists(data, "hasView/format") and \
                   cleaned not in hasview_format:
                    hasview_format.append(cleaned)
                else:
                    if cleaned not in format:
                        format.append(cleaned)

        if format:
            if len(format) == 1:
                setprop(data, prop, format[0])
            else:
                setprop(data, prop, format)
        else:
            delprop(data, prop)
        if hasview_format:
            if len(hasview_format) == 1:
                setprop(data, "hasView/format", hasview_format[0])
            else:
                setprop(data, "hasView/format", hasview_format)

    # Setting the type if it is empty.
    t = getprop(data, type_field, True)
    if not t and exists(data, prop):
        format = getprop(data, prop)
        use_format = None
        if isinstance(format, list) and len(format) > 0:
            use_format = format[0]
        elif isinstance(format, basestring):
            use_format = format

        if use_format:
            use_format = use_format.split("/")[0]

            if use_format in FORMAT_2_TYPE_MAPPINGS:
                setprop(data, type_field, FORMAT_2_TYPE_MAPPINGS[use_format])

    return json.dumps(data)
