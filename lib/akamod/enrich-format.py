import re
import os

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import delprop, getprop, setprop, exists
from amara.lib.iri import is_absolute
from dplaingestion.textnode import textnode


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

    REGEXPS = ('audio/mp3', 'audio/mpeg'), ('images/jpeg', 'image/jpeg'), \
              ('image/jpg', 'image/jpeg'), ('image/jp$', 'image/jpeg'), \
              ('img/jpg', 'image/jpeg'), ('^jpeg$', 'image/jpeg'), \
              ('^jpg$', 'image/jpeg'), ('\W$', '')
    IMT_TYPES = ['application', 'audio', 'image', 'message', 'model',
                 'multipart', 'text', 'video']

    def get_ext(s):
        ext = os.path.splitext(s)[1].split('.')

        return ext[1] if len(ext) == 2 else ""

    def cleanup(s):
        s = s.lower().strip()
        for pattern, replace in REGEXPS:
            s = re.sub(pattern, replace, s)
            s = re.sub(r"^([a-z0-9/]+)\s.*", r"\1", s)
        return s

    def is_imt(s):
        imt_regexes = [re.compile('^' + x + '(/)') for x in IMT_TYPES]
        return any(regex.match(s) for regex in imt_regexes)

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    imt_values = []
    if exists(data, prop):
        v = getprop(data, prop)
        format = []
        hasview_format = []

        if isinstance(v, list):
            st = v
        else:
            st = [textnode(v)]

        for s in st:
            if s is not None and s.startswith("http") and is_absolute(s):
                s = get_ext(s)
            cleaned = cleanup(s)
            if is_imt(cleaned):
                # Append to imt_values for use in type
                imt_values.append(cleaned)
                # Move IMT values to hasView/format else discard
                if exists(data, "hasView") and not \
                        exists(data, "hasView/format") and \
                        cleaned not in hasview_format:
                    hasview_format.append(cleaned)
            else:
                # Retain non-IMT values in sourceResource/format, non-cleaned
                if s not in format:
                    format.append(s)

        if format:
            if len(format) == 1:
                format = format[0]
            setprop(data, prop, format)
        else:
            delprop(data, prop)

        if hasview_format:
            if len(hasview_format) == 1:
                hasview_format = hasview_format[0]
            setprop(data, "hasView/format", hasview_format)

    # Setting the type if it is empty.
    if not exists(data, type_field) and imt_values:
        type = []
        for imt in imt_values:
            t = getprop(FORMAT_2_TYPE_MAPPINGS, imt.split("/")[0], True)
            if t and t not in type:
                type.append(t)

        if type:
            if len(type) == 1:
                type = type[0]
            setprop(data, type_field, type)

    return json.dumps(data)
