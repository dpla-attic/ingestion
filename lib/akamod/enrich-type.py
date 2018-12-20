from akara import logger, response, module_config
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import delprop, getprop, setprop, exists
import dplaingestion.itemtype as itemtype
from dplaingestion.textnode import textnode, NoTextNodeError
import re


type_for_type_keyword = \
    module_config('enrich_type').get('type_for_ot_keyword')
type_for_format_keyword = \
    module_config('enrich_type').get('type_for_phys_keyword')


@simple_service('POST', 'http://purl.org/la/dp/enrich-type', 'enrich-type',
                'application/json')
def enrichtype(body, ctype,
               action="enrich-type",
               prop="sourceResource/type",
               format_field="sourceResource/format",
               default=None,
               send_rejects_to_format=False):
    """   
    Service that accepts a JSON document and enriches the "type" field of that
    document by: 

    By default works on the 'type' field, but can be overridden by passing the
    name of the field to use as a parameter.

    A default type, if none can be determined, may be specified with the
    "default" querystring parameter.  If no default is given, the type field
    will be unmodified, or not added, in the result.
    """
    global type_for_type_keyword, type_for_format_keyword

    try:
        data = json.loads(body)
    except Exception:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    type_strings = []
    format_strings = []
    try:
        sr_type = data['sourceResource'].get('type', [])
        sr_format = data['sourceResource'].get('format', [])
    except KeyError:
        # In this case, sourceResource is not present, so give up and return
        # the original data unmodified.
        return body

    if (not isinstance(sr_format, list)):
        sr_format = [sr_format]

    if (not isinstance(sr_type, list)):
        sr_type = [sr_type]


    if sr_type:
        for t in sr_type:
            try:
                type_strings.append(textnode(t).lower())
            except NoTextNodeError:
                pass
    if sr_format:
        for f in sr_format:
            try:
                format_strings.append(textnode(f).lower())
            except NoTextNodeError:
                pass
    try:
        data['sourceResource']['type'] = \
            itemtype.type_for_strings_and_mappings([
                (format_strings, type_for_format_keyword),
                (type_strings, type_for_type_keyword)
            ])
    except itemtype.NoTypeError:
        if default:
            data['sourceResource']['type'] = default
        else:
            try:
                del data['sourceResource']['type']
            except:
                pass
    finally:
        if send_rejects_to_format and type_strings:
            rej = itemtype.rejects([(type_strings, type_for_type_keyword)])
            if rej:
                if (not isinstance(sr_format, list)):
                    sr_format = [sr_format]
                sr_format.extend(rej)
                data['sourceResource']['format'] = sr_format

    return json.dumps(data)
