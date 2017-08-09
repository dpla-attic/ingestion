import hashlib
from amara.thirdparty import json
from akara.services import simple_service
from akara.util import copy_headers_to_dict
from akara import request, response
from dplaingestion import textnode
from dplaingestion.selector import getprop, exists
from dplaingestion.utilities import couch_rec_id_builder, clean_id


@simple_service('POST', 'http://purl.org/la/dp/select-id-missouri',
                'select-id-missouri', 'application/json')
def selid(body, ctype, prop='handle', use_source='yes'):
    """Service that accepts a JSON document and adds or sets the "id" property to
    the value of the property named by the "prop" parameter"""
    if not prop:
        # Remove this document
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "No id property has been selected"

    try:
        data = json.loads(body)
    except Exception:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    request_headers = copy_headers_to_dict(request.environ)
    source_name = request_headers.get('Source')

    record_id = None

    if exists(data, prop):
        v = getprop(data, prop)
        if isinstance(v, basestring):
            record_id = v
        else:
            if v:
                # Make an array of IDs (if needed) and iterate over it
                for h in (v if isinstance(v, list) else [v]):
                    """ The only valid path to select a original ID for 
                    Missouri is <mods:identifier type='local'>. If this does 
                    not exist then no DPLA ID can be minted."""
                    if not record_id:
                        if getprop(h, "type", True) == "local":
                            record_id = textnode.textnode(h)

    if not record_id:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "No id property was found"

    '''
    If the useSource parameter is True (default) than prepend it to
    the id and use that value when hashing for the DPLA id
    '''
    if use_source.lower() == 'yes':
        data[u'_id'] = couch_rec_id_builder(source_name, record_id)
    else:
        data[u'_id'] = clean_id(record_id)

    data[u'id'] = hashlib.md5(data[u'_id']).hexdigest()

    return json.dumps(data)