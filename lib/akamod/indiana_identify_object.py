"""
Pipeline module to assign edm:object for Indiana Memory data
"""

import json
from akara import response
from akara.services import simple_service


@simple_service('POST',
                'http://purl.org/la/dp/indiana_identify_object',
                'indiana_identify_object',
                'application/json')
def indiana_identify_object(body, ctype_ignored):
    """assign edm:object based on dc:source

    Per Indiana crosswalk, http://bit.ly/dpla-crosswalks
    dc:source lives in originalRecord.source
    """
    try:
        record = json.loads(body)
        record['object'] = record['originalRecord']['source']
        return json.dumps(record)
    except ValueError:
        prepare_error_response()
        return 'Unable to parse request body as JSON'
    except KeyError:
        prepare_error_response()
        return 'No originalRecord.source for determining object'

def prepare_error_response():
    """Set HTTP response code and content type for an error"""
    response.code = 500
    response.add_header('Content-Type', 'text/plain')
