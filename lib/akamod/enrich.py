from akara.services import simple_service
from akara import logger
from akara import request
from akara import module_config
from akara.util import copy_headers_to_dict

import httplib2

@simple_service('POST', 'http://purl.org/la/dp/enrich', 'enrich', 'application/json')
def enrich(body,ctype):
    '''   
    Establishes a pipeline of services identified by an ordered list of URIs provided
    in the Pipeline request header to a PUT request
    '''   
    
    enrichments = request.environ.get(u'HTTP_PIPELINE','').split(',')

    H = httplib2.Http()
    H.force_exception_as_status_code = True

    content = body
    for uri in enrichments:
        headers = copy_headers_to_dict(request.environ,exclude=['HTTP_PIPELINE'])
        headers['content-type'] = ctype
        resp, content = H.request(uri,'POST',headers=headers)
        if not resp.status.startswith('2'):
            response.code = resp.status
            response.add_header('content-type',resp['content-type'])
            break

    return content
