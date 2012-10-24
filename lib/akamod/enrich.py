from akara.services import simple_service
from akara import request, response
from akara import module_config, logger
from akara.util import copy_headers_to_dict
from amara.thirdparty import json, httplib2
import uuid


H = httplib2.Http()
H.force_exception_as_status_code = True

def pipe(content,ctype,enrichments,wsgi_header):
    body = json.dumps(content)
    for uri in enrichments:
        headers = copy_headers_to_dict(request.environ,exclude=[wsgi_header])
        headers['content-type'] = ctype
        resp, cont = H.request(uri,'POST',body=body,headers=headers)
        if not str(resp.status).startswith('2'):
            logger.debug("Error in enrichment pipeline at %s: %s"%(uri,repr(resp)))
            continue

        body = cont

@simple_service('POST', 'http://purl.org/la/dp/enrich', 'enrich', 'application/json')
def enrich(body,ctype):
    '''   
    Establishes a pipeline of services identified by an ordered list of URIs provided
    in two request headers, one for collections and one for records
    '''   
    
    coll_enrichments = request.environ.get(u'HTTP_PIPELINE_COLL','').split(',')
    rec_enrichments = request.environ.get(u'HTTP_PIPELINE_REC','').split(',')

    data = json.loads(body)

    # First, we run the collection representation through its enrichment pipeline

    COLL = {
        "id": str(uuid.uuid4()),
    }

    pipe(COLL, ctype, coll_enrichments, 'HTTP_PIPELINE_COLL')

    # Then the records
    for record in data[u'items']:
        # Assign the record to its collection
        record['collection'] = COLL['id']
        # Preserve record prior to any enrichments
        record['original_record'] = record.copy()

        pipe(record, ctype, rec_enrichments, 'HTTP_PIPELINE_REC')
    
    return json.dumps({})
