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
        if len(uri) < 1: continue # in case there's no pipeline
        headers = copy_headers_to_dict(request.environ,exclude=[wsgi_header])
        headers['content-type'] = ctype
        resp, cont = H.request(uri,'POST',body=body,headers=headers)
        if not str(resp.status).startswith('2'):
            logger.debug("Error in enrichment pipeline at %s: %s"%(uri,repr(resp)))
            continue

        body = cont
    return body

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
        "@id": "http://dp.la/api/collections/" + str(uuid.uuid4()),
    }

    enriched_collection = json.loads(pipe(COLL, ctype, coll_enrichments, 'HTTP_PIPELINE_COLL'))

    # Then the records
    docs = []
    for record in data[u'items']:
        # Preserve record prior to any enrichments
        record['original_record'] = record.copy()         

        # Add collection information
        record[u'collection'] = {
            '@id' : enriched_collection['@id'],
            'title' : enriched_collection['title'] if 'title' in enriched_collection else ""
        }

        doc = pipe(record, ctype, rec_enrichments, 'HTTP_PIPELINE_REC')
        docs.append(json.loads(doc))
    
    return json.dumps({'docs' : docs})
