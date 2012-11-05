from akara.services import simple_service
from akara import request, response
from akara import module_config, logger
from akara.util import copy_headers_to_dict
from amara.thirdparty import json, httplib2
from amara.lib.iri import join
import uuid

COUCH_DATABASE = module_config().get('couch_database')

# FIXME: this should be JSON-LD, but CouchDB doesn't support +json yet
CT_JSON = {'Content-Type': 'application/json'}

H = httplib2.Http()
H.force_exception_as_status_code = True

# FIXME: should support changing media type in a pipeline
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

def couch_rev_check(docuri):
    # Get rev for follow on update. FIXME: should be able to optionally skip this for initial ingest
    resp, cont = H.request(docuri,'GET')
    if str(resp.status).startswith('2'):
        rev = json.loads(cont)['_rev']
        docuri += "?rev="+rev
    return docuri

@simple_service('POST', 'http://purl.org/la/dp/enrich', 'enrich', 'application/json')
def enrich(body,ctype):
    '''   
    Establishes a pipeline of services identified by an ordered list of URIs provided
    in two request headers, one for collections and one for records
    '''

    request_headers = copy_headers_to_dict(request.environ)
    source_name = request_headers.get('Source')
    collection_name = request_headers.get('Collection')

    if not (collection_name or source_name):
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Source and Profile request headers are required"

    coll_enrichments = request_headers.get(u'Pipeline-Coll','').split(',')
    rec_enrichments = request.environ.get(u'Pipeline-Rec','').split(',')

    data = json.loads(body)

    # First, we run the collection representation through its enrichment pipeline
    cid = "%s-%s"%(source_name,collection_name)
    at_id = "http://dp.la/api/collections/" + cid
    COLL = {
        "_id": cid,
        "@id": at_id
    }

    enriched_coll_text = pipe(COLL, ctype, coll_enrichments, 'HTTP_PIPELINE_COLL')
    enriched_collection = json.loads(enriched_coll_text)
    if COUCH_DATABASE:
        docuri = couch_rev_check(join(COUCH_DATABASE,cid))
        resp, cont = H.request(docuri,'PUT',body=enriched_coll_text,headers=CT_JSON)
        if not str(resp.status).startswith('2'):
            logger.debug("Error storing collection in Couch: "+repr(resp))

    # Then the records
    for record in data[u'items']:
        # Preserve record prior to any enrichments
        record['original_record'] = record.copy()         

        # Add collection information
        record[u'collection'] = {'@id' : at_id}
        if 'title' in enriched_collection:
            record[u'collection'].update({'title' : enriched_collection['title']})

        # Set id to value of the first handle, disambiguated w source. Not sure if
        # one is guaranteed or on what scale it's unique
        rid = "%s-%s"%(source_name,record[u'handle'][0].strip())
        record[u'_id'] = rid

        enriched_record = pipe(record, ctype, rec_enrichments, 'HTTP_PIPELINE_REC')
        if COUCH_DATABASE:
            # Get rev for follow on update. FIXME: should be able to optionally skip this for initial ingest
            docuri = couch_rev_check(join(COUCH_DATABASE,rid))
            resp, cont = H.request(docuri,'PUT',body=enriched_record,headers=CT_JSON)
            if not str(resp.status).startswith('2'):
                logger.debug("Error storing record in Couch: "+repr(resp))
                continue
    
    return json.dumps({})
