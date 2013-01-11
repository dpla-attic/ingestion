from akara.services import simple_service
from akara import request, response
from akara import module_config, logger
from akara.util import copy_headers_to_dict
from amara.thirdparty import json, httplib2
from amara.lib.iri import join
from urllib import quote
import datetime
import uuid
import base64

COUCH_DATABASE = module_config().get('couch_database')
COUCH_DATABASE_USERNAME = module_config().get('couch_database_username')
COUCH_DATABASE_PASSWORD = module_config().get('couch_database_password')

COUCH_ID_BUILDER = lambda src, lname: "--".join((src,lname))
# Set id to value of the first identifier, disambiguated w source. Not sure if
# an OAI handle is guaranteed or on what scale it's unique.
# FIXME it's looking like an id builder needs to be part of the profile. Or UUID as fallback?
COUCH_REC_ID_BUILDER = lambda src, rec: COUCH_ID_BUILDER(src,rec.get(u'id','no-id').strip())

COUCH_AUTH_HEADER = { 'Authorization' : 'Basic ' + base64.encodestring(COUCH_DATABASE_USERNAME+":"+COUCH_DATABASE_PASSWORD) }

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

# FIXME: should be able to optionally skip the revision checks for initial ingest
def couch_rev_check_coll(docuri,doc):
    'Add current revision to body so we can update it'
    resp, cont = H.request(docuri,'GET',headers=COUCH_AUTH_HEADER)
    if str(resp.status).startswith('2'):
        doc['_rev'] = json.loads(cont)['_rev']

def couch_rev_check_recs(docs,src):
    '''
    Insert revisions for all records into structure using CouchDB bulk interface.
    Uses key ranges to narrow bulk query to the source being ingested.
    '''
    uri = join(COUCH_DATABASE,'_all_docs')
    start = quote(COUCH_ID_BUILDER(src,''))
    end = quote(COUCH_ID_BUILDER(src,'Z'*100)) # FIXME. Is this correct?
    uri += '?startkey=%s&endkey=%s'%(start,end)
    resp, cont = H.request(join(COUCH_DATABASE,'_all_docs'),'GET',headers=COUCH_AUTH_HEADER)
    if str(resp.status).startswith('2'):
        rows = json.loads(cont)["rows"]
        #revs = { r["id"]:r["value"]["rev"] for r in rows } # 2.7 specific
        revs = {}
        for r in rows:
            revs[r["id"]] = r["value"]["rev"]
        for doc in docs:
            id = doc['id']
            if id in revs:
                doc['_rev'] = revs[id]
    else:
        logger.debug('Unable to retrieve document revisions via bulk interface: '+repr(resp))

def set_ingested_date(doc):
    doc[u'ingestDate'] = datetime.datetime.now().isoformat()

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
        return "Source and Collection request headers are required"

    coll_enrichments = request_headers.get(u'Pipeline-Coll','').split(',')
    rec_enrichments = request_headers.get(u'Pipeline-Rec','').split(',')

    data = json.loads(body)

    # First, we run the collection representation through its enrichment pipeline
    cid = COUCH_ID_BUILDER(source_name,collection_name)
    at_id = "http://dp.la/api/collections/" + cid
    COLL = {
        "_id": cid,
        "@id": at_id,
        "ingestType": "collection"
    }
    set_ingested_date(COLL)

    enriched_coll_text = pipe(COLL, ctype, coll_enrichments, 'HTTP_PIPELINE_COLL')
    enriched_collection = json.loads(enriched_coll_text)
    # FIXME. Integrate collection storage into bulk call below
    if COUCH_DATABASE:
        docuri = join(COUCH_DATABASE,cid)
        couch_rev_check_coll(docuri,enriched_collection)
        resp, cont = H.request(docuri,'PUT',body=json.dumps(enriched_collection),headers=dict(CT_JSON.items()+COUCH_AUTH_HEADER.items()))
        if not str(resp.status).startswith('2'):
            logger.debug("Error storing collection in Couch: "+repr((resp,cont)))

    # Then the records
    docs = []
    for record in data[u'items']:
        # Preserve record prior to any enrichments
        record['original_record'] = record.copy()         

        # Add collection information
        record[u'collection'] = {
            '@id' : at_id,
            'name' : enriched_collection.get('title',"")
        }

        record[u'id'] = COUCH_REC_ID_BUILDER(source_name,record)
        record[u'ingestType'] = 'item'
        set_ingested_date(record)

        doc_text = pipe(record, ctype, rec_enrichments, 'HTTP_PIPELINE_REC')
        doc = json.loads(doc_text)
        if 'id' in doc:
            doc[u'_id'] = doc[u'id']
        docs.append(doc)

    couch_rev_check_recs(docs,source_name)
    couch_docs_text = json.dumps({"docs":docs})
    if COUCH_DATABASE:
        resp, content = H.request(join(COUCH_DATABASE,'_bulk_docs'),'POST',body=couch_docs_text,headers=dict(CT_JSON.items()+COUCH_AUTH_HEADER.items()))
        logger.debug("Couch bulk update response: "+content)
        if not str(resp.status).startswith('2'):
            logger.debug('HTTP error posting to CouchDB: '+repr((resp,content)))

    return json.dumps({'docs' : docs})
