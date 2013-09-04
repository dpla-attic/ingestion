from akara.services import simple_service
from akara import request, response
from akara import module_config
from akara.util import copy_headers_to_dict
from amara.thirdparty import json, httplib2
from amara.lib.iri import join, is_absolute
from urllib import quote, urlencode, quote_plus
from akara import logger
import datetime
import uuid
import base64
import hashlib

def iterify(iterable):
    """Treat iterating over a single item or an iterator seamlessly"""
    if (isinstance(iterable, basestring) or isinstance(iterable, dict)):
        iterable = [iterable]
    try:
        iter(iterable)
    except TypeError:
        iterable = [iterable]

    return iterable

COUCH_ID_BUILDER = lambda src, lname: "--".join((src,lname))
# Set id to value of the first identifier, disambiguated w source. Not sure if
# an OAI handle is guaranteed or on what scale it's unique.
# FIXME it's looking like an id builder needs to be part of the profile. Or UUID as fallback?
COUCH_REC_ID_BUILDER = lambda src, rec: COUCH_ID_BUILDER(src,rec.get(u'id','no-id').strip().replace(" ","__"))

H = httplib2.Http()
H.force_exception_as_status_code = True

COLLECTIONS = {}

# FIXME: should support changing media type in a pipeline
def pipe(content, ctype, enrichments, wsgi_header):
    body = json.dumps(content)
    for uri in enrichments:
        if not uri: continue # in case there's no pipeline
        if not is_absolute(uri):
            prefix = request.environ['wsgi.url_scheme'] + '://' 
            if request.environ.get('HTTP_HOST'):
                prefix += request.environ['HTTP_HOST']
            else:
                prefix += request.environ['SERVER_NAME']
            uri = prefix + uri
        headers = copy_headers_to_dict(request.environ, exclude=[wsgi_header])
        headers['content-type'] = ctype
        logger.debug("Calling url: %s " % uri)
        resp, cont = H.request(uri, 'POST', body=body, headers=headers)
        if not str(resp.status).startswith('2'):
            logger.warn("Error in enrichment pipeline at %s: %s" % 
                        (uri, repr(resp)))
            continue
        body = cont

    return body

def set_ingested_date(doc):
    doc[u'ingestDate'] = datetime.datetime.now().isoformat()

def enrich_coll(ctype, provider, set_id, coll_enrichments, title=None,
                description=None):
    cid = COUCH_ID_BUILDER(provider, set_id)
    id = hashlib.md5(cid).hexdigest()
    at_id = "http://dp.la/api/collections/" + id
    coll = {
        "id": id,
        "_id": cid,
        "@id": at_id,
        "ingestType": "collection"
    }
    if title:
        coll["title"] = title
    if description:
        coll["description"] = description
    set_ingested_date(coll)
    enriched_coll_text = pipe(coll, ctype, coll_enrichments,
                              'HTTP_PIPELINE_COLL')
    enriched_collection = json.loads(enriched_coll_text)

    return enriched_collection

def create_record_collection(collection):
    record_collection = collection.copy()
    for prop in ['_id', 'ingestType', 'ingestDate']:
        del record_collection[prop]

    return record_collection

@simple_service('POST', 'http://purl.org/la/dp/enrich', 'enrich',
                'application/json')
def enrich(body, ctype):
    """
    Establishes a pipeline of services identified by an ordered list of URIs
    provided in two request headers, one for collections and one for records.

    Returns a JSON dump of the collections and records enriched along with a
    count of records enriched.
    """
    request_headers = copy_headers_to_dict(request.environ)
    rec_enrichments = request_headers.get(u'Pipeline-Rec', '').split(',')
    coll_enrichments = request_headers.get(u'Pipeline-Coll', '').split(',')

    data = json.loads(body)
    provider = data['provider']
    collection = data['collection']
    contributor = data['contributor']

    # Enrich collection first
    if collection:
        coll_id = collection.get('id')
        desc = collection.get('description')
        title = collection.get('title')
        COLLECTIONS[coll_id] = enrich_coll(ctype, provider, coll_id,
                                           coll_enrichments, title, desc)

    docs = {}
    for record in data['records']:
        # Preserve record prior to any enrichments
        record['originalRecord'] = record.copy()         

        # Set ingestType, provider, and ingestDate
        record[u'ingestType'] = 'item'
        record[u'provider'] = contributor
        set_ingested_date(record)

        # Add collection(s)
        record[u'collection'] = []
        # OAI records can be part of multiple collections whose titles are
        # listed in the record's "setSpec" property
        sets = record.get('setSpec')
        if sets:
            for set_id in iterify(sets):
                if set_id not in COLLECTIONS:
                    COLLECTIONS[set_id] = enrich_coll(ctype, provider, set_id,
                                                      coll_enrichments)
                record[u'collection'].append(create_record_collection(
                                        COLLECTIONS[set_id])
                                        )

            if len(record[u'collection']) == 1:
                record[u'collection'] = record[u'collection'][0]
        elif collection:
            record[u'collection'] = create_record_collection(
                                        COLLECTIONS[coll_id]
                                        )

        doc_text = pipe(record, ctype, rec_enrichments, 'HTTP_PIPELINE_REC')
        doc = json.loads(doc_text)
        # After pipe doc must have _id and sourceResource
        if doc.get("_id", None):
            if "sourceResource" in doc:
                logger.debug("Enriched record %s" % doc["_id"])
                docs[doc["_id"]] = doc
            else:
                logger.error("Document %s does not have sourceResource: %s" %
                             (doc["_id"], doc))
        else:
            logger.error("Document does not have an _id: %s" % doc)

    enriched_records_count =  len(docs)

    # Add collections to docs
    for collection in COLLECTIONS.values():
        docs[collection["_id"]] = collection

    data = {
        "enriched_records": docs,
        "enriched_records_count": enriched_records_count
    }

    return json.dumps(data)

@simple_service('POST', 'http://purl.org/la/dp/enrich_storage',
                'enrich_storage', 'application/json')
def enrich_storage(body, ctype):
    """Establishes a pipeline of services identified by an ordered list of URIs
       provided in request header 'Pipeline-Rec'
    """

    request_headers = copy_headers_to_dict(request.environ)
    rec_enrichments = request_headers.get(u'Pipeline-Rec','').split(',')

    data = json.loads(body)

    docs = {}
    for record in data:
        doc_text = pipe(record, ctype, rec_enrichments, 'HTTP_PIPELINE_REC')
        doc = json.loads(doc_text)
        docs[doc["_id"]] = doc

    return json.dumps(docs)
