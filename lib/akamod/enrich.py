import datetime
from akara import logger
from akara import request
from amara.lib.iri import is_absolute
from akara.services import simple_service
from akara.util import copy_headers_to_dict
from amara.thirdparty import json, httplib2

H = httplib2.Http()
H.force_exception_as_status_code = True

# FIXME: should support changing media type in a pipeline
def pipe(content, ctype, enrichments, wsgi_header):
    body = json.dumps(content)
    for uri in enrichments:
        if not uri: continue # in case there's no pipeline
        if not is_absolute(uri):
            prefix = request.environ["wsgi.url_scheme"] + "://" 
            if request.environ.get("HTTP_HOST"):
                prefix += request.environ["HTTP_HOST"]
            else:
                prefix += request.environ["SERVER_NAME"]
            uri = prefix + uri
        headers = copy_headers_to_dict(request.environ, exclude=[wsgi_header])
        headers["content-type"] = ctype
        logger.debug("Calling url: %s " % uri)
        resp, cont = H.request(uri, "POST", body=body, headers=headers)
        if not str(resp.status).startswith("2"):
            logger.warn("Error in enrichment pipeline at %s: %s" % 
                        (uri, repr(resp)))
            continue
        body = cont

    return body

@simple_service("POST", "http://purl.org/la/dp/enrich", "enrich",
                "application/json")
def enrich(body, ctype):
    """
    Establishes a pipeline of services identified by an ordered list of URIs
    provided in two request headers, one for collections and one for items.

    Returns a JSON dump of the collections and records enriched along with a
    count of records enriched.
    """
    request_headers = copy_headers_to_dict(request.environ)
    item_enrichments = request_headers.get(u"Pipeline-Item", "").split(",")
    coll_enrichments = request_headers.get(u"Pipeline-Coll", "").split(",")

    records = json.loads(body)

    # Counts for enrich script
    enriched_coll_count = 0
    enriched_item_count = 0
    missing_id_count = 0
    missing_source_resource_count = 0

    enriched_records = {}
    for record in records:
        if record.get("ingestType") == "collection":
            wsgi_header = "HTTP_PIPELINE_COLL"
            enrichments = coll_enrichments
        else:
            wsgi_header = "HTTP_PIPELINE_ITEM"
            enrichments = item_enrichments
            # Preserve record prior to any enrichments
            record["originalRecord"] = record.copy()         
            record["ingestType"] = "item"

        record["ingestDate"] = datetime.datetime.now().isoformat()

        enriched_record_text = pipe(record, ctype, enrichments, wsgi_header)
        enriched_record = json.loads(enriched_record_text)

        ingest_type = record.get("ingestType")
        # Enriched record should have an _id
        if enriched_record.get("_id", None):
            # Item records should have sourceResource
            if (ingest_type == "item" and not "sourceResource" in
                enriched_record):
                logger.error("Records %s does not have sourceResource: %s" %
                             (enriched_record["_id"], enriched_record))
                missing_source_resource_count += 1
            else:
                enriched_records[enriched_record["_id"]] = enriched_record
                if ingest_type == "item":
                    enriched_item_count += 1
                else:
                    enriched_coll_count += 1
        else:
            logger.error("Found a record without an _id %s" % enriched_record)
            missing_id_count += 1

    data = {
        "enriched_records": enriched_records,
        "enriched_coll_count": enriched_coll_count,
        "enriched_item_count": enriched_item_count,
        "missing_id_count": missing_id_count,
        "missing_source_resource_count": missing_source_resource_count
    }

    return json.dumps(data)

@simple_service("POST", "http://purl.org/la/dp/enrich_storage",
                "enrich_storage", "application/json")
def enrich_storage(body, ctype):
    """Establishes a pipeline of services identified by an ordered list of URIs
       provided in request header "Pipeline-Item"
    """

    request_headers = copy_headers_to_dict(request.environ)
    rec_enrichments = request_headers.get(u"Pipeline-Item","").split(",")

    records = json.loads(body)

    # Counts
    enriched_coll_count = 0
    enriched_item_count = 0
    missing_id_count = 0
    missing_source_resource_count = 0

    enriched_records = {}
    for record in records:
        enriched_record_text = pipe(record, ctype, rec_enrichments,
                                    "HTTP_PIPELINE_ITEM")
        enriched_record = json.loads(enriched_record_text)

        if enriched_record.get("_id", None):
            ingest_type = enriched_record.get("ingestType")
            # Item records should have sourceResource
            if (ingest_type == "item" and not
                "sourceResource" in enriched_record):
                logger.error("Record %s does not have sourceResource: %s" %
                             (enriched_record["_id"], enriched_record))
                missing_source_resource_count += 1
            else:
                enriched_records[enriched_record["_id"]] = enriched_record
                if ingest_type == "item":
                    enriched_item_count += 1
                else:
                    enriched_coll_count += 1
        else:
            logger.error("Found a record without an _id %s" % enriched_record)
            missing_id_count += 1

    data = {
        "enriched_records": enriched_records,
        "enriched_coll_count": enriched_coll_count,
        "enriched_item_count": enriched_item_count,
        "missing_id_count": missing_id_count,
        "missing_source_resource_count": missing_source_resource_count
    }

    return json.dumps(data)


    return json.dumps(docs)
