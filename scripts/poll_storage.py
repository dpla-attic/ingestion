#!/usr/bin/env python
"""
Script to run pipeline against records pulled from a CouchDB

Simple use cases:
    Download thumbnails for all records related to a profile that have not
    already been downloaded

    Validate all URLs within records on a profile and flag records with URLs
    that don't resolve

    Perform geocoding for all records on a profile

    Pass a pipeline via the command line as comma-separated enrichments

Usage:
    $ python poll_storage.py profile pipeline

Example:
    $ python scripts/poll_storage.py profiles/artstor.pjs geocode

    $ python scripts/poll_storage.py profiles/artstor.pjs \
    > "/set_prop?prop=subject&value=a%3Bb,/shred?prop=subject%2Cformat"
"""
import sys
import argparse
import dashboard_cleanup
import remove_deleted_records
import create_ingestion_document
from datetime import datetime
from dplaingestion.couch import Couch
from amara.thirdparty import json, httplib2

ENRICH = "/enrich_storage"
H = httplib2.Http()
H.force_exception_to_status_code = True

def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    profile_help = "The path to the profile to be processed"
    pipeline_help = "Either the name of an enrichment pipeline in the " + \
                    "profile that contains the list of enrichments to be " + \
                    "run or a comma-separated enrichments string"
    parser.add_argument("profile_path", help=profile_help)
    parser.add_argument("pipeline", help=pipeline_help, type=str)
    return parser

def enrich(docs, uri_base, pipeline):
    headers = {
        "Content-Type": "application/json",
        "Pipeline-Item": pipeline
    }

    resp, content = H.request(uri_base + ENRICH, "POST",
                              body=json.dumps(docs), headers=headers)
    if not str(resp.status).startswith("2"):
        raise IOError("%d %s: %s" % (resp.status, resp.reason, content))

    return json.loads(content)

def enrich_and_save_batch(doc_batch, ingestion_doc, pipeline, couch):
    data = None
    try:
        data = enrich(doc_batch, ingestion_doc["uri_base"],
                      pipeline)
    except Exception, e:
        error_msg = "Error enriching documents: %s" % e
        return error_msg, data

    enriched_docs = data["enriched_records"]
    resp, error_msg = couch.process_and_post_to_dpla(enriched_docs,
                                                     ingestion_doc)
    return error_msg, data

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])
    with open(args.profile_path, "r") as f:
        try:
            profile = json.load(f)
        except Exception, e:
            print "Error reading profile: %s" % e
            return False

    if args.pipeline in profile:
        pipeline = ",".join(profile[args.pipeline])
    else:
        pipeline = args.pipeline
    provider = profile.get(u"name")
    contributor = profile.get(u"contributor", {})

    # Create ingestion document
    couch = Couch()
    ingestion_doc_id = create_ingestion_document.main([None,
                                                       args.profile_path])
    ingestion_doc = couch.dashboard_db[ingestion_doc_id]

    # Update ingestion document
    kwargs = {
        "poll_storage_process/status": "running",
        "poll_storage_process/start_time": datetime.now().isoformat(),
        "poll_storage_process/end_time": None,
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return

    # Back up current data
    resp = couch._back_up_data(ingestion_doc)

    if resp == -1: 
        # Fatal error, do not continue with save process
        kwargs = { 
            "poll_storage_process/status": "error",
            "poll_storage_process/end_time": datetime.now().isoformat(),
            "poll_storage_process/error": "Error backing up DPLA records"
        }   
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
        return

    # Fetch provider documents
    docs = []
    errors = []
    total_enriched = 0
    enriched_items = 0
    enriched_colls = 0
    missing_id = 0
    missing_source_resource = 0

    for doc in couch._query_all_dpla_provider_docs(provider):
        docs.append(doc)
        # Enrich in batches of batch_size
        if len(docs) == couch.batch_size:
            error, data = enrich_and_save_batch(docs, ingestion_doc, pipeline,
                                                couch)
            docs = []
            if error is None:
                # Update counts
                errors.extend(data["errors"])
                enriched_items += data["enriched_item_count"]
                enriched_colls += data["enriched_coll_count"]
                missing_id += data["missing_id_count"]
                missing_source_resource += data["missing_source_resource_count"]
                total_enriched += len(data["enriched_records"])
                print "Enriched %s" % total_enriched
            else:
                errors.append(error)
                break
            
    # Enrich last batch
    if docs:
        error, data = enrich_and_save_batch(docs, ingestion_doc, pipeline,
                                            couch)
        if error is None:
            # Update counts
            errors.extend(data["errors"])
            enriched_items += data["enriched_item_count"]
            enriched_colls += data["enriched_coll_count"]
            missing_id += data["missing_id_count"]
            missing_source_resource += data["missing_source_resource_count"]
            total_enriched += len(data["enriched_records"])
            print "Enriched %s" % total_enriched

    if errors:
        print "Error %s" % errors

    # Update ingestion document
    kwargs = {
        "poll_storage_process/status": "complete",
        "poll_storage_process/end_time": datetime.now().isoformat(),
        "poll_storage_process/total_enriched": total_enriched,
        "poll_storage_process/total_items": enriched_items,
        "poll_storage_process/total_collections":  enriched_colls,
        "poll_storage_process/missing_id": missing_id,
        "poll_storage_process/missing_source_resource": missing_source_resource,
        "poll_storage_process/error": errors,
        "save_process/status": "complete"
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return

    # Run remove_deleted_records
    resp = remove_deleted_records.main([None, ingestion_doc_id])
    if not resp == 0:
        print "Error deleting records"
        return

    # Run dashboard cleanup
    resp = dashboard_cleanup.main([None, ingestion_doc_id])
    if not resp == 0:
        print "Error cleaning up dashboard"

if __name__ == "__main__":
    main(sys.argv)
