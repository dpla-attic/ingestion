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
    $ python poll_storage.py uri_base profile pipeline

Example:
    $ python scripts/poll_storage.py http://localhost:8879 \
    > profiles/artstor.pjs geocode

    $ python scripts/poll_storage.py http://localhost:8879 \
    > profiles/artstor.pjs \
    > "/set_prop?prop=subject&value=a%3Bb,/shred?prop=subject%2Cformat"
"""
import sys
import argparse
from amara.thirdparty import json, httplib2
from dplaingestion.couch import Couch
import create_ingestion_document

ENRICH = "/enrich_storage"
H = httplib2.Http()
H.force_exception_to_status_code = True

def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    uri_help = "The base URI for the server hosting the enrichment pipeline"
    profile_help = "The path to the profile to be processed"
    pipeline_help = "Either the name of an enrichment pipeline in the " + \
                    "profile that contains the list of enrichments to be " + \
                    "run or a comma-separated enrichments string"
    parser.add_argument("uri_base", help=uri_help)
    parser.add_argument("profile_path", help=profile_help)
    parser.add_argument("pipeline", help=pipeline_help, type=str)
    return parser

def enrich(docs, uri_base, pipeline):
    headers = {
        "Content-Type": "application/json",
        "Pipeline-Rec": pipeline
    }

    resp, content = H.request(uri_base + ENRICH, "POST",
                              body=json.dumps(docs), headers=headers)
    if not str(resp.status).startswith("2"):
        raise IOError("%d %s: %s" % (resp.status, resp.reason, content))

    return json.loads(content)

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
    ingestion_doc["poll_storage_process"] = {"status": "running"}
    couch.dashboard_db.update([ingestion_doc])

    # Fetch provider documents
    docs = []
    count = 0
    for doc in couch._query_all_dpla_provider_docs(provider):
        docs.append(doc)
        count += 1
        # Enrich in batches of 1000
        if len(docs) == 1000:
            enriched_docs = enrich(docs, args.uri_base, pipeline)
            couch.process_and_post_to_dpla(enriched_docs, ingestion_doc)
            print "Enriched %s documents" % count
            docs = []
    # Enrich last batch
    if docs:
        enriched_docs = enrich(docs, args.uri_base, pipeline)
        couch.process_and_post_to_dpla(enriched_docs, ingestion_doc)
        print "Enriched %s documents" % count

    # Update ingestion document
    ingestion_doc["fetch_process"] = {"status": "complete"}
    ingestion_doc["enrich_process"] = {"status": "complete"}
    ingestion_doc["save_process"] = {"status": "complete"}
    ingestion_doc["delete_process"] = {"status": "complete"}
    ingestion_doc["poll_storage_process"]["status"] = "complete"
    couch.dashboard_db.update([ingestion_doc])

if __name__ == "__main__":
    main(sys.argv)
