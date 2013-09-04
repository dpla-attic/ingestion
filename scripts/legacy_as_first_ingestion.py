#!/usr/bin/env python
"""
Script to create first ingestion document and update records with ingestion
version 1 for legacy data.

Usage:
    $ python scripts/legacy_as_first_ingestion.py provider_name
"""
import sys
import argparse
from dplaingestion.couch import Couch

def define_arguments():
    """Defines command line arguments for the current script
    """
    parser = argparse.ArgumentParser()
    name_help = "The provider's name as listed in the provider's profile"
    parser.add_argument("provider_name", help=name_help, type=str)
    return parser

def main(argv=None, couch=None, provider_name=None):
    # For testing, couch and provider_name will be provided as params
    if couch:
        provider_name = provider_name
    else:
        couch = Couch()
        parser = define_arguments()
        args = parser.parse_args(argv[1:])
        provider_name = args.provider_name

    provider_legacy_docs = couch._query_all_dpla_provider_docs(provider_name)
    ingest_docs = couch._query_all_provider_ingestion_docs(provider_name)

    # Proceed only if there are no ingestion documents for the provider but
    # there are provider_legacy_docs.
    proceed = True
    if len(ingest_docs) > 0:
        num = len(ingest_docs)
        print >> sys.stderr, "Error: %s ingestion document(s) exists" % num
        proceed = False
    try:
        next_item = next(couch._query_all_dpla_provider_docs(provider_name))
    except:
        print >> sys.stderr, "Error: No documents found for %s" % provider_name
        proceed = False

    def _post(dpla_docs, dashboard_docs, ingest_doc):
        couch._bulk_post_to(couch.dpla_db, dpla_docs)
        couch._bulk_post_to(couch.dashboard_db, dashboard_docs)
        couch._update_ingestion_doc_counts(ingest_doc,
                                           countAdded=len(dashboard_docs))

    if proceed:
        ingest_doc_id = couch.create_ingestion_doc_and_backup_db(provider_name)
        ingest_doc = couch.dashboard_db[ingest_doc_id]

        docs = []
        added_docs = []
        print >> sys.stderr, "Fetching all docs..."
        count = 0
        for doc in provider_legacy_docs:
            count += 1
            doc["ingestionSequence"] = 1
            docs.append(doc)

            added_docs.append({"id": doc["_id"],
                               "type": "record",
                               "status": "added",
                               "provider": provider_name,
                               "ingestionSequence": 1})
            # POST every 1000
            if len(docs) == 1000:
                print >> sys.stderr, "Processed %s docs" % count
                _post(docs, added_docs, ingest_doc)
                # Reset
                docs = []
                added_docs = []

        # Last POST
        if docs:
            print >> sys.stderr, "Processed %s docs" % count
            _post(docs, added_docs, ingest_doc)

        print >> sys.stderr, "Complete" 

if __name__ == "__main__":
    main(sys.argv)
