#!/usr/bin/env python
"""
Script to create first ingestion document and update records with ingestion
version 1 for legacy data.

Usage:
    $ python scripts/legacy_as_first_ingestion.py provider_legacy_name
"""
import sys
import argparse
from dplaingestion.couch import Couch

def define_arguments():
    """Defines command line arguments for the current script
    """
    parser = argparse.ArgumentParser()
    name_help = "The provider's name as listed in the provider's legacy profile"
    parser.add_argument("provider_legacy_name", help=name_help, type=str)
    return parser

def main(argv=None, couch=None, provider_legacy_name=None):
    # For testing, couch and provider_legacy_name will be provided as params
    if couch:
        provider_legacy_name = provider_legacy_name
    else:
        couch = Couch()
        parser = define_arguments()
        args = parser.parse_args(argv[1:])
        provider_legacy_name = args.provider_legacy_name

    if provider_legacy_name == "clemson":
        provider_name = "scdl-clemson"
    elif provider_legacy_name == "ia":
        provider_name = "internet_archive"
    elif provider_legacy_name == "kentucky":
        provider_name = "kdl"
    elif provider_legacy_name == "minnesota":
        provider_name = "mdl"
    else:
        provider_name = provider_legacy_name

    provider_legacy_rows = couch._query_all_dpla_provider_docs(provider_name)
    ingest_docs = couch._query_all_provider_ingestion_docs(provider_name)

    # Proceed only if there are no ingestion documents for the provider but
    # there are provider_legacy_rows.
    if len(ingest_docs) > 0:
        num = len(ingest_docs)
        print >> sys.stderr, "Error: %s ingestion document(s) exists" % num
    elif len(provider_legacy_rows) == 0:
        print >> sys.stderr, "Error: No documents found for this provider"
    else:
        # Use the new provider_name for the ingestion document
        ingest_doc_id = couch.create_ingestion_doc_and_backup_db(provider_name)

        added_docs = []
        count = 0
        total = len(provider_legacy_rows)
        print >> sys.stderr, "Fetching all docs..."
        for row in provider_legacy_rows:
            count += 1
            row["doc"]["ingestion_version"] = 1
            couch.dpla_db.save(row["doc"])

            added_docs.append({"id": row["doc"]["id"],
                               "type": "record",
                               "status": "added",
                               "ingestion_version": 1})
            # POST every 1000
            if len(added_docs) == 1000 or count == total:
                print >> sys.stderr, "Processed %s of %s" % (count, total)
                couch.bulk_post_to_dashboard(added_docs)
                couch._update_ingestion_doc_counts(ingest_doc_id,
                                                  count_added=len(added_docs))
        print >> sys.stderr, "Complete" 

if __name__ == "__main__":
    main(sys.argv)
