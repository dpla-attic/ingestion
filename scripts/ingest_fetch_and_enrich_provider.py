#!/usr/bin/env python
"""
Script to ingest provider data

Usage:
    $ python ingest_fetch_and_enrich_provider.py profile_path
"""
import sys
import argparse
import create_ingestion_document
import fetch_records
import enrich_records
import check_fetch_and_enrich_counts


def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("profile_path",
                        help="The path to the profile(s) to be processed",
                        nargs="+")
    return parser


def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    for profile_path in args.profile_path:
        print "Creating ingestion document for profile %s" % profile_path
        ingestion_doc_id = create_ingestion_document.main([None, profile_path])
        if ingestion_doc_id is None:
            print "Error creating ingestion document"
            continue

        resp = fetch_records.main([None, ingestion_doc_id])
        if not resp == 0:
            print "Error fetching records"
            continue

        resp = enrich_records.main([None, ingestion_doc_id])
        if not resp == 0:
            print "Error enriching records"
            continue

        resp = check_fetch_and_enrich_counts.main([None, ingestion_doc_id])
        if not resp == 0:
            print "Error checking counts"
            continue

        print "Fetch and enrich complete!!"

if __name__ == '__main__':
    main(sys.argv)
