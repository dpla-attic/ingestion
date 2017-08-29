#!/usr/bin/env python
"""
Script to ingest provider data

Usage:
    $ python ingest_provider.py profile_path
"""
import sys
import argparse
import save_records
import remove_deleted_records
import check_ingestion_counts
import dashboard_cleanup


def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("ingestion_doc_id",
                        help="The doc ID to process",
                        nargs="+")
    return parser


def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    for ingestion_doc_id in args.ingestion_doc_id:
        resp = save_records.main([None, ingestion_doc_id])
        if not resp == 0:
            print "Error saving records"
            continue

        resp = remove_deleted_records.main([None, ingestion_doc_id])
        if not resp == 0:
            print "Error deleting records"
            continue

        resp = check_ingestion_counts.main([None, ingestion_doc_id])
        if not resp == 0:
            print "Error checking counts"
            continue

        resp = dashboard_cleanup.main([None, ingestion_doc_id])
        if not resp == 0:
            print "Error cleaning up dashboard"
            continue

        print "Ingestion complete!"

if __name__ == '__main__':
    main(sys.argv)
