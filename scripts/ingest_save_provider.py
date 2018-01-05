#!/usr/bin/env python
"""
Script to ingest provider data

Usage:
    $ python ingest_provider.py profile_path
"""
import argparse
import sys

from dplaingestion.couch import Couch
from dplaingestion.utilities import iso_utc_with_tz

import save_s3


def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("ingestion_document_id",
                        help="The doc ID to process")
    return parser


def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])
    ingestion_doc_id = args.ingestion_document_id
    resp = save_s3.main([None, ingestion_doc_id])
    if not resp == 0:
        print "Error saving records"
        return -1

    # Mark delete, check, cleanup steps as 'complete' with note
    couch = Couch()
    ingestion_doc = couch.dashboard_db[ingestion_doc_id]

    kwargs = {
        "delete_process/status": "complete",
        "delete_process/start_time": iso_utc_with_tz(),
        "delete_process/end_time": iso_utc_with_tz(),
        "delete_process/error": "Process not executed. See JSON/S3 export.",
        "check_counts_process/status": "complete",
        "check_counts_process/start_time": iso_utc_with_tz(),
        "check_counts_process/end_time": iso_utc_with_tz(),
        "check_counts_process/error": "Process not executed. See JSON/S3 "
                                      "export.",
        "dashboard_cleanup_process/status": "complete",
        "dashboard_cleanup_process/start_time": iso_utc_with_tz(),
        "dashboard_cleanup_process/end_time": iso_utc_with_tz(),
        "dashboard_cleanup_process/error": "Process not executed. See "
                                           "JSON/S3 export."
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return -1

    print "Ingestion complete!"


if __name__ == '__main__':
    main(sys.argv)
