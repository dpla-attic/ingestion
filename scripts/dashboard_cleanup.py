#!/usr/bin/env python
"""
Script to remove a provider's dashboard item-level documents whose ingestion
sequence is not in the last three ingestions.

Usage:
    $ python dashboard_cleanup.py ingestion_document_id
"""
import os
import sys
import argparse
from akara import logger
from datetime import datetime
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop

def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("ingestion_document_id", 
                        help="The ID of the ingestion document")

    return parser

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    couch = Couch()
    ingestion_doc = couch.dashboard_db[args.ingestion_document_id]
    if getprop(ingestion_doc, "delete_process/status") != "complete":
        print "Error, delete process did not complete"
        return -1

    # Update ingestion document
    kwargs = {
        "dashboard_cleanup_process/status": "running",
        "dashboard_cleanup_process/start_time": datetime.now().isoformat()
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return -1

    resp, total_deleted = couch.dashboard_cleanup(ingestion_doc)
    if resp == -1:
        status = "error"
        error_msg = "Error deleting documents; only %s deleted" % total_deleted
    else:
        status = "complete"
        error_msg = None
    print "Total dashboard documents deleted: %s" % total_deleted

    # Update ingestion document
    kwargs = {
        "dashboard_cleanup_process/status": status,
        "dashboard_cleanup_process/error": error_msg,
        "dashboard_cleanup_process/end_time": datetime.now().isoformat()
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return -1

    return 0 if status == "complete" else -1

if __name__ == '__main__':
    main(sys.argv)
