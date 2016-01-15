#!/usr/bin/env python
"""
Script to upload a provider's data to Rackspace and update it's bulk_download
database document

Usage:
    $ python upload_bulk_data ingestion_document_id
"""
import os
import sys
import argparse
import export_database
from akara import logger
from datetime import datetime
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop
from dplaingestion.utilities import iso_utc_with_tz

def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("ingestion_document_id", 
                        help="The ID of the ingestion document")

    return parser

def main(argv):
    print "WARNING: Bulk data is now exported/maintained using elasticdump."
    print "See https://github.com/dpla/automation/blob/develop/ansible/roles/exporter/files/export-provider.sh"

    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    couch = Couch()
    ingestion_doc = couch.dashboard_db[args.ingestion_document_id]
    if getprop(ingestion_doc,
               "dashboard_cleanup_process/status") != "complete":
        print "Error, dashboard cleanup process did not complete"
        return -1

    # Update ingestion document
    kwargs = {
        "upload_bulk_data_process/status": "running",
        "upload_bulk_data_process/start_time": iso_utc_with_tz(),
        "upload_bulk_data_process/end_time": None,
        "upload_bulk_data_process/error": None,
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return -1

    # TODO: as in the fetch_records.py script, we need profile in this scope
    #       and the file shouldn't have to be opened again 
    with open(ingestion_doc["profile_path"], "r") as profile:
        contributor = getprop(json.load(profile), "contributor/name")

    resp = export_database.main([None, "source", contributor, "upload"])
    if resp == -1:
        status = "error"
        error_msg = "Error uploading bulk data"
    else:
        status = "complete"
        error_msg = None

    # Update ingestion document
    kwargs = {
        "upload_bulk_data_process/status": status,
        "upload_bulk_data_process/error": error_msg,
        "upload_bulk_data_process/end_time": iso_utc_with_tz()
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return -1

    return 0 if status == "complete" else -1

if __name__ == '__main__':
    main(sys.argv)
