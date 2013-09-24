#!/usr/bin/env python
"""
Script to fetch records from a provider.

Usage:
    $ python fetch_records.py ingestion_document_id
"""
import os
import sys
import uuid
import argparse
import tempfile
from akara import logger
from datetime import datetime
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.fetcher import create_fetcher
from dplaingestion.selector import getprop

def create_fetch_dir(provider):
    return tempfile.mkdtemp("_" + provider)

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

    # Update ingestion document
    fetch_dir = create_fetch_dir(ingestion_doc["provider"])
    kwargs = {
        "fetch_process/status": "running",
        "fetch_process/data_dir": fetch_dir,
        "fetch_process/start_time": datetime.now().isoformat()
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        logger.error("Error updating ingestion doc %s in %s" %
                     (ingestion_doc["id"], __name__))
        return -1

    error_msg = []
    fetcher = create_fetcher(ingestion_doc["profile_path"],
                             ingestion_doc["uri_base"])

    print "Fetching records for " + fetcher.provider
    total_fetched_records = 0
    for response in fetcher.fetch_all_data():
        if response["error"]:
            error_msg.extend(response["error"])
            print response["error"]
        else:
            # Write records to file
            filename = os.path.join(fetch_dir, str(uuid.uuid4()))
            with open(filename, "w") as f:
                f.write(json.dumps(response["data"]))
            print "Records written to " + filename
            total_fetched_records += len(getprop(response, "data/records"))

    logger.info("Total records fetched: %s" % total_fetched_records)

    # Update ingestion document
    try:
        os.rmdir(fetch_dir)
        # Error if fetch_dir was empty
        status = "error"
        error_msg = "Error, no records fetched"
        logger.error(error_msg)
    except:
        status = "complete"
    kwargs = {
        "fetch_process/status": status,
        "fetch_process/error": error_msg,
        "fetch_process/end_time": datetime.now().isoformat()
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        logger.error("Error updating ingestion doc %s in %s" %
                     (ingestion_doc["id"], __name__))
        return -1

    return 0 if status == "complete" else -1

if __name__ == '__main__':
    main(sys.argv)
