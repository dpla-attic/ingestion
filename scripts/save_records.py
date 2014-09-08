#!/usr/bin/env python
"""
Script to save data from JSON files to the CouchDB database

Usage:
    $ python save_records.py ingestion_document_id
"""
import os
import sys
import shutil
import argparse
import ConfigParser
from akara import logger
from datetime import datetime
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop
from dplaingestion.utilities import make_tarfile, iso_utc_with_tz


def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("ingestion_document_id", 
                        help="The ID of the ingestion document")
    parser.add_argument("--no-backup", dest="backup", action="store_false",
                        help="skip the backup process")
    parser.set_defaults(backup=True)

    return parser

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    # ConfigParser.ConfigParser().getboolean() expects a string
    config = ConfigParser.ConfigParser({"SyncQAViews": "True"})
    config.readfp(open('akara.ini'))
    sync_qa_views = config.getboolean('CouchDb', 'SyncQAViews')

    batch_size = 500

    couch = Couch()
    ingestion_doc = couch.dashboard_db[args.ingestion_document_id]
    if getprop(ingestion_doc, "enrich_process/status") != "complete":
        print "Cannot save, enrich process did not complete"
        return -1

    # Update ingestion document
    kwargs = {
        "save_process/status": "running",
        "save_process/start_time": iso_utc_with_tz(),
        "save_process/end_time": None,
        "save_process/error": None,
        "save_process/total_saved": None
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return -1

    # Back up provider data
    if args.backup:
        resp = couch._back_up_data(ingestion_doc)

        if resp == -1:
            # Fatal error, do not continue with save process
            kwargs = {
                "save_process/status": "error",
                "save_process/end_time": iso_utc_with_tz(),
                "save_process/error": "Error backing up DPLA records"
            }
            couch.update_ingestion_doc(ingestion_doc, **kwargs)
            return resp

    error_msg = None
    enrich_dir = getprop(ingestion_doc, "enrich_process/data_dir")
    total_items = 0
    total_collections = 0
    sync_point = 5000
    docs = {}
    for file in os.listdir(enrich_dir):
        filename = os.path.join(enrich_dir, file)
        with open(filename, "r") as f:
            try:
                file_docs = json.loads(f.read())
            except:
                error_msg = "Error loading " + filename
                break

        # Save when docs is about to exceed the batch size
        print >> sys.stderr, "Read file %s" % filename
        if docs and len(docs) + len(file_docs) > batch_size:
            resp, error_msg = couch.process_and_post_to_dpla(docs,
                                                             ingestion_doc)
            if resp == -1:
                docs = None
                break

            items = len([doc for doc in docs.values() if
                         doc.get("ingestType") == "item"])
            total_items += items
            total_collections += len(docs) - items
            print "Saved %s documents" % (total_items + total_collections)

            if total_items > sync_point:
                print "Syncing views"
                couch.sync_views(couch.dpla_db.name, sync_qa_views)
                sync_point = total_items + 10000

            # Set docs for the next iteration
            docs = file_docs
        else:
            docs.update(file_docs)

    # Last save
    if docs:
        resp, error_msg = couch.process_and_post_to_dpla(docs,
                                                         ingestion_doc)
        if resp != -1:
            items = len([doc for doc in docs.values() if
                         doc.get("ingestType") == "item"])
            total_items += items
            total_collections += len(docs) - items
            print "Saved %s documents" % (total_items + total_collections)
            print "Syncing views"
            couch.sync_views(couch.dpla_db.name, sync_qa_views)

    print "Total items: %s" % total_items
    print "Total collections: %s" % total_collections

    if error_msg:
        status = "error"
    else:
        status = "complete"
    kwargs = {
        "save_process/status": status,
        "save_process/error": error_msg,
        "save_process/end_time": iso_utc_with_tz(),
        "save_process/total_items": total_items,
        "save_process/total_collections": total_collections
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return -1

    # Compress enrich dir, then delete
    make_tarfile(enrich_dir)
    shutil.rmtree(enrich_dir)

    return 0 if status == "complete" else -1

if __name__ == '__main__':
    main(sys.argv)
