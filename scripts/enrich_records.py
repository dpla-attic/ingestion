#!/usr/bin/env python
"""
Script to enrich data from JSON files

Expected JSON structure: Array of objects

Usage:
    $ python enrich_records.py ingestion_document_id
"""
import os
import sys
import shutil
import tempfile
import argparse
from akara import logger
from datetime import datetime
from amara.thirdparty import json
from amara.thirdparty import httplib2
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop
from dplaingestion.utilities import make_tarfile

H = httplib2.Http('/tmp/.pollcache')
H.force_exception_as_status_code = True

def create_enrich_dir(provider):
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
    if getprop(ingestion_doc, "fetch_process/status") != "complete":
        print "Cannot enrich, fetch process did not complete"
        return -1

    # Update ingestion document
    status = "running"
    enrich_dir = create_enrich_dir(ingestion_doc["provider"])
    kwargs = {
        "enrich_process/status": status,
        "enrich_process/data_dir": enrich_dir,
        "enrich_process/start_time": datetime.now().isoformat(),
        "enrich_process/end_time": None,
        "enrich_process/error": None,
        "enrich_process/total_items": None,
        "enrich_process/total_collections": None,
        "enrich_process/missing_id": None,
        "enrich_process/missing_source_resource": None
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return -1

    # Set the headers sent with the enrich request
    with open(ingestion_doc["profile_path"], "r") as f:
        profile = json.loads(f.read())
    headers = {
        "Source": ingestion_doc["provider"],
        "Content-Type": "application/json",
        "Pipeline-Item": ",".join(profile["enrichments_item"]),
        "Pipeline-Coll": ",".join(profile["enrichments_coll"])
    }

    errors = []
    fetch_dir = getprop(ingestion_doc, "fetch_process/data_dir")

    # Counts for logger info
    enriched_items = 0
    enriched_colls = 0
    missing_id = 0
    missing_source_resource = 0

    file_count = 0
    files = os.listdir(fetch_dir)
    for filename in files:
        file_count += 1
        filepath = os.path.join(fetch_dir, filename)
        with open(filepath, "r") as f:
            try:
                data = json.loads(f.read())
            except:
                errors.append("Error loading " + filepath)
                break

        # Enrich
        print "Enriching file %s (%s of %s)" % (filepath, file_count,
                                                len(files))
        enrich_path = ingestion_doc["uri_base"] + "/enrich"
        resp, content = H.request(enrich_path, "POST", body=json.dumps(data),
                                  headers=headers)
        if not resp["status"].startswith("2"):
            errors.append("Error (status %s) enriching data from %s" %
                          (resp["status"], filepath))
            print "Stopped enrichment process: %s" % errors
            status = "error"
            break

        data = json.loads(content)
        enriched_records = data["enriched_records"]

        # Update counts
        enriched_items += data["enriched_item_count"]
        enriched_colls += data["enriched_coll_count"]
        missing_id += data["missing_id_count"]
        missing_source_resource += data["missing_source_resource_count"]
        errors.extend(data["errors"])

        # Write enriched data to file
        with open(os.path.join(enrich_dir, filename), "w") as f:
            f.write(json.dumps(enriched_records))

    print "Enriched items: %s" % enriched_items
    print "Enriched collections: %s" % enriched_colls
    print "Missing ID: %s" % missing_id
    print "Missing sourceResource: %s" % missing_source_resource

    # Update ingestion document
    if not status == "error":
        status = "complete"
    kwargs = {
        "enrich_process/status": status,
        "enrich_process/error": errors,
        "enrich_process/end_time": datetime.now().isoformat(),
        "enrich_process/total_items": enriched_items,
        "enrich_process/total_collections": enriched_colls,
        "enrich_process/missing_id": missing_id,
        "enrich_process/missing_source_resource": missing_source_resource
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return -1

    # Compress fetch directory, then delete
    #make_tarfile(fetch_dir)
    #shutil.rmtree(fetch_dir)

    return 0 if status == "complete" else -1

if __name__ == '__main__':
    main(sys.argv)
