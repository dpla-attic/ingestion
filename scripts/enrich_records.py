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
import threading
import time
import Queue
import ConfigParser
import httplib
from akara import logger
from datetime import datetime
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop
from dplaingestion.utilities import make_tarfile, iso_utc_with_tz

config = ConfigParser.ConfigParser()
config.readfp(open('akara.ini'))
threads_working = 0

class EnrichmentError(Exception):
    pass


class EnricherThread(threading.Thread):
    """
    Thread that enriches files in the given queue
    """

    def __init__(self, queue, errors, dashboard_errors, ingestion_doc,
                 profile, stats, enrich_dir):
        """
        queue:            Queue object
        errors:           list of thread-related error messages
        dashboard_errors: list of error messages to save to dashboard database
        ingestion_doc:    parsed JSON ingestion document from dashboard database
        profile:          parsed JSON from provider profile file
        stats:            dict of statistics for whole run
        enrich_dir:       name of output directory
        """
        self.queue = queue
        self.errors = errors
        self.dashboard_errors = dashboard_errors
        self.enrich_dir = enrich_dir
        self.headers = {
            "Source": ingestion_doc["provider"],
            "Content-Type": "application/json",
            "Pipeline-Item": ",".join(profile["enrichments_item"]),
            "Pipeline-Coll": ",".join(profile["enrichments_coll"])
        }
        self.stats = stats
        threading.Thread.__init__(self)

    def run(self):
        while True:
            try:
                filename = self.queue.get(True)
                self.enrich(filename)
            except EnrichmentError as e:
                msg = "Could not enrich %s: %s\n" % (filename, e.message)
                self.errors.append(msg)
                break
            except Exception as e:
                msg = "Unexpected exception %s: %s" % (e.__class__, e.message)
                self.errors.append(msg)
                break

    def enrich(self, input_filename):
        """Enrich the records in the given file"""

        global threads_working
        threads_working += 1

        with open(input_filename, "r") as f:
            data = json.loads(f.read())

        # Enrich
        port = int(config.get('Akara', 'Port'))
        conn = httplib.HTTPConnection("localhost", port)
        conn.request("POST", "/enrich", json.dumps(data), self.headers)
        resp = conn.getresponse()

        if not resp.status == 200:
            raise EnrichmentError("Error (status %s)" % resp.status)

        data = json.loads(resp.read())
        enriched_records = data["enriched_records"]

        # Update counts
        self.stats['enriched_items'] += data["enriched_item_count"]
        self.stats['enriched_colls'] += data["enriched_coll_count"]
        self.stats['missing_id'] += data["missing_id_count"]
        self.stats['missing_source_resource'] += \
                data["missing_source_resource_count"]

        self.dashboard_errors.extend(data["errors"])

        # Write enriched data to file
        basename = os.path.basename(input_filename)
        with open(os.path.join(self.enrich_dir, basename), "w") as f:
            f.write(json.dumps(enriched_records))

        threads_working -= 1


def queue_and_errors(ingestion_doc, profile, stats, enrich_dir):
    """
    Spawn threads to process files from queue, and return the queue and a
    list to hold any errors
    """
    global config
    queue_size = int(config.get('Enrichment', 'QueueSize'))
    thread_count = int(config.get('Enrichment', 'ThreadCount'))
    queue = Queue.Queue(queue_size)
    errors = []
    dashboard_errors = []
    threads = [EnricherThread(queue,
                              errors,
                              dashboard_errors,
                              ingestion_doc,
                              profile,
                              stats,
                              enrich_dir)
               for i in range(thread_count)]
    for t in threads:
        t.daemon = True
        t.start()
    return queue, errors

def print_errors_thrown(thread_errors):
    if thread_errors:
        print >> sys.stderr, "\n".join([s for s in thread_errors])
        return True
    else:
        return False

def create_enrich_dir(provider):
    return tempfile.mkdtemp("_" + provider)

def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("ingestion_document_id", 
                        help="The ID of the ingestion document")

    return parser

def main(argv):
    global threads_working
    parser = define_arguments()
    args = parser.parse_args(argv[1:])
    couch = Couch()
    ingestion_doc = couch.dashboard_db[args.ingestion_document_id]
    fetch_dir = getprop(ingestion_doc, "fetch_process/data_dir")
    enrich_dir = create_enrich_dir(ingestion_doc["provider"])
    if getprop(ingestion_doc, "fetch_process/status") != "complete":
        print >> sys.stderr, "Cannot enrich, fetch process did not complete"
        return 1

    # Update ingestion document
    status = "running"
    kwargs = {
        "enrich_process/status": status,
        "enrich_process/data_dir": enrich_dir,
        "enrich_process/start_time": iso_utc_with_tz(),
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
        print >> sys.stderr, "Error updating ingestion document " + \
                ingestion_doc["_id"]
        return 1

    with open(ingestion_doc["profile_path"], "r") as f:
        profile = json.loads(f.read())

    # Counts for logger info
    stats = {'enriched_items': 0,
             'enriched_colls': 0,
             'missing_id': 0,
             'missing_source_resource': 0
            }

    # Initialize queue and threads
    queue, thread_errors = queue_and_errors(ingestion_doc=ingestion_doc,
                                            profile=profile,
                                            stats=stats,
                                            enrich_dir=enrich_dir)
    # Initialize list of input files
    listing = os.listdir(fetch_dir)
    # Initialize counters and statistics
    dashboard_errors = []
    file_count = 0
    status = None
    total_files = len(listing)
    files = iter(listing)

    try:
        # Keep the queue full of filenames
        while True:
            time.sleep(0.25)
            try:
                if print_errors_thrown(thread_errors):
                    dashboard_errors.extend(thread_errors)
                    raise Exception()
                if not queue.full():
                    basename = files.next()
                    filename = os.path.join(fetch_dir, basename)
                    file_count += 1
                    print "Enqueuing: %s (%s of %s)" % \
                            (filename, file_count, total_files)
                    queue.put(filename)
            except StopIteration:
                break
        # Wait for queue to be empty before returning
        while True:
            if queue.empty() and not threads_working:
                if not print_errors_thrown(thread_errors):
                    break
                else:
                    dashboard_errors.extend(thread_errors)
                    raise Exception()
            time.sleep(0.25)
    except KeyboardInterrupt:
        status = "error"
        msg = "\nCaught keyboard interrupt"
        print >> sys.stderr, msg
        dashboard_errors.append(msg)
    except Exception as e:
        if e.message:
            print >> sys.stderr, e.message
        status = "error"
        dashboard_errors.append(e.message)
    finally:
        print "Enriched items: %s" % stats['enriched_items']
        print "Enriched collections: %s" % stats['enriched_colls']
        print "Missing ID: %s" % stats['missing_id']
        print "Missing sourceResource: %s" % stats['missing_source_resource']
        if not status == "error":
            status = "complete"
        # Prepare fields for ingestion document update
        couch_kwargs = {
            "enrich_process/status": status,
            "enrich_process/error": dashboard_errors,
            "enrich_process/end_time": iso_utc_with_tz(),
            "enrich_process/total_items": stats['enriched_items'],
            "enrich_process/total_collections": stats['enriched_colls'],
            "enrich_process/missing_id": stats['missing_id'],
            "enrich_process/missing_source_resource": \
                    stats['missing_source_resource']
        }

    try:
        # Update ingestion document
        couch.update_ingestion_doc(ingestion_doc, **couch_kwargs)
    except:
        print >> sys.stderr, "Error updating ingestion document " + \
                             ingestion_doc["_id"]
        return 1

    return 0 if status == "complete" else 1

if __name__ == '__main__':
    rv = main(sys.argv)
    sys.exit(rv)


