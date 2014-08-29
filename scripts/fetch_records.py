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
import threading
import Queue
import time
import traceback
from akara import logger
from datetime import datetime
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop
from dplaingestion.utilities import iterify, iso_utc_with_tz
from dplaingestion.create_fetcher import create_fetcher


threads_working = 0


class FetchError(Exception):
    pass

class FetcherThread(threading.Thread):
    """
    Thread that fetches records for sets that it gets from a queue
    """

    def __init__(self, queue, fetcher, t_errors, d_errors, fetch_dir,
                 stats):
        """
        queue:      queue of set IDs
        fetcher:    fetcher instance
        t_errors:   list of thread-related error messages
        d_errors:   list of data-related errors to store in the dashboard db
        fetch_dir:  directory in which to store files
        stats:      dictionary of stats to update
        """
        self.queue = queue
        self.fetcher = fetcher
        self.t_errors = t_errors
        self.d_errors = d_errors
        self.fetch_dir = fetch_dir
        self.stats = stats
        threading.Thread.__init__(self)

    def run(self):
        while True:
            try:
                set_id = self.queue.get(True)
                self.fetch(set_id)
            except FetchError as e:
                msg = "Could not enrich %s: %s\n" % (set_id, e.message)
                self.t_errors.append(msg)
                break
            except Exception as e:
                tb = traceback.format_exc(5)
                msg = "Unexpected exception %s: %s\n%s" % (e.__class__,
                                                           e.message,
                                                           tb)
                self.t_errors.append(msg)
                break

    def fetch(self, set_id):
        """Fetch all records for the given set"""
        global threads_working
        threads_working += 1
        rv = fetch_all_for_set(set_id,
                               self.fetcher,
                               self.fetch_dir) 
        self.stats["total_items"] += rv["total_items"]
        self.stats["total_collections"] += rv["total_collections"]
        self.d_errors += rv["errors"]
        threads_working -= 1

def queue_and_errors(num_threads, in_doc, config_file, fetch_dir, stats):
    queue = Queue.Queue(num_threads)
    t_errors = []
    d_errors = []
    threads = [FetcherThread(queue,
                             create_fetcher(in_doc["profile_path"],
                                            in_doc["uri_base"],
                                            config_file),
                             t_errors,
                             d_errors,
                             fetch_dir,
                             stats)
               for i in range(num_threads)]
    for t in threads:
        t.daemon = True
        t.start()
    return queue, t_errors, d_errors

def print_errors_thrown(th_errors):
    if th_errors:
        print >> sys.stderr, "\n".join([s for s in th_errors])
        return True
    else:
        return False

def create_fetch_dir(provider):
    return tempfile.mkdtemp("_" + provider)

def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("ingestion_document_id", 
                        help="The ID of the ingestion document")

    return parser


def fetch_all_for_set(set, fetcher, fetch_dir):
    """
    Fetch all records (and create all fetch files) for the given set and
    fetcher, or all sets if this value is empty.

    Returns a dictionary of errors and statistics.
    """
    errors = []
    total_items = 0
    total_collections = 0
    # Thread obj needs:  fetcher, error_msg, fetch_dir
    for response in fetcher.fetch_all_data(set):
        if response["errors"]:
            errors.extend(iterify(response["errors"]))
            print response["errors"]
        if response["records"]:
            # Write records to file
            filename = os.path.join(fetch_dir, str(uuid.uuid4()))
            with open(filename, "w") as f:
                f.write(json.dumps(response["records"]))

            items = len([record for record in response["records"] if not
                         record.get("ingestType") == "collection"])
            total_items += items
            total_collections += len(response["records"]) - items
    return {
        "errors": errors,
        "total_items": total_items,
        "total_collections": total_collections
        }    

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    couch = Couch()
    ingestion_doc = couch.dashboard_db[args.ingestion_document_id]

    # TODO:  profile should be passed to create_fetcher, below.
    #        We need profile in this scope, and the file shouldn't have to
    #        be opened again by create_fetcher.
    with open(ingestion_doc["profile_path"], "r") as f:
        profile = json.load(f)

    # Update ingestion document
    fetch_dir = create_fetch_dir(ingestion_doc["provider"])
    kwargs = {
        "fetch_process/status": "running",
        "fetch_process/data_dir": fetch_dir,
        "fetch_process/start_time": iso_utc_with_tz(),
        "fetch_process/end_time": None,
        "fetch_process/error": None,
        "fetch_process/total_items": None,
        "fetch_process/total_collections": None
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        logger.error("Error updating ingestion doc %s in %s" %
                     (ingestion_doc["_id"], __name__))
        return -1

    error_msg = []
    config_file = "akara.ini"

    fetcher = create_fetcher(ingestion_doc["profile_path"],
                             ingestion_doc["uri_base"],
                             config_file)

    print "Fetching records for %s" % ingestion_doc["provider"]
    stats = {
        "total_items": 0,
        "total_collections": 0
    }
    try:
        threads = int(profile.get("fetcher_threads")) or 1
        print "Threads: %d" % threads
    except:
        print >> sys.stderr, "Can not determine fetcher threads, so using 1"
        threads = 1
    sets = None
    sets_supported = (profile.get("sets") != "NotSupported")
    if threads > 1 and sets_supported and hasattr(fetcher, "fetch_sets"):
        sets_err, s = fetcher.fetch_sets()
        if s:
            sets = iter(s)
        else:
            print >> sys.stderr, "Could not get sets: ", sets_err
            return -1
        queue, th_errors, d_errors = queue_and_errors(threads,
                                                      ingestion_doc,
                                                      config_file,
                                                      fetch_dir,
                                                      stats)
        status = None
        try:
            while True:
                time.sleep(0.1)
                try:
                    if print_errors_thrown(th_errors):
                        d_errors.extend(th_errors)
                        raise Exception()
                    if not queue.full():
                        queue.put(sets.next())
                except StopIteration:
                    break
            # Wait for queue to be empty before returning
            while True:
                if queue.empty() and not threads_working:
                    if not print_errors_thrown(th_errors):
                        break
                    else:
                        d_errors.extend(th_errors)
                        raise Exception()
                time.sleep(0.1)
        except KeyboardInterrupt:
            status = "error"
            msg = "\nCaught keyboard interrupt"
            print >> sys.stderr, msg
            d_errors.append(msg)
        except Exception as e:
            if e.message:
                print >> sys.stderr, e.message
            status = "error"
            d_errors.append(e.message)
        finally:
            if not status == "error":
                status = "complete"
    else:  # not threads
        rv = fetch_all_for_set(None, fetcher, fetch_dir)
        stats["total_items"] += rv["total_items"]
        stats["total_collections"] += rv["total_collections"]
        error_msg += rv["errors"]

    print "Total items: %s" % stats["total_items"]
    print "Total collections: %s" % stats["total_collections"]


    # Update ingestion document
    try:
        os.rmdir(fetch_dir)
        # Error if fetch_dir was empty
        status = "error"
        error_msg.append("Error, no records fetched")
        logger.error(error_msg)
    except:
        status = "complete"
    kwargs = {
        "fetch_process/status": status,
        "fetch_process/error": error_msg,
        "fetch_process/end_time": iso_utc_with_tz(),
        "fetch_process/total_items": stats["total_items"],
        "fetch_process/total_collections": stats["total_collections"]
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        logger.error("Error updating ingestion doc %s in %s" %
                     (ingestion_doc["_id"], __name__))
        return -1

    return 0 if status == "complete" else -1

if __name__ == '__main__':
    main(sys.argv)
