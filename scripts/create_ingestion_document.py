#!/usr/bin/env python
"""
Script to create an ingestion document

Usage:
    $ python create_ingestion_document.py profile_path
"""
import sys
import argparse
import ConfigParser
from akara import logger
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop

def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("profile_path",
                        help="The path to the profile to be processed")

    return parser

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    config_file = ("akara.ini")
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file))
    uri_base = "http://localhost:" + config.get("Akara", "Port")

    with open(args.profile_path, "r") as f:
        try:
            profile = json.load(f)
        except:
            print "Error, could not load profile in %s" % __name__
            return None
    provider = profile["name"]

    couch = Couch()
    latest_ingestion_doc = couch._get_last_ingestion_doc_for(provider)
    if latest_ingestion_doc and \
       getprop(latest_ingestion_doc, "delete_process/status") != "complete":
        error_msg = "Error, last ingestion did not complete. Review " + \
                    "dashboard document %s for errors." % \
                    latest_ingestion_doc["_id"]
        logger.error(error_msg)
        print error_msg
        return None

    ingestion_document_id = couch._create_ingestion_document(provider,
                                                             uri_base,
                                                             args.profile_path)
    logger.debug("Ingestion document %s created." % ingestion_document_id)

    return ingestion_document_id

if __name__ == '__main__':
    main(sys.argv)
