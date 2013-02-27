#!/usr/bin/env python
"""
Script to run pipeline against records pulled from a CouchDB view

Simple use cases:
    Download thumbnail ls for all records related to a profile that have not already been downloaded
    Validate all URLs within records on a profile, and flag records with URLs that don't resolve
    Perform geocoding for all records on a profile

Usage:
    $ poll_storage.py [-h] [--filter FILTER] profile pipeline service
"""

import argparse
import sys

from amara.thirdparty import json, httplib2

def define_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("profile", help="The path to profile to be processed")
    parser.add_argument("pipeline", help="The name of an enrichment pipeline in the profile that contains the list of enrichments to be run")
    parser.add_argument("service", help="The URL of the enrichment service")
    parser.add_argument("--filter", help="Name or identifier for a CouchDB view that would limit the number of records to be processed")
    return parser

def enrichment_list(profile_path, pipeline_name):
    with open(profile_path) as f:
        profile_dict = json.load(f)
        return tuple(profile_dict[pipeline_name])

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])
    print enrichment_list(args.profile, args.pipeline)

if __name__ == "__main__":
    main(sys.argv)
