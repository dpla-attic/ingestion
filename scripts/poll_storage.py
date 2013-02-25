#!/usr/bin/env python
"""
Script to run pipeline against records pulled from a CouchDB view

Simple use cases:
    Download thumbnail ls for all records related to a profile that have not already been downloaded
    Validate all URLs within records on a profile, and flag records with URLs that don't resolve
    Perform geocoding for all records on a profile

Usage:
    $ python poll_storage.py ...
"""

import argparse
import sys

def define_arguments():
    parser = argparse.ArgumentParser()
    return parser

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

if __name__ == "__main__":
    main(sys.argv)
