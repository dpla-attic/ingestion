#!/usr/bin/env python
"""
Script to add/update then build the views of a database.

Usage:
    $ python scripts/sync_couch_views.py <database_name>

    where database_name is either "dpla" or "dashboard"
"""
import sys
import time
import argparse
from dplaingestion.couch import Couch

def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    db_name_help = "The name of the database (either \"dpla\" or " + \
                   "\"dashboard\" for now)"
    parser.add_argument("database_name", help=db_name_help)

    return parser
    

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    couch = Couch()
    if args.database_name in ["dpla", "dashboard"]:
        couch.sync_views(args.database_name)
    else:
        print >> sys.stderr, "The database_name parameter should be " + \
                             "either \"dpla\" or \"dashboard\""

if __name__ == "__main__":
    main(sys.argv)
