#!/usr/bin/env python
"""
Script to add/update then build the views of a database. By default,
it will sync QA views required by the Content QA application. To disable
syncing of QA views, set CouchDb.SyncQAViews to "False" in akara.ini.

Usage:
    $ python scripts/sync_couch_views.py <database_name>

    where database_name is either "dpla", "dashboard", or "bulk_download"
"""
import sys
import time
import argparse
import ConfigParser
from dplaingestion.couch import Couch

# ConfigParser.ConfigParser().getboolean() expects a string
config = ConfigParser.ConfigParser({"SyncQAViews": "True"})
config.readfp(open('akara.ini'))

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
    sync_qa_views = config.getboolean('CouchDb', 'SyncQAViews')
    couch = Couch()
    database_names = ["dpla", "dashboard", "bulk_download"]
    if args.database_name in database_names:
        couch.sync_views(args.database_name, sync_qa_views)
    else:
        print >> sys.stderr, "The database_name parameter should be " + \
                             "either %s" % " or ".join(database_names)

if __name__ == "__main__":
    main(sys.argv)
