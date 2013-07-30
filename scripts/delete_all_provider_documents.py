#!/usr/bin/env python
"""
Script to delete all of a provider's documents from both the DPLA and
Dashboard databases.

Usage:
i   $ python scripts/delete_all_provider_documents.py profile_path
"""
import sys
import argparse
from amara.thirdparty import json
from dplaingestion.couch import Couch

def confirm_deletion(provider):
    prompt = "Are you sure you want to delete all DPLA and Dashboard " + \
             "documents for %s? yes | no\n" % provider
    while True:
        ans = raw_input(prompt).lower()
        if ans == "yes":
            return True
        elif ans == "no":
            return False
        else:
            print "Please enter yes or no"
            continue

def define_arguments():
    """Defines command line arguments for the current script
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("profile_path",
                        help="The path to the provider's profile", type=str)
    return parser

def main(argv):
    couch = Couch()
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    with open(args.profile_path, "r") as f:
        profile = json.load(f)

    provider = profile.get("name")
    if confirm_deletion(provider):
        couch._delete_all_provider_documents(provider)
    else:
        return False

if __name__ == "__main__":
    main(sys.argv)
