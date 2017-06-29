#!/usr/bin/env python
"""
Script to facet on the ingesiton sequence number for a given provider
to help detect when data that should have been deleted from the index remains.

Requires that the ES index url exist in akara.ini, like:

[Elasticsearch]
Index=http://search-prod1:9200/dpla_alias

Usage:
    $ python check_ingestion_sequences.py path_to_profile
"""

import sys
import argparse
from amara.thirdparty import json
import ConfigParser
import requests


def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("profile_path",
                        help="The path to the profile to be processed")

    return parser


def get_provider_id(profile_path):
    with open(profile_path, "r") as f:
        try:
            profile = json.load(f)
            return profile['contributor']['@id']
        except Exception, err:
            print "Error, could not load profile in %s: %s" % (__name__, err)
            return None


def get_elasticsearch_url():
    config = ConfigParser.ConfigParser()

    with open("akara.ini") as f:
        try:
            config.readfp(f)
            return config.get("Elasticsearch", "Index")
        except Exception, err:
            print "Error, couldn't read elasticsearch index from config: " \
                  + str(err)
            return None


def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])
    provider_id = get_provider_id(args.profile_path)
    quoted_provider_id = provider_id.replace(":", "\\:").replace("/", "\\/")
    base_url = get_elasticsearch_url()

    query = {
        "size": 0,
        "query": {
            "field": {"provider.@id": quoted_provider_id}
        },
        "facets": {
            "ingestionSequence": {
                "terms": {
                    "field": "ingestionSequence",
                    "size": 10
                }
            }
        }
    }

    query_url = base_url + "/_search"
    response = requests.post(query_url, json=query)
    response.raise_for_status()
    data = json.loads(response.text)
    facets = data["facets"]["ingestionSequence"]
    print("")
    print("Results for %s:" % provider_id)

    for hit in facets["terms"]:
        print("%s\t%s" % (hit['term'], hit['count']))

    print("%s\t%s" % ("Total", facets['total']))
    print("%s\t%s" % ("Missing", facets['missing']))

    return 0


if __name__ == '__main__':
    main(sys.argv)
