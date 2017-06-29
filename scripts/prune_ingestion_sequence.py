#!/usr/bin/env python
"""
Script for deleting items for a given provider and sequence number.

Unless the argument 'no_dry_run' is given as the last argument, this will
operate in a non-destructive mode and just print out the records it would
delete for safety. Otherwise, it will delete them.

NOTE: There is a bug with Elasticsearch actually processing all the DELETE
requests, at least on my VM. This script may need to be run more than once
depending on how many records there are to delete.

Usage:
    $ python prune_ingestion_sequence.py ingestion_document_id\
     sequence_number no_dry_run|anything

"""

import sys
import argparse
from amara.thirdparty import json
import ConfigParser
import requests
import urllib


def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("profile_path",
                        help="The path to the profile to be processed")
    parser.add_argument("ingestion_sequence",
                        help="The ingestionSequence number to remove")
    parser.add_argument("dry_run",
                        help="Only deletes the records if this argument is "
                             "'no_dry_run'. Otherwise, prints the potentially "
                             "deleted ids.")

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
            print "Error, couldn't read elasticsearch index from config: %s" \
                  % err
            return None


def handle_destructive_results(base_url, results):
    for hit in results:
        _type = str(hit["_type"])
        _id = urllib.quote_plus(str(hit["_id"]))
        url = "%s/%s/%s" % (base_url, _type, _id)
        response = requests.delete(url)
        response.raise_for_status()
        print(str(response.text))


def handle_dry_run_results(base_url, results):
    for hit in results:
        _type = str(hit["_type"])
        _id = str(hit["_id"])
        print("%s | %s | %s" % (base_url, _type, _id))


def extract_hits(data):
    inner_results = []
    for hit in data["hits"]["hits"]:
        _type = str(hit["_type"])
        _id = str(hit["_id"])
        inner_results.append({"_type": _type, "_id": _id})
    return inner_results


def has_results(data):
    if "hits" in data and "hits" in data["hits"]:
        return len(data["hits"]["hits"]) > 0
    else:
        return False


def load_next_page(search_query_url, query, _from, size):
    inner_query_url = "%s?from=%s&size=%s" % (search_query_url, _from, size)
    response = requests.post(inner_query_url, json=query)
    response.raise_for_status()
    return json.loads(response.text)


def get_query(provider_id, ingestion_sequence):
    quoted_provider_id = provider_id.replace(":", "\\:").replace("/", "\\/")
    return \
        {
            "fields": ["_id", "provider.@id", "ingestionSequence", "type"],
            "query": {
                "field": {"provider.@id": quoted_provider_id},
                "constant_score": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"term": {"ingestionSequence":
                                          ingestion_sequence}}

                            ]
                        }
                    }
                }
            }
        }


def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])
    ingestion_sequence = args.ingestion_sequence
    provider_id = get_provider_id(args.profile_path)
    base_url = get_elasticsearch_url()
    search_query_url = base_url + "/_search"

    page_size = 1000
    page_count = 0
    query = get_query(provider_id, ingestion_sequence)
    data = load_next_page(search_query_url, query, page_count, page_size)

    results = []

    while (has_results(data)):
        results = results + extract_hits(data)
        page_count += 1
        data = load_next_page(
            search_query_url,
            query,
            page_count * page_size,
            page_size
        )

    if args.dry_run == "no_dry_run":
        handle_destructive_results(base_url, results)
    else:
        handle_dry_run_results(base_url, results)

    return 0


if __name__ == '__main__':
    main(sys.argv)
