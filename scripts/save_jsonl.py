#!/usr/bin/env python
"""
Script to save enriched data as Elasticsearch compatible JSON-L

Usage:
    $ python save_jsonl.py ingestion_document_id output_directory

"""
import argparse
import io
import os
import sys
import traceback

from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop


def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "ingestion_document_id",
        help="The ID of the ingestion document"
    )
    parser.add_argument(
        "output_file",
        help="The jsonl file to be saved"
    )
    return parser


def main(argv):
    try:
        parser = define_arguments()
        args = parser.parse_args(argv[1:])
        enrich_dir = get_enrich_dir(args.ingestion_document_id)
        total_items = write_jsonl(
            enrich_dir,
            args.output_file,
        )
        print >> sys.stderr, "Saved %s documents" % total_items
        return 0

    except Exception, e:
        traceback.print_exc(file=sys.stdout)
        print >> sys.stderr, "Caught error: %s" % e
        return 1


def write_jsonl(enrich_dir, output_filename):
    with io.open(output_filename, 'w', encoding='utf-8') as f:
        total_items = 0
        for enriched_file in os.listdir(enrich_dir):
            filename = os.path.join(enrich_dir, enriched_file)
            with open(filename, "r") as input_file:
                file_docs = json.loads(input_file.read())
                for key in file_docs:
                    doc = file_docs[key]
                    if doc['ingestType'] == 'item':
                        f.write(unicode(
                            json.dumps(
                                {'_type': 'item',
                                 '_id': doc['_id'],
                                '_source': doc
                                },
                                ensure_ascii=False) + "\n"))
                        total_items += 1

            print >> sys.stderr, "Read file %s" % filename
    return total_items


def get_enrich_dir(ingestion_document_id):
    couch = Couch()
    ingestion_doc = couch.dashboard_db[ingestion_document_id]

    if getprop(ingestion_doc, "enrich_process/status") != "complete":
        raise AssertionError(
            "Cannot save JSON-L file, enrich process did not complete")

    return getprop(ingestion_doc, "enrich_process/data_dir")


if __name__ == '__main__':
    retval = main(sys.argv)
    sys.exit(retval)
