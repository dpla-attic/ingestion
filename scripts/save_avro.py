#!/usr/bin/env python
"""
Script to save data from JSON files to an Avro file

Usage:
    $ python save_avro.py ingestion_document_id codec output_directory

To use snappy support you need to install snappy with your package manager,
    and pip install python-snappy

Homebrew package is "snappy", in Ubuntu you need libsnappy1 and libsnappy-dev

"""
import os
import sys
import argparse
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop

avro_schema = """
{
    "namespace": "ingestion1.avro",
    "type": "record",
    "name": "record",
    "fields": [
        { "name": "id",         "type": "string" },
        { "name": "document",   "type": "string" }
    ]
}"""


def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "ingestion_document_id",
        help="The ID of the ingestion document"
    )
    parser.add_argument(
        "codec",
        help="One of null, deflate, snappy (if installed)."
    )
    parser.add_argument(
        "output_file",
        help="The folder where the Avro records will save"
    )
    return parser


def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    schema = avro.schema.parse(avro_schema)
    writer = DataFileWriter(
        open(args.output_file, "w"),
        DatumWriter(),
        schema,
        args.codec
    )

    couch = Couch()
    ingestion_doc = couch.dashboard_db[args.ingestion_document_id]

    if getprop(ingestion_doc, "enrich_process/status") != "complete":
        print "Cannot save Avro files, enrich process did not complete"
        return -1

    enrich_dir = getprop(ingestion_doc, "enrich_process/data_dir")
    total_items = 0

    for enriched_file in os.listdir(enrich_dir):
        filename = os.path.join(enrich_dir, enriched_file)
        with open(filename, "r") as f:
            try:
                file_docs = json.loads(f.read())
                for key in file_docs:
                    doc = file_docs[key]
                    writer.append({"id": key, "document": json.dumps(doc)})
                    total_items += 1

            except Exception, e:
                print >> sys.stderr, "Error loading %s: %s" % (filename, e)
                break

        print >> sys.stderr, "Read file %s" % filename

    writer.close()
    print >> sys.stderr, "Saved %s documents" % total_items

if __name__ == '__main__':
    main(sys.argv)
