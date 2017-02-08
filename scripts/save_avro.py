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
    "namespace": "DPLA_MAP_v3.1.avro",
    "type": "record",
    "name": "enriched_record",
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
    try:
        parser = define_arguments()
        args = parser.parse_args(argv[1:])
        enrich_dir = get_enrich_dir(args.ingestion_document_id)
        schema = avro.schema.parse(avro_schema)
        total_items = write_avro(
            args.codec,
            enrich_dir,
            args.output_file,
            schema
        )
        print >> sys.stderr, "Saved %s documents" % total_items
        return 0

    except Exception, e:
        print >> sys.stderr, "Caught error: %s" % e.message
        return 1


def write_avro(codec, enrich_dir, output_filename, schema):
    with open(output_filename, "w") as outfile:
        writer = DataFileWriter(outfile, DatumWriter(), schema, codec)
        total_items = 0

        for enriched_file in os.listdir(enrich_dir):
            filename = os.path.join(enrich_dir, enriched_file)
            with open(filename, "r") as input_file:
                file_docs = json.loads(input_file.read())
                for key in file_docs:
                    doc = file_docs[key]
                    writer.append({"id": key, "document": json.dumps(doc)})
                    total_items += 1

            print >> sys.stderr, "Read file %s" % filename

        writer.close()
    return total_items


def get_enrich_dir(ingestion_document_id):
    couch = Couch()
    ingestion_doc = couch.dashboard_db[ingestion_document_id]

    if getprop(ingestion_doc, "enrich_process/status") != "complete":
        raise AssertionError(
            "Cannot save Avro files, enrich process did not complete")

    return getprop(ingestion_doc, "enrich_process/data_dir")


if __name__ == '__main__':
    retval = main(sys.argv)
    sys.exit(retval)
