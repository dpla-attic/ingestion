#!/usr/bin/env python
"""
Script to save enriched data as Elasticsearch compatible JSON-L to S3

Usage:
    $ python save_s3.py ingestion_document_id

"""
import argparse
import datetime
import sys
import traceback
import boto3

from datetime import datetime
from pytz import timezone
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop
from akara import logger

import save_jsonl


def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "ingestion_document_id",
            help="The ID of the ingestion document"
    )
    return parser


def main(argv):
    try:
        parser = define_arguments()
        args = parser.parse_args(argv[1:])

        tmp_out = "/tmp/%s.json" % args.ingestion_document_id

        # Use provider name from name property in ingest profile/CouchDB doc
        name = get_name(args.ingestion_document_id)

        tz = timezone('EST')
        d = datetime.now(tz)
        date_time = d.strftime("%Y%m%d_%H%M%S")

        s3_out = "%s/jsonl/%s-%s-MAP3_1.IndexRecord.jsonl/batch1.json" \
                 % (name, date_time, name)

        save_jsonl.main([None, args.ingestion_document_id, tmp_out])
        moveFile(tmp_out, s3_out)
    except Exception, e:
        traceback.print_exc(file=sys.stdout)
        print >> sys.stderr, "Caught error: %s" % e
        return 1


def moveFile(src, dest):
    s3 = boto3.session.Session().client("s3")
    config = boto3.s3.transfer.TransferConfig()
    transfer = boto3.s3.transfer.S3Transfer(client=s3, config=config)
    transfer.upload_file(src, 'dpla-master-dataset', dest)
    print("Saved JSON to s3://dpla-master-dataset/%s" % dest)


def get_name(ingestion_document_id):
    couch = Couch()
    ingestion_doc = couch.dashboard_db[ingestion_document_id]

    if not getprop(ingestion_doc, "provider"):
        raise AssertionError("Cannot get provider name from Couch doc")

    return getprop(ingestion_doc, "provider")


if __name__ == '__main__':
    retval = main(sys.argv)
    sys.exit(retval)
