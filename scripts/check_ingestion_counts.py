#!/usr/bin/env python
"""
Script to check the added, changed, and removed counts of an ingestion 
document and email an alert if any threshold has been exceeded

Usage:
    $ python check_ingestion_counts.py ingestion_document_id
"""
import os
import sys
import argparse
import traceback
from akara import logger
from datetime import datetime
from amara.thirdparty import json
from dplaingestion.couch import Couch
import smtplib
from email.mime.text import MIMEText
from dateutil import parser as dateparser
import ConfigParser
from dplaingestion.utilities import iso_utc_with_tz


def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("ingestion_document_id", 
                        help="The ID of the ingestion document")

    return parser

def alerts(i_doc):
    """Return a string for the alerts part of the email

    i_doc: the dict-like object from Couch.dashboard_db for the ingestion doc.
    """
    alerts = []
    count_type = ('Added', 'Changed', 'Deleted')
    for ct in count_type:
        count = int(i_doc['count' + ct])
        threshold = int(i_doc['thresholds'][ct.lower()])
        if count > threshold:
            alerts.append("  * %d items %s exceeds threshold of %d."
                          % (count, ct.lower(), threshold))
    if alerts:
        return "Alerts:\n%s" % "\n".join(alerts)
    else:
        return "There were no alerts."

def statistics(i_doc):
    """Return a string for the statistics part of the email

    i_doc: the dict-like object from Couch.dashboard_db for the ingestion doc.
    """
    stats = []
    stats.append("  * Records added: %d" % num(i_doc['countAdded']))
    stats.append("  * Records changed: %d" % num(i_doc['countChanged']))
    stats.append("  * Records deleted: %d" % num(i_doc['countDeleted']))
    stats.append("  * Items fetched: %d"
                 % num(i_doc['fetch_process']['total_items']))
    stats.append("  * Items enriched: %d"
                 % num(i_doc['enrich_process']['total_items']))
    stats.append("  * Items saved: %d"
                 % num(i_doc['save_process']['total_items']))
    stats.append("  * Collections fetched: %s"
                 % num(i_doc['fetch_process']['total_collections']))
    stats.append("  * Collections enriched: %d"
                 % num(i_doc['enrich_process']['total_collections']))
    stats.append("  * Collections saved: %d"
                 % num(i_doc['save_process']['total_collections']))
    return "Statistics:\n%s" % "\n".join(stats)

def num(n):
    if n == None:
        return 0
    else:
        return int(n)

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    couch = Couch()
    i_doc = couch.dashboard_db[args.ingestion_document_id]
    if i_doc['delete_process']['status'] != 'complete':
        print >> sys.stderr, 'Error: delete process did not complete'
        return 1

    # Update ingestion document to indicate that we're running
    kwargs = {'check_counts_process/status': 'running',
              'check_counts_process/start_time': iso_utc_with_tz()}
    try:
        couch.update_ingestion_doc(i_doc, **kwargs)
    except:
        tb = traceback.format_exc(5)
        print "Error updating ingestion document %s\n%s" % (i_doc["_id"], tb)
        return 1

    error_msg = None
    try:
        config = ConfigParser.ConfigParser()                                    
        config.readfp(open('akara.ini'))
        to = [s.strip() for s in config.get('Alert', 'To').split(',')]
        frm = config.get('Alert', 'From')
        body = "%s\n\n%s" % (alerts(i_doc), statistics(i_doc))
        msg = MIMEText(body)
        msg['Subject'] = "%s ingest #%s" % (i_doc['provider'],
                                            i_doc['ingestionSequence'])
        msg['To'] = ', '.join(to)
        msg['From'] = frm
        s = smtplib.SMTP("localhost")
        s.sendmail(frm, to, msg.as_string())
        s.quit()
    except Exception, e:
        error_msg = e
        tb = traceback.format_exc(5)
        print >> sys.stderr, "Error sending alert email: %s\n%s" % (error_msg,
                                                                    tb)

    # Update ingestion document
    kwargs = {'check_counts_process/status': 'complete',
              'check_counts_process/error': error_msg,
              'check_counts_process/end_time': iso_utc_with_tz()}
    try:
        couch.update_ingestion_doc(i_doc, **kwargs)
    except:
        tb = traceback.format_exc(5)
        print >> sys.stderr, "Error updating ingestion document %s\n%s" \
                             % (i_doc["_id"], tb)
        return 1

    return 0

if __name__ == '__main__':
    rv = main(sys.argv)
    sys.exit(rv)
