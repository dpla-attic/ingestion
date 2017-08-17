#!/usr/bin/env python
"""
Script to check the added, changed, and removed counts of an ingestion
document and email an alert if any threshold has been exceeded

Usage:
    $ python check_ingestion_counts.py ingestion_document_id
"""
import sys
import argparse
import traceback
import smtplib
import ConfigParser
from dplaingestion.couch import Couch
from email.mime.text import MIMEText


def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("ingestion_document_id", help="The ID of the "
                                                      "ingestion document")

    return parser


def alerts(i_doc):
    """Return a string for the alerts part of the email

    i_doc: the dict-like object from Couch.dashboard_db for the ingestion doc.
    """
    alerts = []

    alerts.append("Alerts for ingestion document ID: %s" % i_doc["_id"])
    count_type = ('fetch_process', 'enrich_process')
    for ct in count_type:
        count = int(i_doc[ct]["total_items"])
        alerts.append("\t%s %d items" % (ct.lower().split("_")[0]+"ed", count))

    return "\n".join(alerts)


def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    couch = Couch()
    i_doc = couch.dashboard_db[args.ingestion_document_id]
    if i_doc['enrich_process']['status'] != 'complete':
        print >> sys.stderr, 'Error: enrich process did not complete'
        return 1

    error_msg = None
    try:
        config = ConfigParser.ConfigParser()                                    
        config.readfp(open('akara.ini'))
        to = [s.strip() for s in config.get('Alert', 'To').split(',')]
        frm = config.get('Alert', 'From')
        body = "%s" % (alerts(i_doc))
        msg = MIMEText(body)
        msg['Subject'] = "%s fetched and enriched %s" % \
                         (i_doc['provider'], i_doc['ingestionSequence'])
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

    return 0

if __name__ == '__main__':
    rv = main(sys.argv)
    sys.exit(rv)
