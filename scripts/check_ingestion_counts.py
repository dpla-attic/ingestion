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
from akara import logger
from datetime import datetime
from amara.thirdparty import json
from dplaingestion.couch import Couch
from dplaingestion.selector import getprop
import smtplib
from email.mime.text import MIMEText
from dateutil import parser as dateparser
import ConfigParser


def define_arguments():
    """Defines command line arguments for the current script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("ingestion_document_id", 
                        help="The ID of the ingestion document")

    return parser

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])

    couch = Couch()
    ingestion_doc = couch.dashboard_db[args.ingestion_document_id]
    if getprop(ingestion_doc, "delete_process/status") != "complete":
        print "Error, delete process did not complete"
        return -1

    # Update ingestion document
    kwargs = {
        "check_counts_process/status": "running",
        "check_counts_process/start_time": datetime.now().isoformat()
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return -1

    # Check each count against the threshold
    alerts = []
    count_type = ("Added", "Changed", "Deleted")
    for ctype in count_type:
        count = int(ingestion_doc["count" + ctype])
        threshold = int(ingestion_doc["thresholds"][ctype.lower()])
        if count > threshold:
            alerts.append("%s items %s exceeds threshold of %s" %
                          (count, ctype.lower(), threshold))

    error_msg = None
    if alerts:
        config_file = "akara.ini"
        config = ConfigParser.ConfigParser()                                    
        config.readfp(open(config_file))
        to = [s.strip() for s in config.get("Alert", "To").split(",")]
        frm = config.get("Alert", "From")

        month = dateparser.parse(ingestion_doc["ingestDate"]).strftime("%B")
        alerts = "\n".join(alerts)
        msg = MIMEText(alerts)
        msg["Subject"] = "Threshold(s) exceeded for %s ingestion of %s" % \
                         (month, ingestion_doc["provider"])
        msg["To"] = ", ".join(to)
        msg["From"] = frm

        try:
            s = smtplib.SMTP("localhost")
            s.sendmail(frm, to, msg.as_string())
            s.quit()
        except Exception, e:
            error_msg = e

    if error_msg:
        print >> sys.stderr, ("********************\n" +
                              "Error sending alert email: %s" % error_msg)
        print >> sys.stderr, ("Alerts:\n%s" % alerts +
                              "\n********************")

    # Update ingestion document
    status = "complete"
    kwargs = {
        "check_counts_process/status": status,
        "check_counts_process/error": error_msg,
        "check_counts_process/end_time": datetime.now().isoformat()
    }
    try:
        couch.update_ingestion_doc(ingestion_doc, **kwargs)
    except:
        print "Error updating ingestion document " + ingestion_doc["_id"]
        return -1

    return 0 if status == "complete" else -1

if __name__ == '__main__':
    main(sys.argv)
