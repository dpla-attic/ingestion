#!/usr/bin/env python
#
# Usage: python scripts/nara_update_links.py

import sys
import time
from dplaingestion.selector import getprop
from dplaingestion.couch import Couch

def nara_update_links():
    couch = Couch()
    url = "http://research.archives.gov/description/"
    docs = []
    print >> sys.stderr, "Fetching all documents"
    count = 0
    start = time.time()
    for doc in couch._query_all_dpla_provider_docs("nara"):
        if count == 0:
            view_time = time.time() - start
            start = time.time()
        count += 1
        arc_id_desc = getprop(doc, "originalRecord/arc-id-desc",
                              keyErrorAsNone=True)
        if arc_id_desc:
            doc.update({"isShownAt": url + arc_id_desc})
            docs.append(doc)

        # POST every 1000 documents
        if len(docs) == 1000:
            print >> sys.stderr, "Processed %s documents" % count
            couch.bulk_post_to_dpla(docs)
            docs = []

    # Last POST
    if docs:
        print >> sys.stderr, "Processed %s documents" % count
        couch.bulk_post_to_dpla(docs)

    process_time = time.time() - start
    print >> sys.stderr, "Done"
    print >> sys.stderr, "View time: %s" % view_time
    print >> sys.stderr, "Process time: %s" % process_time

if __name__ == '__main__':
    nara_update_links()
