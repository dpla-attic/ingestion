#!/usr/bin/env python
#
# Usage: python scripts/nara_update_links.py

import sys
from dplaingestion.selector import getprop
from dplaingestion.couch import Couch

def nara_update_links():
    couch = Couch()
    url = "http://research.archives.gov/description/"
    docs = []
    all_rows = couch._query_all_dpla_provider_docs("nara")
    print >> sys.stderr, "Fetching %s documents" % len(all_rows)
    for row in all_rows:
        doc = row["doc"]
        arc_id_desc = getprop(doc, "originalRecord/arc-id-desc",
                              keyErrorAsNone=True)
        if arc_id_desc:
            doc.update({"isShownAt": url + arc_id_desc})
            docs.append(doc)

        # POST every 1000 documents
        if len(docs) == 1000:
            print >> sys.stderr, "Processed %s of %s documents" %(len(docs),
                                                                  len(all_rows))
            couch.bulk_post_to_dpla(docs)
            docs = []

    # Last POST
    if docs:
        print >> sys.stderr, "Processed %s of %s documents" %(len(docs),
                                                              len(all_rows))
        couch.bulk_post_to_dpla(docs)

if __name__ == '__main__':
    nara_update_links()
