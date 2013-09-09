#!/usr/bin/env python
"""
Script to update legacy ingestion documents with *_process fields

Usage:
    $ python scripts/update_legacy_ingestion_documents.py
"""
import os
from amara.thirdparty import json
from dplaingestion.couch import Couch

def main():
    couch = Couch()

    new_fields = {
        "fetch_process": {"status": "complete"},
        "enrich_process": {"status": "complete"},
        "save_process": {"status": "complete"},
        "delete_process": {"status": "complete"},
    }
    for profile in os.listdir("profiles/"):
        if profile.endswith(".pjs"):
            with open("profiles/" + profile, "r") as f:
                p = json.loads(f.read())

            provider = p["name"]

            for doc in couch._query_all_provider_ingestion_docs(provider):
                doc.update(new_fields)
                couch.dashboard_db.update([doc])

if __name__ == "__main__":
    main()
