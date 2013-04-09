"""
Script that downloads all document ids of the Internet Archive profile
and saves them into sqlite3 file.

table name: ia_dbs
table structure: id | collection | saved_at
"""

import sys
import sqlite3
import argparse
from itertools import izip_longest
import json
from urllib import urlencode

from dplaingestion.internet_archive import fetch_url


class IdsDb(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._init_storage()

    def _init_storage(self):
        c = self._conn.cursor()
        try:
            c.execute("""CREATE TABLE IF NOT EXISTS ia_ids
             (id text PRIMARY KEY, collection text, saved_at text DEFAULT CURRENT_TIMESTAMP)""")
        finally:
            c.close()
        self._conn.commit()

    def save_ids(self, ids, collection):
        c = self._conn.cursor()
        try:
            c.executemany("INSERT OR IGNORE INTO ia_ids (id, collection) VALUES (?, ?)",
                          izip_longest(ids, tuple(), fillvalue=collection))
        finally:
            c.close()
        self._conn.commit()

    def __del__(self):
        try:
            self._conn.close()
        except Exception:
            pass


def process_ia_coll(db, profile, subr, page_size):
    """
    Fetcheds and saves all ids for given collection

    db - IdsDb instance
    profile - profile dict
    subr - collection name
    page_size - the number of ids fetched at once
    """

    def get_docs_list(request_url):
        content = fetch_url(request_url)
        parsed = json.loads(content)
        response_key = "response"
        if response_key in parsed:
            return parsed[response_key]
        else:
            raise Exception("No \"%s\" key in returned json" % response_key)

    endpoint = profile[u'endpoint_URL'].format(subr)
    args = {"rows": page_size, "page": 1}
    print "Downloading ids for collection:", subr
    done = False
    page = 1
    while not done:
        args["page"] = page
        request_url = endpoint + "&" + urlencode(args)
        response_dict = get_docs_list(request_url)
        total_docs = int(response_dict["numFound"])
        read_docs = int(response_dict["start"])
        done = (total_docs - read_docs) < args["rows"]
        page += 1
        assert "docs" in response_dict
        db.save_ids((i["identifier"] for i in response_dict["docs"]), subr)


def read_profile(profile_path):
    """
    Returns profile dict by path to the profile file
    """
    with open(profile_path) as f:
        return json.load(f)


def define_arguments():
    """
    Defines command line arguments for the current script
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", help="The path to the sqlite file for saving ids")
    parser.add_argument("profile", help="The path to profile to be processed")
    parser.add_argument("-n", "--page-size", help="The limit number of ids to be fetched at once, default 500",
                        default=500, type=int)
    return parser


def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])
    db = IdsDb(args.db_path)
    profile = read_profile(args.profile)
    for collection in profile["subresources"]:
        process_ia_coll(db, profile, collection, args.page_size)


if __name__ == "__main__":
    main(sys.argv)
