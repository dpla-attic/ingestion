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
import time
from functools import wraps
import urllib2
from urllib import urlencode


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
            c.executemany("INSERT OR IGNORE INTO ia_ids (id, collection) VALUES (?, ?)", izip_longest(ids, tuple(), fillvalue=collection))
        finally:
            c.close()
        self._conn.commit()

    def __del__(self):
        try:
            self._conn.close()
        except Exception:
            pass

def with_retries(attempts_num=3, delay_sec=1):
    """
    Wrapper (decorator) that calls given func(*args, **kwargs);
    In case of exception does 'attempts_num'
    number of attempts with "delay_sec * attempt number" seconds delay
    between attempts.

    Usage:
    @with_retries(5, 2)
    def get_document(doc_id, uri): ...
    d = get_document(4444, "...") # now it will do the same logic but with retries

    Or:
    def get_document(...): ...
    get_document = with_retries(5, 2)(get_document)
    d = get_document(...) # now it will do the same logic but with retries
    """
    def apply_with_retries(func):
        assert attempts_num >= 1
        assert isinstance(attempts_num, int)
        assert delay_sec >= 0
        @wraps(func)
        def func_with_retries(*args, **kwargs):

            def pause(attempt):
                """Do pause if current attempt is not the last"""
                if attempt < attempts_num:
                    sleep_sec = delay_sec * attempt
                    print >> sys.stderr, "Sleeping for %d seconds..." % sleep_sec
                    time.sleep(sleep_sec)

            for attempt in xrange(1, attempts_num + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print >> sys.stderr, "Error [%s: %s] occurred while trying to call \"%s\". Attempt #%d failed" % (e.__class__.__name__, str(e), func.__name__, attempt)
                    if attempt == attempts_num:
                        raise
                    else:
                        pause(attempt)
        return func_with_retries
    return apply_with_retries

@with_retries(10, 3)
def fetch_url(url):
    """Downloads data related to given url,
    checks that http response code is 200"""
    print "fetching url", url, "..."
    d = None
    try:
        d = urllib2.urlopen(url, timeout=10)
        code = d.getcode()
        assert code == 200, "Bad response code = " + str(code)
        return d.read()
    finally:
        if d:
            d.close()

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
    parser.add_argument("-n", "--page-size", help="The limit number of ids to be fetched at once, default 500", default=500, type=int)
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
