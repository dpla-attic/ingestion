#!/usr/bin/env python
"""
Script to run pipeline against records pulled from a CouchDB view

Simple use cases:
    Download thumbnail ls for all records related to a profile that have not already been downloaded
    Validate all URLs within records on a profile, and flag records with URLs that don't resolve
    Perform geocoding for all records on a profile

Usage:
    $ python poll_storage.py [-h] [--filter FILTER] [-n PAGE_SIZE] profile pipeline service

Example:
    $ python scripts/poll_storage.py -n 100 profiles/artstor.pjs geocode http://localhost:8879/enrich_storage
"""

import argparse
import sys
import base64
from urllib import urlencode

from amara.thirdparty import json, httplib2
from amara.lib.iri import join


HTTP_TYPE_JSON = 'application/json'


class Couch(object):
    """
    Basic wrapper class for operations on a couchDB
    """

    def __init__(self, database_uri, username, password):
        self.authorization_header = {'Authorization': 'Basic ' + base64.encodestring(username + ":" + password)}
        self.uri = database_uri
        self.http = httplib2.Http()
        self.http.force_exception_to_status_code = True
        self.page_size = 500

    def doc_count(self):
        db_info = json.loads(self.get(self.uri))
        return db_info['doc_count']

    def _last_profile_doc_id(self, profile_name):
        """
        Returns the last document id for the given profile name

        Raises ValueError in case of empty result from db for given
        profile name
        """
        id_prefix = self._slugify(profile_name)
        request_parameters = urlencode((
            ("descending", "true"),
            ("limit", "1"),
            ("endkey", "\"" + id_prefix + "\"")
        ))
        request_uri = join(self.uri, "_all_docs?" + request_parameters)
        response = json.loads(self.get(request_uri))
        if response["rows"]:
            last_doc_id = response["rows"][0]["id"]
            assert last_doc_id.startswith(id_prefix), "Document id must start from profile name, but it is " + last_doc_id
            return last_doc_id
        else:
            raise ValueError("Can not get last document id for \"%s\" profile" % profile_name)

    def list_profile_doc(self, profile_name):
        """
        Iterates and returns all documents related to the
        given profile name
        """
        id_prefix = self._slugify(profile_name)
        last_id = self._last_profile_doc_id(profile_name)
        start_key = id_prefix
        while True:
            request_parameters = urlencode((
                ("descending", "false"),
                ("limit", self.page_size + 1),
                ("startkey", "\"" + start_key + "\""),
                ("include_docs", "true")
            ))
            request_uri = join(self.uri, "_all_docs?" + request_parameters)
            rows = json.loads(self.get(request_uri))["rows"]
            for row in rows[:-1]:
                doc = row["doc"]
                if "ingestType" in doc and doc["ingestType"] == "item":
                    yield doc
                    if row["id"] == last_id:
                        break
            next_page_row = rows[-1:][0]
            start_key = next_page_row["id"]

            if len(rows) < self.page_size:
                yield next_page_row["doc"]
                break


    def get(self, uri):
        """
        Sends a get request to the given URI

        Raises IOError in case of http request problem
        """
        headers = {"Accept": HTTP_TYPE_JSON}
        headers.update(self.authorization_header)
        response, content = self.http.request(uri, "GET", headers=headers)
        if self._successful(response):
            return content
        else:
            raise IOError("%d %s: %s" % (response.status, response.reason, content))

    def post(self, uri, body=None):
        """
        Sends a post request to the given URI

        Raises IOError in case of http request problem
        """
        headers = {"Content-type": HTTP_TYPE_JSON}
        headers.update(self.authorization_header)
        response, content = self.http.request(uri, "POST", body=body, headers=headers)
        if self._successful(response):
            return content
        else:
            raise IOError("%d %s: %s" % (response.status, response.reason, content))


    def _successful(self, response):
        return str(response.status).startswith('2')

    def _slugify(self, s):
        return s.strip().replace(" ", "__")


def define_arguments():
    """
    Defines command line arguments for the current script
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("profile", help="The path to profile to be processed")
    parser.add_argument("pipeline", help="The name of an enrichment pipeline in the profile that contains the list of enrichments to be run")
    parser.add_argument("service", help="The URL of the enrichment service")
    parser.add_argument("--filter", help="Name or identifier for a CouchDB view that would limit the number of records to be processed")
    parser.add_argument("-n", "--page-size", help="The limit number of record to be processed at once, default 500", default=500, type=int)
    return parser

def read_profile(profile_path):
    """
    Returns profile dict by path to the profile file
    """
    with open(profile_path) as f:
        return json.load(f)

def enrich(service_url, profile_dict, pipeline_name, docs):
    """
    Calls enrichment service url with docs,
    fetched from storage, passing a pipeline
    for processing

    Raises IOError if enrichment service returns unsuccessful response code
    """
    http = httplib2.Http()
    http.force_exception_to_status_code = True
    headers = {
        "Content-Type": HTTP_TYPE_JSON,
        "Pipeline-Rec": ','.join(profile_dict[pipeline_name]),
        "Source": profile_dict['name'],
        "Contributor": base64.b64encode(json.dumps(profile_dict.get(u'contributor',{})))
    }

    response, content = http.request(service_url, 'POST', body=json.dumps(docs), headers=headers)
    if not str(response.status).startswith('2'):
        raise IOError("%d %s: %s" % (response.status, response.reason, content))

def main(argv):
    parser = define_arguments()
    args = parser.parse_args(argv[1:])
    profile = read_profile(args.profile)
    couch = Couch("http://camp.dpla.berkman.temphost.net:5979/dpla", "couchadmin", "couchM3")
    couch.page_size = args.page_size
    docs = []
    cnt = 0
    for doc in couch.list_profile_doc(profile["name"]):
        docs.append(doc)
        cnt += 1
        if len(docs) == couch.page_size:
            enrich(args.service, profile, args.pipeline, docs)
            print "processed", cnt, "documents"
            docs = []
    if docs:
        enrich(args.service, profile, args.pipeline, docs)
        print "processed", cnt, "documents"

if __name__ == "__main__":
    main(sys.argv)
