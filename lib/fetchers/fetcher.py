import os
import re
import sys
import time
import hashlib
import fnmatch
import urllib2
import xmltodict
import ConfigParser
import itertools as it
from urllib import urlencode
from datetime import datetime
from amara.thirdparty import json
from amara.thirdparty import httplib2
from amara.lib.iri import is_absolute
from dplaingestion.selector import exists
from dplaingestion.selector import setprop
from dplaingestion.selector import getprop as get_prop
from dplaingestion.utilities import iterify, couch_id_builder

def getprop(obj, path):
    return get_prop(obj, path, keyErrorAsNone=True)

XML_PARSE = lambda doc: xmltodict.parse(doc, xml_attribs=True, attr_prefix='',
                                        force_cdata=False,
                                        ignore_whitespace_cdata=True)

class Fetcher(object):
    """The base class for all fetchers.
       Includes attributes and methods that are common to all types.
    """
    def __init__(self, profile, uri_base, config_file):
        """Set common attributes"""
        self.config_file = config_file
        self.uri_base = uri_base

        # Which OAI sets get added as collections
        self.sets = profile.get("sets")

        self.provider = profile.get("name")
        self.blacklist = profile.get("blacklist")
        self.set_params = profile.get("set_params")
        self.contributor = profile.get("contributor")
        self.collections = profile.get("collections", {})
        self.endpoint_url = profile.get("endpoint_url")
        self.collection_titles = profile.get("collection_titles")
        self.http_handle = httplib2.Http('/tmp/.pollcache')
        self.http_handle.force_exception_as_status_code = True

        # Set batch_size
        self.batch_size = 500

        # Set response
        self.response = {"errors": [], "records": []}

    def reset_response(self):
        self.response = {"errors": [], "records": []}

    def remove_blacklisted_sets(self):
        if self.blacklist:
            for set in self.blacklist:
                if set in self.sets:
                    del self.sets[set]

    def request_content_from(self, url, params={}, attempts=3):
        error, content = None, None
        if params:
            if "?" in url:
                url += "&" + urlencode(params)
            else:
                url += "?" + urlencode(params)

        for i in range(attempts):
            resp, content = self.http_handle.request(url)
            # Break if 2xx response status
            if resp["status"].startswith("2"):
                break
            time.sleep(2)

        # Handle non 2xx response status
        if not resp["status"].startswith("2"):
            error = "Error ('%s') resolving URL %s" % (resp["status"], url)
        elif not len(content) > 2:
            error = "Length of content is no > 2 for URL %s" %  url

        return error, content

    def create_collection_records(self):
        if self.collections:
            for set_spec in self.collections.keys():
                _id = couch_id_builder(self.provider, set_spec)
                id = hashlib.md5(_id).hexdigest()
                at_id = "http://dp.la/api/collections/" + id
    
                self.collections[set_spec]["id"] = id
                self.collections[set_spec]["_id"] = _id
                self.collections[set_spec]["@id"] = at_id
                self.collections[set_spec]["ingestType"] = "collection"

    def add_provider_to_item_records(self, item_records):
        if item_records:
            for item_record in item_records:
                item_record["provider"] = self.contributor
