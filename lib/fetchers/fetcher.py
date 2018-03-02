import os
import sys
import time
import hashlib
import fnmatch
import urllib2
import xmltodict
import ConfigParser
import socket
import itertools as it
from urllib import urlencode
from datetime import datetime
from amara.thirdparty import json
from amara.lib.iri import is_absolute
from dplaingestion.selector import exists
from dplaingestion.selector import setprop
from dplaingestion.selector import getprop as get_prop
from dplaingestion.utilities import iterify, couch_id_builder
import requests
from requests import RequestException
from requests.adapters import HTTPAdapter
import re


def getprop(obj, path):
    return get_prop(obj, path, keyErrorAsNone=True)


XML_PARSE = lambda doc: xmltodict.parse(xmltodict_str(doc),
                                        xml_attribs=True,
                                        attr_prefix='',
                                        force_cdata=False,
                                        ignore_whitespace_cdata=True)


def xmltodict_str(s):
    """Temporary kludge to get Getty to work"""
    try:
        # encode() converts a `unicode' string to a byte string (`str').
        # `decode()` converts a byte string (`str') to Unicode.
        # `encode()` converts a `unicode' string to a `str' byte string.
        return s.encode('utf-8')
    except UnicodeEncodeError:
        return s


class Fetcher(object):
    """The base class for all fetchers.
       Includes attributes and methods that are common to all types.
    """

    def __init__(self, profile, uri_base, config_file):
        """Set common attributes"""
        self.uri_base = uri_base
        self.config = ConfigParser.ConfigParser()
        with open(config_file, "r") as f:
            self.config.readfp(f)

        # Which OAI sets get added as collections
        self.sets = profile.get("sets")

        self.provider = profile.get("name")
        self.blacklist = profile.get("blacklist")
        self.set_params = profile.get("set_params")
        self.contributor = profile.get("contributor")
        self.collections = profile.get("collections", {})
        self.endpoint_url = profile.get("endpoint_url")
        self.collection_titles = profile.get("collection_titles")
        self.http_headers = profile.get("http_headers", {})

        # Set batch_size
        self.batch_size = 500

        # Set response
        self.response = {"errors": [], "records": []}

        self.retries = profile.get("retries", 1)

    def reset_response(self):
        self.response = {"errors": [], "records": []}

    def remove_blacklisted_sets(self):
        if self.blacklist:
            for set in self.blacklist:
                if set in self.sets:
                    del self.sets[set]

    def request_content_from(self, url, params={}):
        error = None
        resp = None
        r = None

        try:
            s = requests.Session()
            s.mount(url, HTTPAdapter(max_retries=self.retries))
            r = s.get(url, params=params, headers=self.http_headers)
            r.raise_for_status()
            resp = r.text
        except RequestException:
            error = "Error (%s--%s) requesting %s?%s" % (r.status_code,
                                                         r.reason, url,
                                                         urlencode(params))
        return error, resp

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
