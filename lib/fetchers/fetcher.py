import os
import re
import sys
import time
import hashlib
import fnmatch
import urllib2
import xmltodict
import ConfigParser
import httplib
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

    def reset_response(self):
        self.response = {"errors": [], "records": []}

    def remove_blacklisted_sets(self):
        if self.blacklist:
            for set in self.blacklist:
                if set in self.sets:
                    del self.sets[set]

    def request_content_from(self, url, params={}, attempts=3):
        error = None
        resp = None
        m = re.match(r"(https?)://(.*?)(/.*)", url)
        socket_parts = m.group(2).split(":")
        host = socket_parts[0]
        ssl = m.group(1) == "https"

        if len(socket_parts) == 2:
            port = int(socket_parts[1])
        elif ssl:
            port = 443
        else:
            port = 80

        req_uri = m.group(3)
        if params:
            if "?" in req_uri:
                req_uri += "&" + urlencode(params, True)
            else:
                req_uri += "?" + urlencode(params, True)

        for i in range(attempts):
            try:
                if ssl:
                    con = httplib.HTTPSConnection(host, port)
                else:
                    con = httplib.HTTPConnection(host, port)
                con.request("GET", req_uri, None, self.http_headers)
                resp = con.getresponse()
                if resp.status == 200:
                    break  # connection stays open for read()!
                con.close()
            except Exception as e:
                if type(e) == socket.error:
                    err_msg = e
                else:
                    err_msg = e.message
                print >> sys.stderr, "Requesting %s: %s" % (req_uri, err_msg)
            time.sleep(2)

        if resp and resp.status == 200:
            rv = resp.read()
            con.close()
        elif resp:
            error = "Error ('%d') requesting %s" % (resp.status, url)
            rv = None
        else:
            error = "Could not request %s" % url
            rv = None
        return error, rv

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
