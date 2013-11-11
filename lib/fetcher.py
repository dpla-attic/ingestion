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
from amara.thirdparty import json
from amara.thirdparty import httplib2
from amara.lib.iri import is_absolute
from multiprocessing.dummy import Pool
from Queue import Full, Empty, Queue
try:
    from collections import OrderedDict
except:
    from ordereddict import OrderedDict

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
        config = ConfigParser.ConfigParser()
        config.readfp(open(self.config_file))
        self.batch_size = int(config.get("CouchDb", "BatchSize"))

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

class OAIVerbsFetcher(Fetcher):
    def __init__(self, profile, uri_base, config_file):
        self.metadata_prefix = profile.get("metadata_prefix")
        super(OAIVerbsFetcher, self).__init__(profile, uri_base, config_file)

    def list_sets(self):
        """Requests all sets via the ListSets verb"""
        list_set_content = None
        list_sets_url = self.uri_base + "/oai.listsets.json?endpoint=" + \
                        self.endpoint_url

        error, content = self.request_content_from(list_sets_url)
        if error is None:
            try:
                list_set_content = json.loads(content)
            except:
                error = "Error parsing content from URL %s" % list_sets_url

        return error, list_set_content
 
    def fetch_sets(self):
        """Fetches all sets

           Returns an (error, sets) tuple
        """
        error = None
        sets = {}

        if self.sets == "NotSupported":
            sets = {"": None}
        else:
            error, list_sets_content = self.list_sets()
            if error is None:
                for s in list_sets_content:
                    set_spec = s["setSpec"]
                    sets[set_spec] = {
                        "title": s["setName"]
                    }
                    if "description" in s:
                        sets[set_spec]["description"] = s["description"].strip()

                if not sets:
                    error = "No sets received with ListSets request to %s" % \
                            self.endpoint_url

        return error, sets

    def list_records(self, url, params):
        """Requests records via the ListRecords verb

           Returns an (error, records_content) tuple where error is None if the
           request succeeds, the requested content is not empty and is
           parseable, and records is a dictionary with key "items".
        """
        records_content = {}
        list_records_url = self.uri_base + "/dpla-list-records?endpoint=" + url

        # Add set params, if any
        if self.set_params:
            set_params = self.set_params.get(params.get("oaiset"))
            if set_params:
                params.update(set_params)

        error, content = self.request_content_from(list_records_url, params)
        if error is None:
            try:
                records_content = json.loads(content)
            except ValueError:
                error = "Error decoding content from URL %s with params %s" % \
                        (list_records_url, params)

            if not records_content.get("items"):
                error = "No records received from URL %s with params %s" % \
                        (list_records_url, params)

        return error, records_content

    def add_collection_to_item_records(self, item_records):
        for item_record in item_records:
            collections = []
            for set_spec in iterify(item_record.get("setSpec")):
                collection = self.collections.get(set_spec)
                if collection:
                    collections.append(collection)
            if collections:
                item_record["collection"] = collection

    def set_collections(self):
        if not self.collections:
            error, sets = self.fetch_sets()
            if error is not None:
                self.response["errors"].append(error)
            elif sets:
                if self.sets:
                    # Remove any sets that are not in self.sets
                    for set in sets.keys():
                        if set not in self.sets:
                            del sets[set]
                if self.blacklist:
                    # Remove any sets that are blacklisted
                    for set in sets.keys():
                        if set in self.blacklist:
                            del sets[set]
                self.collections = sets

    def fetch_all_data(self):
        """A generator to yield batches of records fetched, and any errors
           encountered in the process, via the self.response dicitonary.
        """
        # Set self.collections
        self.set_collections()

        # Create records of ingestType "collection"
        if self.collections and any(self.collections.values()):
            self.create_collection_records()
            self.response["records"].extend([v.copy() for v in
                                             self.collections.values()])
            if len(self.response["records"]) >= self.batch_size:
                yield self.response
                self.reset_response()
            # Now that self.collections.values has been added to the
            # response, let's remove the "_id" and "ingesType" fields since
            # we'll be using the values for each item record's collection
            # field
            for k, v in self.collections.items():
                for prop in ["_id", "ingestType"]:
                    del v[prop]

        # Fetch all records for each set
        for set in self.collections.keys():
            print "Fetching records for set " + set

            # Set initial params
            params = {}
            if not set == "":
                params["oaiset"] = set
        
            request_more = True
            resumption_token = ""
            url = self.endpoint_url

            # Set the metadataPrefix
            if self.metadata_prefix is not None:
                params["metadataPrefix"] = self.metadata_prefix

            # Request records until a resumption token is not received
            while request_more:
                # Send request
                error, content = self.list_records(url, params)

                if error is not None:
                    # Stop requesting from this set
                    request_more = False
                    self.response["errors"].append(error)
                else:
                    self.add_provider_to_item_records(content["items"])
                    self.add_collection_to_item_records(content["items"])
                    self.response["records"].extend(content["items"])
                    # Get resumption token
                    resumption_token = content.get("resumption_token")
                    if (resumption_token is not None and
                        len(resumption_token) > 0):
                        # Add resumption token
                        params["resumption_token"] = resumption_token
                    else:
                        request_more = False

                if len(self.response["records"]) >= self.batch_size:
                    yield self.response
                    self.reset_response()

        # Last yield
        if self.response["errors"] or self.response["records"]:
            yield self.response

class AbsoluteURLFetcher(Fetcher):
    def __init__(self, profile, uri_base, config_file):
        self.get_sets_url = profile.get("get_sets_url")
        self.get_records_url = profile.get("get_records_url")
        self.endpoint_url_params = profile.get("endpoint_url_params")
        super(AbsoluteURLFetcher, self).__init__(profile, uri_base, config_file)

    def extract_content(self, content, url):
        """Calls extract_xml_content by default but can be overriden in
           child classes
        """
        return self.extract_xml_content(content, url)

    def extract_xml_content(self, content, url):
        error = None
        try:
            content = XML_PARSE(content)
        except:
            error = "Error parsing content from URL %s" % url

        return error, content

    def fetch_sets(self):
        """Fetches all sets

           Returns an (error, sets) tuple
        """
        error = None
        sets = {}

        if self.sets == "NotSupported":
            sets = {"": None}

        return error, sets

    def set_collections(self):
        if not self.collections:
            error, sets = self.fetch_sets()
            if error is not None:
                self.response["errors"].append(error)
            elif sets:
                self.collections = sets

    def request_records(self):
        # Implemented in child classes
        pass

    def add_collection_to_item_records(self, set, records):
        collection = self.collections.get(set)
        if collection:
            for record in records:
                record["collection"] = collection

    def fetch_all_data(self):
        """A generator to yield batches of records fetched, and any errors
           encountered in the process, via the self.response dicitonary.
        """

        # Set self.collections
        self.set_collections()
        if not self.collections:
            self.response["errors"] = "If sets are not supported then the " + \
                                     "sets field in the profile must be set " \
                                     "to \"NotSupported\""
            yield self.response

        # Create records of ingestType "collection"
        if self.collections and any(self.collections.values()):
            self.create_collection_records()
            self.response["records"].extend([v.copy() for v in
                                             self.collections.values()])
            if len(self.response["records"]) >= self.batch_size:
                yield self.response
                self.reset_response()
            # Now that self.collections.values has been added to the
            # response, let's remove the "_id" and "ingesType" fields since
            # we'll be using the values for each item record's collection
            # field
            for k, v in self.collections.items():
                for prop in ["_id", "ingestType"]:
                    del v[prop]

        # Request records for each set
        for set in self.collections.keys():
            print "Fetching records for set " + set

            request_more = True
            if set:
                url = self.endpoint_url.format(set)
            else:
                url = self.endpoint_url
            params = self.endpoint_url_params

            while request_more:
                error, content = self.request_content_from(
                    url, self.endpoint_url_params
                    )

                if error is not None:
                    # Stop requesting from this set
                    request_more = False
                    self.response["errors"].append(error)
                    continue

                error, content = self.extract_content(content, url)
                if error is not None:
                    request_more = False
                    self.response["errors"].extend(iterify(error))
                else:
                    for error, records, request_more in \
                        self.request_records(content):
                        if error is not None:
                            self.response["errors"].extend(iterify(error))
                        self.add_provider_to_item_records(records)
                        self.add_collection_to_item_records(set, records)
                        self.response["records"].extend(records)
                        if len(self.response["records"]) >= self.batch_size:
                            yield self.response
                            self.reset_response()

        # Last yield
        if self.response["errors"] or self.response["records"]:
            yield self.response

class IAFetcher(AbsoluteURLFetcher):
    def __init__(self, profile, uri_base, config_file):
        super(IAFetcher, self).__init__(profile, uri_base, config_file)
        self.count = 0
        self.get_file_url = profile.get("get_file_url")
        self.prefix_files = profile.get("prefix_files")
        self.prefix_meta = profile.get("prefix_meta")
        self.prefix_dc = profile.get("prefix_dc")
        self.shown_at_url = profile.get("shown_at_url")
        self.fetch_pool = None
        self.queue = Queue(maxsize=self.batch_size)

    def extract_content(self, content, url):
        error = None
        try:
            data = json.loads(content)
        except:
            error = "Could not load content from %s as JSON" % url
            return error, content

        content = data.get("response")
        if content is None:
            error = "Error, there is no \"response\" field in content from " \
                    "URL %s" % url

        return error, content

    def extract_xml_content(self, content, url):
        error = None
        try:
            content = xmltodict.parse(content,
                                      xml_attribs=True,
                                      attr_prefix='',
                                      force_cdata=False,
                                      ignore_whitespace_cdata=True)
        except:
            error = "Error parsing content from URL %s" % url

        return error, content

    def fetch_url(self, url):
        error = None
        content = None
        response = None
        try:
            response = urllib2.urlopen(url, timeout=10)
            if response.getcode() == 200:
                content = response.read()
        except:
            error = "Error requesting content from URL %s" % url

        if response:
            response.close()

        return error, content

    def request_then_extract_xml(self, url):
        error, content = self.fetch_url(url)
        try:
            error, content = self.extract_xml_content(content, url)
        except:
            pass

        return error, content

    def request_records(self, content):
        self.fetch_pool = Pool(processes=10)

        total_records = int(content["numFound"])
        read_records = int(content["start"])
        expected_records = self.endpoint_url_params["rows"]

        total_pages = total_records/expected_records + 1
        request_more =  total_pages != self.endpoint_url_params["page"]
        if not request_more:
            # Since we are at the last page the expected_records will not
            # be equal to self.endpoint_url_params["rows"]
            expected_records = total_records - read_records
            # Reset the page for the next colleciton
            self.endpoint_url_params["page"] = 1
        else:
            self.endpoint_url_params["page"] += 1

        ids = [doc["identifier"] for doc in content["docs"]]
        for doc in content["docs"]:
            id = doc["identifier"]
            self.fetch_pool.apply_async(self.fetch_record, [id],
                                        callback=self.queue_fetch_response)
        self.fetch_pool.close()
        self.fetch_pool.join()

        count = 0
        errors = []
        records = []
        while (count != expected_records):
            try:
                record = (self.queue.get(block=False))
                count += 1
                if isinstance(record, basestring):
                    errors.append(record)
                else:
                    records.append(record)
            except:
                pass

            if count % self.batch_size == 0:
                # Yield every self.batch_size records
                yield errors, records, request_more
                errors = []
                records = []
                print "Fetched %s of %s" % (read_records + count,
                                            total_records)

        # Last yield
        if records:
            yield errors, records, request_more
            print "Fetched %s of %s" % (read_records + count, total_records)

    def fetch_record(self, id):
        """Fetches a record and places it in the queue"""
        file_url = self.get_file_url.format(id,
                                            self.prefix_files.format(id))
        error, content = self.request_then_extract_xml(file_url)
        if error is not None:
            return error

        files_content = content["files"]
        record_data = {
            "dc": None,
            "marc": None,
            "meta": None,
            "gif": None,
            "pdf": None,
            "shown_at": self.shown_at_url.format(id)
        }
        for file in files_content["file"]:
            format = file["format"]
            name = file["name"]
            if format == "Text PDF":
                record_data["pdf"] = name
            elif format == "Animated GIF":
                record_data["gif"] = name
            elif (format == "Grayscale LuraTech PDF" and not
                  record_data["pdf"]):
                record_data["pdf"] = name
            elif format == "Metadata" and name.endswith("_meta.xml"):
                record_data["meta"] = name
            elif format == "Dublin Core":
                record_data["dc"] = name
            elif format == "MARC":
                record_data["marc"] = name

        if record_data["meta"] is None:
            error = "Document %s meta data is absent" % id
            return error
        record = {
            "files": record_data,
            "_id":  id
        }

        meta_url = self.get_file_url.format(id, record_data["meta"])
        error, content = self.request_then_extract_xml(meta_url)
        if error is not None:
            return error
        record.update(content)

        if record_data["marc"]:
            marc_url = self.get_file_url.format(
                           id, record_data["marc"]
                       )
            error, content = self.request_then_extract_xml(marc_url)
            if error is not None:
                return error
            record.update(content)

        return record

    def queue_fetch_response(self, response): 
        try:
            self.queue.put(response, block=False)
        except Exception, e:
            print "Error: %s" % e

class MWDLFetcher(AbsoluteURLFetcher):
    def __init__(self, profile, uri_base, config_file):
        super(MWDLFetcher, self).__init__(profile, uri_base, config_file)

    def mwdl_extract_records(self, content):
        error = None
        total_records = getprop(content,
                                "SEGMENTS/JAGROOT/RESULT/DOCSET/TOTALHITS")
        records = getprop(content, "SEGMENTS/JAGROOT/RESULT/DOCSET/DOC")

        if records:
            records = iterify(records)
            for record in records:
                record["_id"] = getprop(record,
                                        "PrimoNMBib/record/control/recordid")
        else:
            error = "No records found in MWDL content"

        return error, total_records, records

    def request_records(self, content):
        request_more = True
        error, total_records, records = self.mwdl_extract_records(content)
        self.endpoint_url_params["indx"] += len(records)
        print "Fetched %s of %s" % (self.endpoint_url_params["indx"],
                                    total_records)
        request_more = (int(total_records) >=
                        int(self.endpoint_url_params["indx"]))

        yield error, records, request_more

class NYPLFetcher(AbsoluteURLFetcher):
    def __init__(self, profile, uri_base, config_file):
        super(NYPLFetcher, self).__init__(profile, uri_base, config_file)

    def fetch_sets(self):
        """Fetches all sets

           Returns an (error, sets) tuple
        """
        error = None
        sets = {}

        url = self.get_sets_url
        error, content = self.request_content_from(url)
        if error is not None:
            return error, sets

        error, content = self.extract_content(content, url)
        if error is not None:
            return error, sets

        for item in content["response"]:
            if item == "collection":
                for coll in content["response"][item]:
                    if "uuid" in coll:
                        sets[coll["uuid"]] = {
                            "id": coll["uuid"],
                            "title": coll["title"]
                        }

        if not sets:
            error = "Error, no sets from URL %s" % url

        return error, sets

    def extract_content(self, content, url):
        error = None
        try:
            parsed_content = XML_PARSE(content)
        except:
            error = "Error parsing content from URL %s" % url
            return error, content

        content = parsed_content.get("nyplAPI")
        if content is None:
            error = "Error, there is no \"nyplAPI\" field in content from " \
                    "URL %s" % url
        elif exists(content, "response/headers/code") and \
             getprop(content, "response/headers/code") != "200":
            error = "Error, response code is not 200 for request to URL %s" % \
                    url
        return error, content

    def request_records(self, content):
        self.endpoint_url_params["page"] += 1
        error = None
        total_pages = getprop(content, "request/totalPages")
        current_page = getprop(content, "request/page")
        request_more = total_pages != current_page
        if not request_more:
            # Reset the page for the next collection
            self.endpoint_url_params["page"] = 1

        records = []
        items = getprop(content, "response/capture")
        count = 0
        for item in items:
            count += 1
            print "Fetching %s of %s records from page %s of %s" % \
                  (count, len(items), current_page, total_pages)
            record_url = self.get_records_url.format(item["uuid"])
            error, content = self.request_content_from(record_url)
            if error is None:
                error, content = self.extract_content(content, record_url)

            if error is None:
                record = getprop(content, "response/mods")
                record["_id"] = item["uuid"]
                record["tmp_image_id"] = item.get("imageID")
                record["tmp_item_link"] = item.get("itemLink")
                record["tmp_high_res_link"] = item.get("highResLink")
                records.append(record)

            if error is not None:
                yield error, records, request_more

        yield error, records, request_more

class UVAFetcher(AbsoluteURLFetcher):
    def __init__(self, profile, uri_base, config_file):
        super(UVAFetcher, self).__init__(profile, uri_base, config_file)

    def uva_extract_records(self, content, url):
        error = None
        records = []

        # Handle "mods:<key>" in UVA book collection
        key_prefix = ""
        if "mods:mods" in content:
            key_prefix = "mods:"

        if key_prefix + "mods" in content:
            item = content[key_prefix + "mods"]
            for _id_dict in iterify(item[key_prefix + "identifier"]):
                if _id_dict["type"] == "uri":
                    item["_id"] = _id_dict["#text"]
                    records.append(item)
            if "_id" not in item:
                # Handle localtion url
                for _loc_dict in iterify(item[key_prefix + "location"]):
                    if "url" in _loc_dict:
                        for url in _loc_dict["url"]:
                            if ("usage" in url and
                                url["usage"] == "primary display"):
                                item["_id"] = url.get("#text")
                                records.append(item)

        if not records:
            error = "Error, no records found in content from URL %s" % url

        yield error, records

    def uva_request_records(self, content):
        error = None

        for item in content["mets:mets"]:
            if "mets:dmdSec" in item:
                records = content["mets:mets"][item]
                for rec in records:
                    if not rec["ID"].startswith("collection-description-mods"):
                        url = rec["mets:mdRef"]["xlink:href"]
                        error, cont = self.request_content_from(url)
                        if error is not None:
                            yield error, cont
                        else:
                            error, cont = self.extract_content(cont, url)
                            if error is not None:
                                yield error, cont
                            else:
                                for error, recs in \
                                    self.uva_extract_records(cont, url):
                                    yield error, recs

    def request_records(self, content):
        # UVA will not use the request_more flag
        request_more = False

        for error, records in self.uva_request_records(content):
            yield error, records, request_more

class FileFetcher(Fetcher):
    def __init__(self, profile, uri_base, config_file):
        self.collections = {}
        super(FileFetcher, self).__init__(profile, uri_base, config_file)

    def extract_xml_content(self, filepath):
        error = None
        file = open(filepath, "r")
        try:
            content = XML_PARSE(file)
        except:
            error = "Error parsing content from file %s" % filepath

        return error, content

    def extract_records(self, filepath):
        # Implemented in child classes
        error = "Function extract_records is not implemented"
        records = []

        yield error, records

    def add_provider_to_item_records(self, item_records):
        if item_records:
            for record in item_records:
                if record.get("ingestType") != "collection":
                    record["provider"] = self.contributor

    def create_collection_record(self, hid, title):
        if hid not in self.collections:
            _id = couch_id_builder(self.provider, hid)
            id = hashlib.md5(_id).hexdigest()
            at_id = "http://dp.la/api/collections/" + id

            self.collections[hid] = {
                "id": id,
                "_id": _id,
                "@id": at_id,
                "title": title,
                "ingestType": "collection"
            }

    def fetch_all_data(self):
        """A generator to yield batches of records fetched, and any errors
           encountered in the process, via the self.response dicitonary.
        """
        # The endpoint URL is actually a file path and should have the form:
        # file:/path/to/files/
        if self.endpoint_url.startswith("file:/"):
            path = self.endpoint_url[5:]
            for (root, dirs, files) in os.walk(path):
                filtered_files = fnmatch.filter(files, self.file_filter)
                total_files = len(files)
                file_count = 0
                for filename in fnmatch.filter(files, self.file_filter):
                    file_count += 1
                    print ("Fetching from %s (file %s of %s)" %
                           (filename, file_count, total_files))
                    filepath = os.path.join(root, filename)
                    for errors, records in self.extract_records(filepath):
                        self.response["errors"].extend(errors)
                        self.add_provider_to_item_records(records)
                        self.response["records"].extend(records)
                        # Yield when response["records"] reaches
                        # self.batch_size
                        if len(self.response["records"]) >= self.batch_size:
                            yield self.response
                            self.reset_response()
            # Last yield
            if self.response["errors"] or self.response["records"]:
                yield self.response
        else:
            self.response["error"] = "The endpoint URL must start with " + \
                                     '"file:/"'
            yield self.response

        # Yield the collection records
        if self.collections:
            self.reset_response()
            self.response["records"] = self.collections.values()
            yield self.response

class NARAFetcher(FileFetcher):
    def __init__(self, profile, uri_base, config_file):
        self.file_filter = "Item_*.xml"
        super(NARAFetcher, self).__init__(profile, uri_base, config_file)

    def add_collection_to_item_record(self, hid, item_record):
        item_record["collection"] = dict(self.collections[hid])
        for prop in ["_id", "ingestType"]:
            del item_record["collection"][prop]

    def extract_records(self, file_path):
        errors = []
        records = []

        error, content = self.extract_xml_content(file_path)
        if error is None:
            record = content.get("archival-description")

            try:
                record["_id"] = record["arc-id"]

                hier_items = getprop(record, "hierarchy/hierarchy-item")
                # Keep records whose "hierarchy-item-lod" is "record group"
                for hitem in iterify(hier_items):
                    if hitem["hierarchy-item-lod"].lower() == "record group":
                        hid = hitem["hierarchy-item-id"]
                        title = hitem["hierarchy-item-title"]
                        self.create_collection_record(hid, title)
                        self.add_collection_to_item_record(hid, record)
                        # Append record
                        records.append(record)
                        break
                if not records:
                    errors.append("Record does not have a hierarchy-item-lod" +
                                  " of 'record group' in file %s" % file_path)
            except:
                errors.append("Could not find 'arc_id' in file %s" % file_path)
        else:
            errors.append(error)

        yield errors, records

class EDANFetcher(FileFetcher):
    def __init__(self, profile, uri_base, config_file):
        self.file_filter = "*_DPLA.xml"
        super(EDANFetcher, self).__init__(profile, uri_base, config_file)

    def add_collection_to_item_record(self, hid, item_record):
        if "collection" not in item_record:
            item_record["collection"] = []
        collection = dict(self.collections[hid])
        for prop in ["_id", "ingestType"]:
            del collection[prop]

        item_record["collection"].append(collection)

    def parse(self, doc_list):
        parsed_docs = xmltodict.parse("<docs>" + "".join(doc_list) + "</docs>")
        return parsed_docs["docs"]["doc"]

    def extract_xml_content(self, filepath):
        error = None
        # First <doc> is not on its own line so let's get it there
        cmd = "grep -rl '><doc>' %s | xargs sed -i 's/><doc>/>\\n<doc>/g'" % \
              filepath
        os.system(cmd)

        # Read in batches of self.batch_size
        docs = []
        with open(filepath, "r") as f:
            while True:
                try:
                    line = f.readline()
                    if not line:
                        break
                    if line.startswith("<doc>"):
                        docs.append(line)
                    if len(docs) == self.batch_size:
                        yield error, self.parse(docs)
                        docs = []
                except Exception, e:
                    error = "Error parsing content from file %s: %s" % \
                            (filepath, e)
                    yield error, None
                    break
        # Last yield
        if docs and error is None:
            yield error, self.parse(docs)

    def extract_records(self, file_path):
        def _normalize_collection_title(title):
            """Removes bad characters from collection titles"""
            norm = re.sub(r'[^\w\[\]]+', r'_', title)
            return norm.lower()

        errors = []
        records = []

        for error, content in self.extract_xml_content(file_path):
            if error is None:
                for record in content:
                    try:
                        record["_id"] = \
                            record["descriptiveNonRepeating"]["record_ID"]
                    except:
                        # Exclude records with no record_ID
                        continue

                    set_names = record["freetext"].get("setName")
                    if set_names is None:
                        # Exclude records with no setName
                        continue
                    else:
                        for set in iterify(set_names):
                            title = set["#text"]
                            hid = _normalize_collection_title(title)
                            self.create_collection_record(hid, title)
                            self.add_collection_to_item_record(hid, record)

                    records.append(record)
            else:
                errors.append(error)

            yield errors, records
            errors = []
            records = []

class HathiFetcher(FileFetcher):
    def __init__(self, profile, uri_base, config_file):
        self.file_filter = "*.xml"
        super(HathiFetcher, self).__init__(profile, uri_base, config_file)

    def parse(self, grouped_records, file):
        error = None
        try:
            parsed_xml = XML_PARSE("<group_records>\n<record>\n" + \
                                   "<record>\n".join(grouped_records) + \
                                   "</group_records>")
            parsed_docs = parsed_xml["group_records"]["record"]
        except Exception, e:
            error = "Error parsing grouped records from file %s: %s" % \
                    (file, e)
            parsed_docs = None

        return error, parsed_docs

    def extract_xml_content(self, filepath):
        error = None

        # Read in every self.batch_size docs
        grouped_records = []
        with open(filepath, "r") as f:
            first_group = True
            for key, group in it.groupby(f, lambda line:
                                         line.startswith("<record>")):
                if not key:
                    try:
                        grouped_records.append("".join(list(group)))
                        if first_group:
                            # First item is not a record
                            grouped_records = grouped_records[1:]
                            first_group = False
                        if len(grouped_records) == self.batch_size:
                            error, parsed_docs = self.parse(grouped_records,
                                                            filepath)
                            yield error, parsed_docs
                            grouped_records = []
                    except Exception, e:
                        error = "Error grouping records from file %s: %s" % \
                                (filepath, e)
                        yield error, None
                        break
        # Last yield
        if grouped_records:
            if first_group:
                # First item is not a record
                grouped_records = grouped_records[1:]
            # Strip "</collection>" from last item
            last_record = grouped_records[-1].split("</collection>")[0]
            grouped_records[-1] = last_record
            error, parsed_docs = self.parse(grouped_records, filepath)
            yield error, parsed_docs

    def extract_records(self, file_path):
        errors = []

        for error, records in self.extract_xml_content(file_path):
            if error is None:
                for record in records:
                    if record["controlfield"][0]["tag"] == "001":
                        record["_id"] = record["controlfield"][0]["#text"]
            else:
                errors.append(error)

            yield errors, records
            errors = []

def create_fetcher(profile_path, uri_base, config_file):
    fetcher_types = {
        'ia': lambda p, u, c: IAFetcher(p, u, c),
        'uva': lambda p, u, c: UVAFetcher(p, u, c),
        'mwdl': lambda p, u, c: MWDLFetcher(p, u, c),
        'nypl': lambda p, u, c: NYPLFetcher(p, u, c),
        'nara': lambda p, u, c: NARAFetcher(p, u, c),
        'edan': lambda p, u, c: EDANFetcher(p, u, c),
        'hathi': lambda p, u, c: HathiFetcher(p, u, c),
        'oai_verbs': lambda p, u, c: OAIVerbsFetcher(p, u, c),
    }

    with open(profile_path, "r") as f:
        profile = json.load(f)
    type = profile.get("type")
    fetcher = fetcher_types.get(type)(profile, uri_base, config_file)

    return fetcher
