import os
import re
import sys
import time
import fnmatch
import xmltodict
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

def getprop(obj, path):
    return get_prop(obj, path, keyErrorAsNone=True)

def iterify(iterable):
    '''
    Treat iterating over a single item or an interator seamlessly.
    '''
    if (isinstance(iterable, basestring) \
        or isinstance(iterable, dict)):
        iterable = [iterable]
    try:
        iter(iterable)
    except TypeError:
        iterable = [iterable]
    return iterable

XML_PARSE = lambda doc: xmltodict.parse(doc, xml_attribs=True, attr_prefix='',
                                        force_cdata=False,
                                        ignore_whitespace_cdata=True)

class Fetcher(object):
    """The base class for all fetchers.
       Includes attributes and methods that are common to all types.
    """
    def __init__(self, profile, uri_base):
        """Set common attributes"""
        self.uri_base = uri_base
        self.provider = profile.get("name")
        self.blacklist = profile.get("blacklist")
        self.contributor = profile.get("contributor")
        self.subresources = profile.get("subresources")
        self.endpoint_url = profile.get("endpoint_url")
        self.collection_titles = profile.get("collection_titles")
        self.http_handle = httplib2.Http('/tmp/.pollcache')
        self.http_handle.force_exception_as_status_code = True

    def remove_blacklisted_subresources(self):
        if self.blacklist:
            for set in self.blacklist:
                if set in self.subresources:
                    del self.subresources[set]

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
            error = "No sets received from URL %s" %  url

        return error, content

class OAIVerbsFetcher(Fetcher):
    def __init__(self, profile, uri_base):
        self.metadata_prefix = profile.get("metadata_prefix")
        super(OAIVerbsFetcher, self).__init__(profile, uri_base)

    def set_subresources(self):
        """Sets self.subresources

           Returns the error, else None
        """
        if self.subresources == "NotSupported":
            self.subresources = {"": None}
        else:
            # Fetch all sets
            error, sets = self.list_sets()
            if error is not None:
                self.subresources = {}
                return error
            elif sets:
                if not self.subresources:
                    self.subresources = sets
                    self.remove_blacklisted_subresources()
                else:
                    for set in sets.keys():
                        if set not in self.subresources:
                            del sets[set]
                    self.subresources = sets

    def list_sets(self):
        """Requests all sets via the ListSets verb

           Returns an (error, sets) tuple where error is None if the
           request succeeds, the requested content is not empty and is
           parseable, and sets is a dictionary with setSpec as keys and
           a dictionary with keys title and description as the values.
        """
        sets = {}
        list_sets_url = self.uri_base + "/oai.listsets.json?endpoint=" + \
                        self.endpoint_url

        error, content = self.request_content_from(list_sets_url)
        if error is None:
            try:
                list_sets_content = json.loads(content)
            except ValueError:
                error = "Error decoding content from URL %s" % list_sets_url
                return error, subresources

            for s in list_sets_content:
                spec = s["setSpec"]
                sets[spec] = {"id": spec}
                sets[spec]["title"] = s["setName"]
                if "setDescription" in s:
                    sets[spec]["description"] = s["setDescription"]

            if not sets:
                error = "No sets received from URL %s" % list_sets_url

        return error, sets

    def list_records(self, url, params):
        """Requests records via the ListRecords verb

           Returns an (error, records_content) tuple where error is None if the
           request succeeds, the requested content is not empty and is
           parseable, and records is a dictionary with key "items".
        """
        records_content = {}
        list_records_url = self.uri_base + "/dpla-list-records?endpoint=" + url

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

    def fetch_all_data(self):
        """A generator to yield batches of records along with the collection,
           if any, the provider, and any errors encountered along the way. The
           reponse dictionary has the following structure:

            response = {
                "error": <Any error encountered>,
                "data": {
                    "provider": <The provider>,
                    "contributor": <The contributor>,
                    "records": <The batch of records fetched>,
                    "collection": {
                        "title": <The collection title, if any>,
                        "description": <The collection description, if any>
                    }
                }
            }
        """
        response = {
            "error": None,
            "data": {
                "provider": self.provider,
                "contributor": self.contributor,
                "records": None,
                "collection": None
            }
        }

        # Set the subresources
        response["error"] = self.set_subresources()
        if response["error"] is not None:
            yield response

        # Fetch all records for each subresource
        for subresource in self.subresources.keys():
            print "Fetching records for subresource " + subresource

            # Set response["data"]["collection"] and initial params
            params = {}
            if not subresource == "":
                params["oaiset"] = subresource
                setprop(response, "data/collection",
                        self.subresources[subresource])
        
            request_more = True
            resumption_token = ""
            url = self.endpoint_url

            # Set the metadataPrefix
            if self.metadata_prefix is not None:
                params["metadataPrefix"] = self.metadata_prefix

            # Flag to remove subresource if no records fetched
            remove_subresource = True

            # Request records until a resumption token is not received
            while request_more:
                # Send request
                response["error"], content = self.list_records(url, params)

                if response["error"] is not None:
                    # Stop requesting from this subresource
                    request_more = False
                else:
                    # Get resumption token
                    remove_subresource = False
                    setprop(response, "data/records", content["items"])
                    resumption_token = content.get("resumption_token")
                    if (resumption_token is not None and
                        len(resumption_token) > 0):
                        # Add resumption token
                        params["resumption_token"] = resumption_token
                    else:
                        request_more = False

                yield response

            if remove_subresource:
                del self.subresources[subresource]

class AbsoluteURLFetcher(Fetcher):
    def __init__(self, profile, uri_base):
        self.get_sets_url = profile.get("get_sets_url")
        self.get_records_url = profile.get("get_records_url")
        self.endpoint_url_params = profile.get("endpoint_url_params")
        super(AbsoluteURLFetcher, self).__init__(profile, uri_base)

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

    def set_subresources(self):
        """Sets self.subresources
           Overriden in child classes

           Returns the error, else None
        """
        if isinstance(self.subresources, dict):
            subresources = {}
            for k, v in self.subresources.items():
                subresources[k] = {
                    "id": k,
                    "title": v.get("title"),
                    "description": v.get("description")
                }
            self.subresources = subresources
        elif self.subresources == "NotSupported":
            self.subresources = {"": None}
        else:
            self.subresources = {}
            error = "If subresources are not supported then the " + \
                    "subresources field in the profile must be set to " + \
                    "\"NotSupported\""
            return error

    def request_records(self):
        # Implemented in child classes
        pass

    def fetch_all_data(self):
        """A generator to yield batches of records along with the collection,
           if any, the provider, and any errors encountered along the way. The
           reponse dictionary has the following structure:

            response = {
                "error": <Any error encountered>,
                "data": {
                    "provider": <The provider>,
                    "contributor": <The contributor>,
                    "records": <The batch of records fetched>,
                    "collection": {
                        "title": <The collection title, if any>,
                        "description": <The collection description, if any>
                    }
                }
            }
        """
        response = {
            "error": None,
            "data": {
                "provider": self.provider,
                "contributor": self.contributor,
                "records": None,
                "collection": None
            }
        }

        # Set the subresources
        response["error"] = self.set_subresources()
        if response["error"] is not None:
            yield response

        # Request records for each subresource
        for subresource in self.subresources.keys():
            print "Fetching records for subresource " + subresource

            # Set response["data"]["collection"]
            if not subresource == "":
                setprop(response, "data/collection",
                        self.subresources[subresource])

            request_more = True
            if subresource:
                url = self.endpoint_url.format(subresource)
            else:
                url = self.endpoint_url
            params = self.endpoint_url_params

            while request_more:
                response["error"], content = self.request_content_from(
                    url, self.endpoint_url_params
                    )

                if response["error"] is not None:
                    # Stop requesting from this subresource
                    request_more = False
                    yield response
                    continue

                response["error"], content = self.extract_content(content, url)
                if response["error"] is not None:
                    request_more = False
                    yield response
                else:
                    for response["error"], response["data"]["records"], \
                        request_more in self.request_records(content):
                        yield response

class IAFetcher(AbsoluteURLFetcher):
    def __init__(self, profile, uri_base):
        super(IAFetcher, self).__init__(profile, uri_base)
        self.count = 0
        self.errors = []
        self.get_file_url = profile.get("get_file_url")
        self.prefix_files = profile.get("prefix_files")
        self.prefix_meta = profile.get("prefix_meta")
        self.prefix_dc = profile.get("prefix_dc")
        self.shown_at_url = profile.get("shown_at_url")
        self.fetch_pool = None
        self.queue = Queue(maxsize=self.endpoint_url_params["rows"])
        self.file_url_reqs = 0
        self.file_meta_reqs = 0
        self.file_marc_reqs = 0

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

    def request_then_extract_xml(self, url):
        error, content = self.request_content_from(url)
        try:
            error, content = self.extract_xml_content(content, url)
        except:
            pass

        return error, content

    def request_records(self, content):
        self.fetch_pool = Pool(processes=3)

        total_records = int(content["numFound"])
        read_records = int(content["start"])
        expected_records = self.endpoint_url_params["rows"]
        request_more = (total_records - read_records) > expected_records
        self.endpoint_url_params["page"] += 1

        self.count = 0
        for doc in content["docs"]:
            id = doc["identifier"]
            self.fetch_pool.apply_async(self.fetch_record, [id])
        self.fetch_pool.close()
        self.fetch_pool.join()

        records = []
        while (self.count != expected_records):
            try:
                records.append(self.queue.get(block=False))
            except:
                pass

            if len(records) == 10:
                # Yield every 10 records
                if request_more:
                    print "Fetched %s of %s" % (read_records +
                                                expected_records,
                                                total_records)
                else:
                    print "Fetched %s of %s" % (total_records, total_records)
                yield self.errors, records, request_more
                self.errors = []
                records = []

        # Last yield
        if request_more:
            print "Fetched %s of %s" % (read_records + expected_records,
                                        total_records)
        else:
            print "Fetched %s of %s" % (total_records, total_records)
        yield self.errors, records, request_more
            
    def fetch_record(self, id):
        """Fetchers a record and places it in the queue"""
        file_url = self.get_file_url.format(id,
                                            self.prefix_files.format(id))

        error, content = self.request_then_extract_xml(file_url)
        if error is not None:
            self.errors.append(error)
            self.count += 1
            return None

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
            errors.append("Document %s meta data is absent" % id)
            self.count += 1
            return None

        record = {
            "files": record_data,
            "_id":  id
        }

        meta_url = self.get_file_url.format(id, record_data["meta"])
        error, content = self.request_then_extract_xml(meta_url)
        if error is not None:
            self.count += 1
            self.errors.append(error)
            return None
        record.update(content)

        if record_data["marc"]:
            marc_url = self.get_file_url.format(
                           id, record_data["marc"]
                       )
            error, content = self.request_then_extract_xml(marc_url)
            if error is not None:
                self.count += 1
                errors.append(error)
                return None
            record.update(content)

        self.queue.put(record, block=False)
        self.count += 1

class MWDLFetcher(AbsoluteURLFetcher):
    def __init__(self, profile, uri_base):
        self.count = 0
        super(MWDLFetcher, self).__init__(profile, uri_base)

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
        self.count += self.endpoint_url_params["indx"]
        if self.count >= total_records:
            request_more = False

        yield error, records, request_more

class NYPLFetcher(AbsoluteURLFetcher):
    def __init__(self, profile, uri_base):
        super(NYPLFetcher, self).__init__(profile, uri_base)

    def set_subresources(self):
        """Sets self.subresources

           Returns the error, else None
        """
        self.subresources = {}

        url = self.get_sets_url
        error, content = self.request_content_from(url)
        if error is not None:
            return error

        error, content = self.extract_content(content, url)
        if error is not None:
            return error

        subresources = {}
        for item in content["response"]:
            if item == "collection":
                for coll in content["response"][item]:
                    if "uuid" in coll:
                        subresources[coll["uuid"]] = {
                            "id": coll["uuid"],
                            "title": coll["title"]
                        }

        if not subresources:
            error = "Error, no subresources from URL %s" % url
            return error

        self.subresources = subresources

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

        records = []
        for item in getprop(content, "response/capture"):
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
    def __init__(self, profile, uri_base):
        super(UVAFetcher, self).__init__(profile, uri_base)

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
    def __init__(self, profile, uri_base):
        self.collections = {}
        super(FileFetcher, self).__init__(profile, uri_base)

    def extract_xml_content(self, filepath):
        error = None
        file = open(filepath, "r")
        try:
            content = XML_PARSE(file)
        except:
            error = "Error parsing content from file %s" % filepath

        return error, content

    def extract_collection_and_record(self, filepath):
        # Implemented in child classes
        error = "Function extract_collection_and_record is not implemented"
        collection, record = None, None

        yield error, collection, record

    def fetch_all_data(self):
        """A generator to yield batches of records along with the collection,
           if any, the provider, and any errors encountered along the way. The
           reponse dictionary has the following structure:

            response = {
                "error": <Any error encountered>,
                "data": {
                    "provider": <The provider>,
                    "contributor": <The contributor>,
                    "records": <The batch of records fetched>,
                    "collection": {
                        "title": <The collection title, if any>,
                        "description": <The collection description, if any>
                    }
                }
            }
        """
        response = {
            "error": [],
            "data": {
                "provider": self.provider,
                "contributor": self.contributor,
                "records": None,
                "collection": None
            }
        }

        # The endpoint URL is actually a file path and should have the form:
        # file:/path/to/files/
        if self.endpoint_url.startswith("file:/"):
            path = self.endpoint_url[5:]
            for (root, dirs, files) in os.walk(path):
                for filename in fnmatch.filter(files, self.file_filter):
                    filepath = os.path.join(root, filename)
                    for (error, collection, record) in \
                        self.extract_collection_and_record(filepath):
                        if error is not None:
                           response["error"].append(error)
                        elif record is not None:
                            if collection is not None:
                                id = collection["id"]
                                if id not in self.collections:
                                    self.collections[id] = {
                                        "title": collection["title"],
                                        "records": []
                                    }
                                self.collections[id]["records"].append(record)
                            else:
                                pass
                                #response["error"].append("Records without " +
                                #                         "collection found " +
                                #                         "in %s" % filepath)
                        else:
                           response["error"].append("Did not retrieve " +
                                                    "record from file " +
                                                    "%s" % filepath)

                        # Let's not let self.collections get too big
                        if sum([len(v["records"]) for v in
                               self.collections.values()]) > 1000:
                            # Yield collection with most records
                            ids = sorted(self.collections,
                                         key=lambda k: len(self.collections[k]))
                            coll = {
                                "id": ids[-1],
                                "title": self.collections[ids[-1]]["title"]
                            }
                            coll_records = self.collections[ids[-1]]["records"]
                            setprop(response, "data/collection", coll)
                            setprop(response, "data/records", coll_records)
                            del self.collections[ids[-1]]

                            yield response
                            response["error"] = []

            if self.collections:
                for id in self.collections:
                    coll = {
                        "id": id,
                        "title": self.collections[id]["title"]
                    }
                    coll_records = self.collections[id]["records"]
                    setprop(response, "data/collection", coll)
                    setprop(response, "data/records", coll_records)

                    yield response
            else:
                yield response
        else:
            response["error"] = 'The endpoint URL must start with "file:/"'
            yield response

class NARAFetcher(FileFetcher):
    def __init__(self, profile, uri_base):
        self.file_filter = "Item_*.xml"
        super(NARAFetcher, self).__init__(profile, uri_base)

    def extract_collection_and_record(self, file_path):
        error, collection, record = None, None, None

        error, content = self.extract_xml_content(file_path)
        if error is None:
            record = content.get("archival-description")

            try:
                record["_id"] = record["arc-id"]

                hier_items = getprop(record, "hierarchy/hierarchy-item")
                for hi in iterify(hier_items):
                    htype = hi["hierarchy-item-lod"]
                    # Record Group mapped to collection
                    if htype.lower() == "record group":
                        collection = {
                            "id": hi["hierarchy-item-id"],
                            "title": hi["hierarchy-item-title"]
                        }
                        break
            except:
                error = 'Could not find "arc_id" in file %s' % file_path

        yield error, collection, record

class EDANFetcher(FileFetcher):
    def __init__(self, profile, uri_base):
        self.file_filter = "*_DPLA.xml"
        super(EDANFetcher, self).__init__(profile, uri_base)

    def extract_xml_content(self, filepath):
        error = None
        file = open(filepath, "r")
        try:
            content = xmltodict.parse(file)
        except:
            error = "Error parsing content from file %s" % filepath

        return error, content

    def extract_collection_and_record(self, file_path):
        def _normalize_collection_name(name):
            """Removes bad characters from collection names"""
            norm = re.sub(r'[^\w\[\]]+', r'_', name)
            return norm.lower()

        error, collection, record = None, None, None

        error, content = self.extract_xml_content(file_path)
        if error is None:
            for record in getprop(content, "response/result/doc"):
                try:
                    record["_id"] = \
                        record["descriptiveNonRepeating"]["record_ID"]
                except:
                    error = 'Could not find "record_ID" in file %s' % file_path
                    yield error, collection, record
                    continue

                if not "setName" in record["freetext"]:
                    yield error, collection, record
                    continue
                    
                setname = getprop(record, "freetext/setName")
                for s in iterify(setname):
                    if "#text" in s:
                        collection = {
                            "id": _normalize_collection_name(s["#text"]),
                            "title": s["#text"]
                        }
                        break

                yield error, collection, record
                collection = None
        else:
            yield error, collection, record

def create_fetcher(profile_path, uri_base):
    fetcher_types = {
        "ia": lambda p, u: IAFetcher(p, u),
        'uva': lambda p, u: UVAFetcher(p, u),
        'mwdl': lambda p, u: MWDLFetcher(p, u),
        'nypl': lambda p, u: NYPLFetcher(p, u),
        'nara': lambda p, u: NARAFetcher(p, u),
        'edan': lambda p, u: EDANFetcher(p, u),
        'oai_verbs': lambda p, u: OAIVerbsFetcher(p, u),
    }


    with open(profile_path, "r") as f:
        profile = json.load(f)
    type = profile.get("type")
    fetcher = fetcher_types.get(type)(profile, uri_base)

    return fetcher
