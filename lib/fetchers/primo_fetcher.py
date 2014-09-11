from dplaingestion.fetchers.fetcher import *
from urllib import urlencode
import threading

class PrimoFetcher(Fetcher):
    def __init__(self, profile, uri_base, config_file):
        self.root_key = "PrimoNMBib/record/"
        self.retry = []
        self.total_records = None
        self.endpoint_url_params = profile.get("endpoint_url_params")
        super(PrimoFetcher, self).__init__(profile, uri_base,
                                                 config_file)

    def extract_xml_content(self, content, url):
        error = None
        try:
            content = XML_PARSE(content)
        except:
            error = "Error parsing content from URL %s" % url

        return error, content


    def retry_fetches(self):
        """Retries failed fetch attemps and yield any errors and records"""
        records = []
        for func, kwargs in self.retry:
            error, content = func(**kwargs)
            if error is not None:
                yield [error], records
                continue

            error, content = self.extract_xml_content(content, kwargs["url"])
            if error is not None:
                yield [error], records
            else:
                for error, records, request_more in \
                    self.request_records(content, retry=False):
                    if records:
                        self.add_provider_to_item_records(records)
                        self.add_collection_to_item_records(records)

                    yield filter(None, [error]), records

    def primo_extract_records(self, content):
        error = None
        if not self.total_records:
            total_records_prop = "sear:SEGMENTS/sear:JAGROOT/sear:RESULT" \
                                 "/sear:DOCSET/TOTALHITS"
            self.total_records = getprop(content, total_records_prop)
        records = getprop(content, "sear:SEGMENTS/sear:JAGROOT/sear:RESULT"
                                   "/sear:DOCSET/sear:DOC")

        if records:
            records = iterify(records)
            for record in records:
                record["_id"] = getprop(record,
                                        "PrimoNMBib/record/control/recordid")
        else:
            records = []
            # Elements in the error response are not namespaced
            error = getprop(content, "SEGMENTS/JAGROOT/RESULT/ERROR/MESSAGE")
            if not error:
                error = "No records found in Primo content: %s" % content

        return error, records

    def add_to_collections(self, coll_title, data_provider=None):
        def _normalize(value):
            """Replaced whitespace with underscores"""
            return value.replace(" ", "_")

        if data_provider is None:
            data_provider = self.contributor["name"]

        couch_id_str = "%s--%s" % (data_provider, coll_title)
        _id = _normalize(
            couch_id_builder(self.provider, couch_id_str)
            )
        id = hashlib.md5(_id.encode("utf-8")).hexdigest()
        at_id = "http://dp.la/api/collections/" + id

        self.collections[coll_title] = {
            "_id" : _id,
            "id": id,
            "@id": at_id,
            "title": coll_title,
            "ingestType": "collection"
        }

    def get_collection_for_record(self, record):
        pass

    def add_collection_to_item_records(self, records):
        def _clean_collection(collection):
            exclude = ("_id", "ingestType")
            if isinstance(collection, list):
                clean_collection = []
                for coll in collection:
                    clean_collection.append(
                        {k:v for k, v in coll.items() if k not in exclude}
                        )
            else:
              clean_collection = {k:v for k, v in collection.items() if
                                  k not in exclude}
            return clean_collection

        for record in records:
            collection = self.get_collection_for_record(record)
            if collection:
                record["collection"] = _clean_collection(collection)
                _clean_collection(record)

    def request_records(self, content, retry=True):
        error, records = self.primo_extract_records(content)
        if error:
            if retry:
                print >> sys.stderr, "Error at indx %s: %s" % \
                                      (self.endpoint_url_params["indx"], error)
                self.retry.append((self.request_content_from,
                                   {"url": self.endpoint_url,
                                    "params": self.endpoint_url_params}))
            else:
                error = "Error at index %s: %s" % \
                        (self.endpoint_url_params["indx"],
                         error)

            # Go on to the next indx
            bulk_size = self.endpoint_url_params["bulkSize"]
            self.endpoint_url_params["indx"] += bulk_size
        else:
            self.endpoint_url_params["indx"] += len(records)
            print "Fetched %s of %s" % (self.endpoint_url_params["indx"] - 1,
                                        self.total_records)
        request_more = (int(self.total_records) >=
                        int(self.endpoint_url_params["indx"]))

        yield error, records, request_more

    def add_collection_records_to_response(self):
        # Create records of ingestType "collection"
        if self.collections:
            self.response["records"].extend(self.collections.values())

    def fetch_all_data(self, set):
        """A generator to yield batches of records fetched, and any errors
           encountered in the process, via the self.response dicitonary.
        """

        request_more = True
        while request_more:

            error, content = self.request_content_from(
                self.endpoint_url, self.endpoint_url_params
                )
            print "Requesting %s?%s" % (self.endpoint_url,
                                        urlencode(self.endpoint_url_params,
                                                  True))

            if error is not None:
                # Stop requesting from this set
                request_more = False
                self.response["errors"].append(error)
                break

            error, content = self.extract_xml_content(content,
                                                      self.endpoint_url)
            if error is not None:
                request_more = False
                self.response["errors"].extend(iterify(error))
            else:
                for error, records, request_more in \
                        self.request_records(content):
                    if error is not None:
                        self.response["errors"].extend(iterify(error))
                    self.add_provider_to_item_records(records)
                    self.add_collection_to_item_records(records)
                    self.response["records"].extend(records)
                    if len(self.response["records"]) >= self.batch_size:
                        yield self.response
                        self.reset_response()

        # Retry fetches, if any
        if self.retry:
            print >> sys.stderr, "Retrying %s fetches..." % \
                                 len(self.retry)
            for error, records in self.retry_fetches():
                self.response["errors"].extend(error)
                self.response["records"].extend(records)

                if len(self.response["records"]) >= self.batch_size:
                    yield self.response
                    self.reset_response()

            if self.response["errors"] or self.response["records"]:
                yield self.response

        # Last yield
        self.add_collection_records_to_response()
        if self.response["errors"] or self.response["records"]:
            yield self.response
            self.reset_response()


