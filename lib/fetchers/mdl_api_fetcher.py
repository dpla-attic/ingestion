import json
from dplaingestion.fetchers.fetcher import *

class MDLAPIFetcher(Fetcher):
    def __init__(self, profile, uri_base, config_file):
        super(MDLAPIFetcher, self).__init__(profile, uri_base, config_file)
        self.total_records = None
        self.endpoint_url_params = profile.get("endpoint_url_params")

    def extract_content(self, content, url):
        error = None
        try:
            parsed_content = json.loads(content)
        except Exception, e:
            error = "Error parsing content from URL %s: %s" % (url, e)
            return error, content

        content = parsed_content.get("response")

        if not self.total_records:
            total_records_prop = "numFound"
            self.total_records = getprop(content, total_records_prop)

        if content is None:
            error = "Error, there is no \"response\" field in content from " \
                    "URL %s" % url
        # elif exists(content, "response/headers/code"):
        #     code = getprop(content, "response/headers/code")
        #     if code != "200":
        #         error = "Error, response code %s " % code + \
        #                 "is not 200 for request to URL %s" % url
        return error, content

    def mdl_extract_records(self, content):
        error = None
        if not self.total_records:
            total_records_prop = "numFound"
            self.total_records = getprop(content, total_records_prop)
        records = getprop(content, "docs")

        if records:
            records = iterify(records)
            for record in records:
                record["_id"] = getprop(record, "record_id")
                self.get_collection_for_record(record)
        else:
            records = []
            if not error:
                error = "No records found in MDL content: %s" % content

        return error, records


    def request_records(self, content, set_id=None):
        error, records = self.mdl_extract_records(content)
        if error:
            error = "Error at index %s: %s" % \
                    (self.endpoint_url_params["start"],
                     error)

            # Go on to the next start point
            bulk_size = self.endpoint_url_params["per_page"]
            self.endpoint_url_params["start"] += bulk_size
        else:
            self.endpoint_url_params["start"] += len(records)
            print "Fetched %s of %s" % (self.endpoint_url_params["start"],
                                        self.total_records)
        request_more = (int(self.total_records) >
                        int(self.endpoint_url_params["start"]))

        yield error, records, request_more

    def fetch_all_data(self, set):
        """A generator to yield batches of records fetched, and any errors
           encountered in the process, via the self.response dictonary.
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

            error, content = self.extract_content(content,
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

        # Last yield
        self.add_collection_records_to_response()
        if self.response["errors"] or self.response["records"]:
            yield self.response
            self.reset_response()

    def add_collection_records_to_response(self):
        # Create records of ingestType "collection"
        if self.collections:
            self.response["records"].extend(self.collections.values())

    def get_collection_for_record(self, record):
        coll = getprop(record, "record/sourceResource/collection")
        data_provider = getprop(record, "record/sourceResource/dataProvider")
        if coll:
            coll_title = getprop(coll, "title")

            if coll_title:
                collections = []
                for title in filter(None, iterify(coll_title)):
                    if title not in self.collections:
                        self.add_to_collections(coll, data_provider)
                    collections.append(self.collections[title])
                if len(collections) == 1:
                    return collections[0]
                else:
                    return collections
            else:
                return None
        else:
            return None

    def add_to_collections(self, coll, data_provider=None):
        def _normalize(value):
            """Replaced whitespace with underscores"""
            return value.replace(" ", "__")

        if data_provider is None:
            data_provider = self.contributor["name"]

        couch_id_str = "%s--%s" % (data_provider, coll["title"])
        _id = _normalize(
            couch_id_builder(self.provider, couch_id_str)
            )
        id = hashlib.md5(_id.encode("utf-8")).hexdigest()
        at_id = "http://dp.la/api/collections/" + id

        coll_to_update = coll.copy()
        coll_to_update.update({
            "_id" : _id,
            "id": id,
            "@id": at_id,
            "ingestType": "collection"
        })

        desc = coll_to_update.get("description")
        if desc and isinstance(desc, dict):
            if 'dc' in desc.keys():
                coll_to_update["description"] = getprop(coll_to_update, "description/dc/description")
        self.collections[coll_to_update["title"]] = coll_to_update


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
