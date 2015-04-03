from dplaingestion.fetchers.fetcher import *
import threading


class AbsoluteURLFetcher(Fetcher):
    def __init__(self, profile, uri_base, config_file):
        self.get_sets_url = profile.get("get_sets_url")
        self.get_records_url = profile.get("get_records_url")
        self.endpoint_url_params = profile.get("endpoint_url_params")
        self.retry = []
        super(AbsoluteURLFetcher, self).__init__(profile, uri_base,
                                                 config_file)

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
                return error
            elif sets:
                self.collections = sets

    def request_records(self, content, set_id):
        # Implemented in child classes
        pass

    def add_collection_to_item_records(self, set, records):
        collection = self.collections.get(set)
        if collection:
            for record in records:
                record["collection"] = collection

    def fetch_all_data(self, set_id=None):
        """A generator to yield batches of records fetched, and any errors
           encountered in the process, via the self.response dicitonary.
        """

        if set_id:
            self.sets = [set_id]
            self.collections = {set_id: {"id": set_id}}

        error = self.set_collections()
        if error:
            self.response["errors"].append(error)
            yield self.response
        elif not self.collections:
            self.response["errors"] = "If sets are not supported then the " + \
                                      "sets field in the profile must be " + \
                                      "set to \"NotSupported\""
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
        for set_id in self.collections.keys():

            request_more = True
            if set_id:
                url = self.endpoint_url.format(set_id)
            else:
                url = self.endpoint_url

            while request_more:

                error, content = self.request_content_from(
                    url, self.endpoint_url_params
                    )
                print "requesting %s %s" % (url, self.endpoint_url_params)

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
                            self.request_records(content=content,
                                                 set_id=set_id):
                        if error is not None:
                            self.response["errors"].extend(iterify(error))
                        if records:
                            self.add_provider_to_item_records(records)
                            self.add_collection_to_item_records(set_id,
                                                                records)
                            self.response["records"].extend(records)
                        if len(self.response["records"]) >= self.batch_size:
                            yield self.response
                            self.reset_response()

        # Last yield
        if self.response["errors"] or self.response["records"]:
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
