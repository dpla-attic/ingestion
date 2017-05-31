from dplaingestion.fetchers.absolute_url_fetcher import *
from akara import logger


class LOCFetcher(AbsoluteURLFetcher):
    def __init__(self, profile, uri_base, config_file):
        super(LOCFetcher, self).__init__(profile, uri_base, config_file)
        # token = self.config.get("APITokens", "LOC")
        # authorization = self.http_headers["Authorization"].format(token)
        # self.http_headers["Authorization"] = authorization

    def fetch_sets(self):
        """Fetches all sets

           Returns an (error, sets) tuple
        """
        error = None
        sets = {}
        if self.sets:
            for set_id in self.sets:
                sets[set_id] = {
                    "id": set_id
                }
        else:
            error = "No sets defined in Library of Congress profile."

        return error, sets

    def request_records(self, content, set_id):
        self.endpoint_url_params["sp"] += 1
        error = None
        total_pages = getprop(content, "pagination/total")
        current_page = getprop(content, "pagination/current")
        request_more = total_pages != current_page
        if not request_more:
            # Reset the page for the next collection
            self.endpoint_url_params["sp"] = 1

        records = []
        items = getprop(content, "results")
        count = 0

        for item in iterify(items):
            count += 1

            if error is None:
                record_id = filter(None, item["id"].split("/"))[-1]
                record_url = self.get_records_url.format(record_id)
                error, record_content = self.request_content_from(record_url)

            if error is None:
                error, record_content = self.extract_content(record_content,
                                                             record_url)

            if error is None:
                # Extract record from response
                record = record_content["item"]
                # TODO Is this correct formation of the _id value?
                record["_id"] = record["library_of_congress_control_number"]
                records.append(record)

            if error is not None:
                yield error, records, request_more
                print "Error %s, " % error +\
                      "but fetched %s of %s records from page %s of %s" % \
                      (count, len(items), current_page, total_pages)

        yield error, records, request_more

    def extract_content(self, content, url):
        """Calls extract_json_content by default but can be overriden in
           child classes
        """
        return self.extract_json_content(content, url)


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

                print "requesting %s %s" % (url, self.endpoint_url_params)
                error, content = self.request_content_from(
                    url, self.endpoint_url_params
                    )


                if error is not None:
                    print ("ERROR after requesting item %s" % error)
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
