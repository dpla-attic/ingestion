from dplaingestion.fetchers.absolute_url_fetcher import *
import logging

class LOCFetcher(AbsoluteURLFetcher):
    logger = None

    def __init__(self, profile, uri_base, config_file):
        super(LOCFetcher, self).__init__(profile, uri_base, config_file)
        self.item_params = profile.get("get_item_params")
        logging.basicConfig(filename='logs/harvest_error.log',
                            level=logging.ERROR,
                            format='%(asctime)s %(levelname)s %(name)s %('
                                   'message)s')
        self.logger = logging.getLogger(__name__)


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

            # TODO de-kludge
            # https://github.com/dpla/heidrun/blob/develop/app/harvesters/loc_harvester.rb#L185-L186
            urls = [get_prop(item, "aka", True),
                    get_prop(item, "id", True),
                    get_prop(item, "url", True)]

            urls = set([item for sublist in urls for item in sublist])
            urls = [s for s in urls if "http://www.loc.gov/item/" in s]

            if not urls:
                self.logger.error("Missing item url property [aka,id,url]")
                continue

            record_url = urls[0]
            error, content = self.request_content_from(url=record_url,
                                                       params=self.item_params)
            if error is None:
                error, content = self.extract_content(content, record_url)

            if not exists(content, "item/library_of_congress_control_number") \
                    and not exists(content, "item/control_number"):
                error = "Missing required *control property. " + record_url
                self.logger.error(error)
                continue

            if error is None:
                # TODO Is this correct formation of the _id value?
                record = content["item"]
                # This is a kludge but lets see how many harvest errors this
                # fixes...
                if exists(record, "library_of_congress_control_number"):
                    record["_id"] = record["library_of_congress_control_number"]
                else:
                    record["_id"] = record["control_number"]
                records.append(record)

            if error is not None:
                yield error, records, request_more

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
                print "Requesting %s %s" % (url, self.endpoint_url_params)
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
