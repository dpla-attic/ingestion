import logging
import time
from urllib import urlencode

import sys
from dplaingestion.fetchers.absolute_url_fetcher import AbsoluteURLFetcher
from dplaingestion.selector import exists, getprop
from dplaingestion.utilities import iterify


class LOCFetcher(AbsoluteURLFetcher):
    # Library of Congress specific file-based logger. Will probably be used to
    # log the performance of their API
    logger = None

    def __init__(self, profile, uri_base, config_file):
        super(LOCFetcher, self).__init__(profile, uri_base, config_file)
        self.item_params = profile.get("get_item_params")
        self.retry = []
        fname = "logs/error_loc_harvest_%s.log" % time.strftime("%Y%m%d-%H%M%S")
        logging.basicConfig(filename=fname,
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

        current_params = self.endpoint_url_params
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

            # Checks for the presence of an item url in the item record within
            # a collection results page. If one does not exist the item cannot
            # be harvested. Every item is expected to have one and those that
            # do not *should* be reported to LoC
            # FIXME Only the ID field needs to be checked. Confirm on call.

            # See Requesting a specific item
            # https://libraryofcongress.github.io/data-exploration/requests.html

            # BEGIN original version of harvester
            # urls = getprop(item, "id", True)
                    # getprop(item, "aka", True),
                    # getprop(item, "url", True)]

            # urls = set([item for sublist in urls for item in sublist])
            # urls = [s for s in urls if "www.loc.gov/item/" in s]
            # END original version
            
            record_url = getprop(item, "id", True)

            # If the 'id' property is not present or if its value does not
            # contain 'loc.gov/item' then it is not an item URL that can be
            # harvested. Log the error and move on
            if not record_url or "loc.gov/item" not in record_url:
                self.logger.error("loc.gov/item/ value in 'id' property is "
                                  "missing or invalid for item #%s\n"
                                  "Request URL: %s?%s\n"
                                  "item: %s" % (count, self.endpoint_url.format(set_id),
                                                urlencode(current_params), item))
                break

            error, content = self.request_content_from(url=record_url,
                                                       params=self.item_params)
            if error is None:
                error, content = self.extract_content(content, record_url)

            if error is None and (not exists(content,"item/id")):
                self.logger.error("Item is missing required ID property. " +
                                  record_url)
                break

            if error is None:
                # TODO Is this correct formation of the _id value?
                record = content["item"]
                # Change basis of DPLA ID to the LoC id property
                record["_id"] = record["id"]
                records.append(record)

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
                # Give the user some information about requests
                rqst_info_msg = "Requesting %s?%s" % (url, urlencode(self.endpoint_url_params))
                print rqst_info_msg # prints to console
                logging.info(rqst_info_msg) # log to file

                error, content = self.request_content_from(url, self.endpoint_url_params)

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
            for error, records in self.retry_fetchess():
                self.response["errors"].extend(error)
                self.response["records"].extend(records)

                if len(self.response["records"]) >= self.batch_size:
                    yield self.response
                    self.reset_response()

            if self.response["errors"] or self.response["records"]:
                yield self.response
