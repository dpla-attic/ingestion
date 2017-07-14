from amara.thirdparty import json
from dplaingestion.utilities import iterify
from dplaingestion.fetchers.fetcher import Fetcher, getprop


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
        """Fetch all sets in a collection.

        A set is returned by the OAI endpoint as an object with properties
        setDescription, setName, and setSpec (see list_sets()).  We return
        a dictionary keyed on the short setSpec token, where values are
        {'title': <the set name>}

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
                    if "setDescription" in s:
                        sets[set_spec]["description"] = \
                            s["setDescription"].strip()

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

        return error, records_content

    def add_collection_to_item_records(self, item_records):
        for item_record in item_records:
            collections = []

            # setSpec may be in the item's root or in header
            set_specs = getprop(item_record, "header/setSpec")
            if not set_specs:
                set_specs = item_record.get("setSpec", [])

            for set_spec in iterify(set_specs):
                collection = self.collections.get(set_spec)
                if collection:
                    collections.append(collection)
            if collections:
                if len(collections) == 1:
                    item_record["collection"] = collections[0]
                else:
                    item_record["collection"] = collections

    def set_collections(self):
        """Assign collections with the dictionary of OAI sets from the provider.
        
           The collection dictionary is keyed on the short "setSpec" token.
        """
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

    def fetch_all_data(self, one_set=None):
        """A generator to yield batches (responses) of records fetched, and any
           errors encountered in the process, via the self.response dicitonary.

           Records can be for collections or items.

           one_set is ignored for now.
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
