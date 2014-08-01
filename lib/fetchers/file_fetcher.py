from dplaingestion.fetchers.fetcher import *

class FileFetcher(Fetcher):
    def __init__(self, profile, uri_base, config_file):
        self.collections = {}
        super(FileFetcher, self).__init__(profile, uri_base, config_file)

    def extract_xml_content(self, filepath):
        error = None
        content = None
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

    def fetch_all_data(self, set=None, limit=None):
        """A generator to yield batches of records fetched, and any errors
           encountered in the process, via the self.response dicitonary.

           set is ignored, for now.
           limit is for testing, to limit the total number of records fetched.
        """
        # The endpoint URL is actually a file path and should have the form:
        # file:/path/to/files/
        if self.endpoint_url.startswith("file:/"):
            path = self.endpoint_url[5:]
            # total_rec_count is for use with limit
            total_rec_count = 0
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
                        total_rec_count += len(self.response["records"])
                        if limit and total_rec_count >= limit:
                            raise StopIteration
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
