from dplaingestion.fetchers.absolute_url_fetcher import *

class MWDLFetcher(AbsoluteURLFetcher):
    def __init__(self, profile, uri_base, config_file):
        self.total_records = None
        super(MWDLFetcher, self).__init__(profile, uri_base, config_file)

    def retry_fetches(self):
        """Retries failed fetch attemps and yield any errors and records"""
        records = []
        for func, kwargs in self.retry:
            error, content = func(**kwargs)
            if error is not None:
                yield [error], records
                continue

            error, content = self.extract_content(content, kwargs["url"])
            if error is not None:
                yield [error], records
            else:
                for error, records, request_more in \
                    self.request_records(content, retry=False):
                    if records:
                        self.add_provider_to_item_records(records)
                        self.add_collection_to_item_records(set, records)

                    yield filter(None, [error]), records

    def mwdl_extract_records(self, content):
        error = None
        if not self.total_records:
            total_records_prop = "SEGMENTS/JAGROOT/RESULT/DOCSET/TOTALHITS"
            self.total_records = getprop(content, total_records_prop)
        records = getprop(content, "SEGMENTS/JAGROOT/RESULT/DOCSET/DOC")

        if records:
            records = iterify(records)
            for record in records:
                record["_id"] = getprop(record,
                                        "PrimoNMBib/record/control/recordid")
        else:
            records = []
            error = getprop(content, "SEGMENTS/JAGROOT/RESULT/ERROR/MESSAGE")
            if not error:
                error = "No records found in MWDL content: %s" % content

        return error, records

    def request_records(self, content, retry=True):
        request_more = True
        error, records = self.mwdl_extract_records(content)
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
