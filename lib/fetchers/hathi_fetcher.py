from dplaingestion.fetchers.file_fetcher import *

class HathiFetcher(FileFetcher):
    def __init__(self, profile, uri_base, config_file):
        self.file_filter = "*.xml"
        super(HathiFetcher, self).__init__(profile, uri_base, config_file)

    def parse(self, grouped_records, file):
        error = None
        try:
            parsed_xml = XML_PARSE("<group_records>\n<record>\n" + \
                                   "<record>\n".join(grouped_records) + \
                                   "</group_records>")
            parsed_docs = parsed_xml["group_records"]["record"]
        except Exception, e:
            error = "Error parsing grouped records from file %s: %s" % \
                    (file, e)
            parsed_docs = None

        return error, parsed_docs

    def extract_xml_content(self, filepath):
        error = None

        # Read in every self.batch_size docs
        grouped_records = []
        with open(filepath, "r") as f:
            first_group = True
            for key, group in it.groupby(f, lambda line:
                                         line.startswith("<record>")):
                if not key:
                    try:
                        grouped_records.append("".join(list(group)))
                        if first_group:
                            # First item is not a record
                            grouped_records = grouped_records[1:]
                            first_group = False
                        if len(grouped_records) == self.batch_size:
                            error, parsed_docs = self.parse(grouped_records,
                                                            filepath)
                            yield error, parsed_docs
                            grouped_records = []
                    except Exception, e:
                        error = "Error grouping records from file %s: %s" % \
                                (filepath, e)
                        yield error, None
                        break
        # Last yield
        if grouped_records:
            if first_group:
                # First item is not a record
                grouped_records = grouped_records[1:]
            # Strip "</collection>" from last item
            last_record = grouped_records[-1].split("</collection>")[0]
            grouped_records[-1] = last_record
            error, parsed_docs = self.parse(grouped_records, filepath)
            yield error, parsed_docs

    def extract_records(self, file_path):
        errors = []

        for error, records in self.extract_xml_content(file_path):
            if error is None:
                for record in records:
                    if record["controlfield"][0]["tag"] == "001":
                        record["_id"] = record["controlfield"][0]["#text"]
            else:
                errors.append(error)

            yield errors, records
            errors = []
