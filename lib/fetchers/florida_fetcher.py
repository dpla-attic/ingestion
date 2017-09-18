from dplaingestion.fetchers.file_fetcher import FileFetcher
import json


class FloridaFetcher(FileFetcher):
    def __init__(self, profile, uri_base, config_file):
        self.file_filter = "*.json"
        super(FloridaFetcher, self).__init__(profile, uri_base, config_file)

    def extract_records(self, filepath):
        errors = []
        records = []

        with open(filepath, 'r') as f:
            jsonContent = []
            try:
                jsonContent = json.load(f, encoding="utf-8")
            except Exception as e:
                errors.append("Unable to load JSON file: %s" % e.message)

            for record in jsonContent:
                try:
                    # TODO confirm this is correct construction of the _id prop
                    record["originalRecord"] = str(record)
                    record["_id"] = record["sourceResource"]["identifier"]
                except Exception as e:
                    # Collect error messages and exclude records with no
                    # record_ID
                    errors.append(e.message)
                    continue
                records.append(record)
                # Save out records in batches of 500 by default
                if len(records) == self.batch_size:
                    yield errors, records
                    records = []

            yield errors, records
