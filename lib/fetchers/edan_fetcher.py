import os
import re
import xmltodict
from dplaingestion.utilities import iterify
from dplaingestion.fetchers.file_fetcher import FileFetcher
from xml.parsers.expat import ExpatError
import tempfile
import sys
import re

class EDANFetcher(FileFetcher):
    def __init__(self, profile, uri_base, config_file):
        self.file_filter = "*_DPLA.xml"
        super(EDANFetcher, self).__init__(profile, uri_base, config_file)

    def add_collection_to_item_record(self, hid, item_record):
        if "collection" not in item_record:
            item_record["collection"] = []
        collection = dict(self.collections[hid])
        for prop in ["_id", "ingestType"]:
            del collection[prop]

        item_record["collection"].append(collection)

    def parse(self, doc_list):
        print("IN: parse")
        print("Doc list:" + str(len(doc_list)))
        big_doc = "<docs>" + "".join(doc_list) + "</docs>"
        try:
            parsed_docs = xmltodict.parse(big_doc)
            return parsed_docs["docs"]["doc"]
        except Exception as e:
            fd, error_file_name = tempfile.mkstemp()
            os.write(fd, "<docs>" + "".join(doc_list) + "</docs>")
            print >> sys.stderr, e.message
            print >> sys.stderr, "Bad batch written to %s" % error_file_name
            sys.exit(1)

    def extract_xml_content(self, filepath):
        print("IN extract_xml_content: " + str(filepath))
        error = None

        # Read in batches of self.batch_size
        docs = []
        with open(filepath, "r") as f:
            while True:
                try:
                    line = f.readline()

                    if not line:
                        break
                    
                    line = re.sub(r"</result></response>$", "", line)
                    
                    if line.startswith("<doc>"):
                        docs.append(line)
                    if len(docs) == self.batch_size:
                        yield error, self.parse(docs)
                        docs = []
                except Exception, e:
                    error = "Error parsing content from file %s: %s" % \
                            (filepath, e)
                    print("ERROR")
                    print(str(docs))
                    yield error, None
                    break
        # Last yield
        if docs and error is None:
            yield error, self.parse(docs)

    def extract_records(self, file_path):
        print("IN: extract_records "  + str(file_path))
        def _normalize_collection_title(title):
            """Removes bad characters from collection titles"""
            norm = re.sub(r'[^\w\[\]]+', r'_', title)
            return norm.lower()

        errors = []
        records = []

        for error, content in self.extract_xml_content(file_path):
            if error is None:
                for record in content:
                    try:
                        record["_id"] = \
                            record["descriptiveNonRepeating"]["record_ID"]
                    except:
                        # Exclude records with no record_ID
                        continue

                    set_names = None
                    if "freetext" in record:
                        set_names = record["freetext"].get("setName")
                    if set_names is None:
                        # Placeholder collection
                        title = ""
                        hid = "placeholder"
                        self.create_collection_record(hid, title)
                        self.add_collection_to_item_record(hid, record)
                    else:
                        for set in iterify(set_names):
                            title = set["#text"]
                            hid = _normalize_collection_title(title)
                            self.create_collection_record(hid, title)
                            self.add_collection_to_item_record(hid, record)

                    records.append(record)
            else:
                errors.append(error)

            yield errors, records
            errors = []
            records = []
