from dplaingestion.utilities import iterify
from dplaingestion.fetchers.fetcher import getprop
from dplaingestion.fetchers.file_fetcher import FileFetcher


class NARAFetcher(FileFetcher):
    def __init__(self, profile, uri_base, config_file):
        self.file_filter = "Item_*.xml"
        super(NARAFetcher, self).__init__(profile, uri_base, config_file)

    def add_collection_to_item_record(self, hid, item_record):
        item_record["collection"] = dict(self.collections[hid])
        for prop in ["_id", "ingestType"]:
            del item_record["collection"][prop]

    def extract_records(self, file_path):
        errors = []
        records = []

        error, content = self.extract_xml_content(file_path)
        if error is None:
            record = content.get("archival-description")

            try:
                record["_id"] = record["arc-id"]

                hier_items = getprop(record, "hierarchy/hierarchy-item")
                # Placeholder collection id and title
                coll_id = "placeholder"
                coll_title = ""
                # Check if record has a collection
                for hitem in iterify(hier_items):
                    if hitem["hierarchy-item-lod"].lower() in ("record group",
                                                               "collection"):
                        coll_id = hitem["hierarchy-item-id"]
                        coll_title = hitem["hierarchy-item-title"]
                        break
                self.create_collection_record(coll_id, coll_title)
                self.add_collection_to_item_record(coll_id, record)
                records.append(record)
            except:
                errors.append("Could not find 'arc_id' in file %s" % file_path)
        else:
            errors.append(error)

        yield errors, records
