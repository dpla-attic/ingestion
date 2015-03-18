from dplaingestion.fetchers.primo_fetcher import *

class GettyFetcher(PrimoFetcher):
    def __init__(self, profile, uri_base, config_file):
        super(GettyFetcher, self).__init__(profile, uri_base,
                                                 config_file)

    def get_collection_for_record(self, record):
        source_id = getprop(record, self.root_key + "control/sourceid")
        if  source_id == "GETTY_OCP":
            coll_title = getprop(record, self.root_key + "display/lds43")
        elif source_id == "GETTY_ROSETTA":
            coll_title = getprop(record, self.root_key + "display/lds34")
        else:
            coll_title = None

        if coll_title:
            collections = []
            for title in filter(None, iterify(coll_title)):
                if title not in self.collections:
                    data_provider = "Getty Research Institute"
                    self.add_to_collections(title, data_provider)
                collections.append(self.collections[title])
            if len(collections) == 1:
                return collections[0]
            else:
                return collections
        else:
            return None
