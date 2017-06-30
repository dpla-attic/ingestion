from dplaingestion.utilities import iterify
from dplaingestion.fetchers.primo_fetcher import PrimoFetcher, getprop


class MWDLFetcher(PrimoFetcher):
    def __init__(self, profile, uri_base, config_file):
        super(MWDLFetcher, self).__init__(profile, uri_base,
                                                 config_file)

    def get_collection_for_record(self, record):
        coll_title = getprop(record, self.root_key + "search/lsr13")

        if coll_title:
            collections = []
            for title in filter(None, iterify(coll_title)):
                if title not in self.collections:
                    data_provider = getprop(record,
                                            self.root_key + "display/lds03")
                    self.add_to_collections(title, data_provider)
                collections.append(self.collections[coll_title])
            if len(collections) == 1:
                return collections[0]
            else:
                return collections
        else:
            return None
