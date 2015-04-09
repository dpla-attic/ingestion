from akara import logger
from amara.lib.iri import is_absolute
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mapv3_json_mapper import MAPV3JSONMapper

class CDLJSONMapper(MAPV3JSONMapper):
    def __init__(self, provider_data):
        super(CDLJSONMapper, self).__init__(provider_data)
        self.root_key = "doc/"

    # sourceResource mapping
    def map_collection(self):
        if exists(self.provider_data, "collection"):
            self.update_source_resource({"collection": self.provider_data.get("collection")})