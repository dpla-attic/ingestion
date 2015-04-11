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

    def update_data_provider(self):
        new_data_provider = None
        if isinstance(getprop(self.mapped_data, "dataProvider", True), dict): 
            f = getprop(self.provider_data, "doc/originalRecord/facet-institution")
            if (isinstance(f, list)):
                new_data_provider = f[0]
        if new_data_provider:
            self.mapped_data.update({"dataProvider": new_data_provider})

    def update_mapped_fields(self):
        self.update_data_provider()