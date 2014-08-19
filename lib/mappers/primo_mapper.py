from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mapper import Mapper

class PrimoMapper(Mapper):
    def __init__(self, provider_data):
        super(PrimoMapper, self).__init__(provider_data)
        if exists(provider_data, "PrimoNMBib/record"):
            self.root_key = "PrimoNMBib/record/"
        else:
            self.root_key = ""
        self.links_key = "sear:LINKS/"

    def _map_source_resource_prop(self, prop, provider_prop):
        if exists(self.provider_data, provider_prop):
            p = self._striptags(getprop(self.provider_data, provider_prop))
            self.update_source_resource({prop: p})

    def map_creator(self):
        self._map_source_resource_prop("creator",
                                       self.root_key + "display/creator")

    def map_subject(self):
        self._map_source_resource_prop("subject",
                                       self.root_key + "display/subject")

    def map_object(self):
        prop = self.links_key + "sear:thumbnail"
        if exists(self.provider_data, prop):
            self.mapped_data.update({"object":
                                     getprop(self.provider_data, prop)})
