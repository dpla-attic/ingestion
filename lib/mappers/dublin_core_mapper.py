from akara import logger
from amara.lib.iri import is_absolute
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists
from dplaingestion.mappers.mapper import Mapper

class DublinCoreMapper(Mapper):
    def __init__(self, provider_data):
        super(DublinCoreMapper, self).__init__(provider_data)

    # root mapping
    def map_is_shown_at(self):
        for h in iterify(self.provider_data.get("handle")):
            if is_absolute(h):
                self.mapped_data.update({"isShownAt": h})
                break

    # sourceResource mapping
    def source_resource_prop_to_prop(self, prop):
        if exists(self.provider_data, prop):
            self.update_source_resource({prop: self.provider_data.get(prop)})
            
    def map_collection(self):
        self.source_resource_prop_to_prop("collection")

    def map_contributor(self):
        self.source_resource_prop_to_prop("contributor")

    def map_creator(self):
        self.source_resource_prop_to_prop("creator")

    def map_date(self):
        self.source_resource_prop_to_prop("date")

    def map_description(self):
        self.source_resource_prop_to_prop("description")

    def map_extent(self):
        self.source_resource_prop_to_prop("extent")

    def map_format(self):
        self.source_resource_prop_to_prop("format")

    def map_identifier(self):
        self.source_resource_prop_to_prop("identifier")

    def map_language(self):
        self.source_resource_prop_to_prop("language")

    def map_publisher(self):
        self.source_resource_prop_to_prop("publisher")

    def map_relation(self):
        self.source_resource_prop_to_prop("relation")

    def map_rights(self):
        self.source_resource_prop_to_prop("rights")

    def map_subject(self):
        self.source_resource_prop_to_prop("subject")

    def map_title(self):
        self.source_resource_prop_to_prop("title")

    def map_type(self):
        self.source_resource_prop_to_prop("type")

    def map_spatial(self):
        prop= "coverage"
        if exists(self.provider_data, prop):
            self.update_source_resource({"spatial":
                                         self.provider_data.get(prop)})
