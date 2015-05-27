from akara import logger
from amara.lib.iri import is_absolute
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mapper import Mapper

class MAPV3JSONMapper(Mapper):
    def __init__(self, provider_data):
        super(MAPV3JSONMapper, self).__init__(provider_data)
        self.root_key = ""

    # root mapping
    def root_prop_to_prop(self, prop):
        k = self.root_key + prop
        if exists(self.provider_data, k):
            self.mapped_data.update({prop: getprop(self.provider_data, k)})

    def map_data_provider(self):
        self.root_prop_to_prop("dataProvider")

    def map_has_view(self):
        self.root_prop_to_prop("hasView")

    def map_intermediate_provider(self):
        self.root_prop_to_prop("intermediateProvider")

    def map_is_shown_at(self):
        self.root_prop_to_prop("isShownAt")

    def map_object(self):
        self.root_prop_to_prop("object")

    def map_edm_rights(self):
        self.root_prop_to_prop("rights")

    # sourceResource mapping
    def source_resource_prop_to_prop(self, prop):
        k = self.root_key + "sourceResource/" + prop
        if exists(self.provider_data, k):
            self.update_source_resource({prop: getprop(self.provider_data, k)})

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

    def map_spatial(self):
        self.source_resource_prop_to_prop("spatial")

    def map_spec_type(self):
        self.source_resource_prop_to_prop("specType")

    def map_state_located_in(self):
        self.source_resource_prop_to_prop("stateLocatedIn")

    def map_subject(self):
        self.source_resource_prop_to_prop("subject")

    def map_temporal(self):
        self.source_resource_prop_to_prop("temporal")

    def map_title(self):
        self.source_resource_prop_to_prop("title")

    def map_type(self):
        self.source_resource_prop_to_prop("type")