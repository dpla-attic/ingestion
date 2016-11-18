from akara import logger
from amara.lib.iri import is_absolute
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.dublin_core_mapper import DublinCoreMapper
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists


class CTMapper(DublinCoreMapper):
    def __init__(self, provider_data):
        super(CTMapper, self).__init__(provider_data)

    # sourceResource mapping
    # uses source_resource_prop_to_dest to grab stuff in rdf:Description
    def source_resource_prop_to_prop(self, prop):
        self.source_resource_prop_to_dest(prop, prop)

    #specialization of source_resource_prop_to_prop() that allows for specifying destination, origin in rdf:Description
    def source_resource_prop_to_dest(self, prop, dest):
        path = "rdf:Description/dc:" + prop
        if exists(self.provider_data, path):
            self.update_source_resource({
                dest: getprop(self.provider_data, path)
            })

    def map_collection(self):
        #have to parse the collection set from the id URI
        path = "originalRecord/id"
        if exists(self.provider_data, path):
            id = getprop(self.provider_data, path)
            segments = id.split(":")
            if len(segments) > 2:
                self.update_source_resource({"isPartOf": segments[2]})

    def map_contributor(self):
        #any dc:contributor except the last
        try:
            path = "rdf:Description/dc:contributor"

            if exists(self.provider_data, path):
                contributors = iterify(getprop(self.provider_data, path))
                for index, contributor in enumerate(contributors):
                    if index != len(contributors) - 1:
                        self.update_source_resource({"contributor": contributor})

        except Exception as e:
            logger.error("Error mapping contributor: " + str(e))


    def map_creator(self):
        self.source_resource_prop_to_prop("creator")

    def map_date(self):
        self.source_resource_prop_to_prop("date")

    def map_description(self):
        self.source_resource_prop_to_prop("description")

    def map_extent(self):
        #none
        pass

    def map_format(self):
        self.source_resource_prop_to_prop("format")

    def map_identifier(self):
        #none
        pass

    def map_language(self):
        self.source_resource_prop_to_prop("language")

    def map_spatial(self):
        self.source_resource_prop_to_dest("coverage", "spatial")

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

    def map_data_provider(self, prop="source"):
        #last dc:contributor
        path = "rdf:Description/dc:contributor"
        if exists(self.provider_data, path):
            contributors = iterify(getprop(self.provider_data, path))
            data_provider = contributors[-1]
            self.mapped_data.update({"dataProvider": data_provider})

    def map_is_shown_at(self):
        identifier = getprop(self.provider_data, "rdf:Description/dc:identifier")
        if identifier and is_absolute(identifier):
            self.mapped_data.update({"isShownAt": identifier})

    def map_has_view(self):
        source = getprop(self.provider_data, "rdf:Description/dc:source")
        if source and is_absolute(source):
            self.mapped_data.update({"preview": source})
