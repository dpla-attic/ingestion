from akara import logger
from amara.lib.iri import is_absolute
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.dublin_core_mapper import DublinCoreMapper
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists


class CTMapper(DublinCoreMapper):
    def __init__(self, provider_data):
        super(CTMapper, self).__init__(provider_data)

    def map_is_shown_at(self):
        h = getprop(self.provider_data, "rdf:Description/dc:identifier")
        if h and is_absolute(h):
            self.mapped_data.update({"isShownAt": h})

    def map_creator(self):
        logger.error("Creator\n%s" % getprop(self.provider_data, "rdf:Description/dc:creator"))

        if exists(self.provider_data, "rdf:Description/dc:creator"):
            self.update_source_resource({
                "creator": getprop(self.provider_data, "rdf:Description/dc:creator")
            })

    # sourceResource mapping
    def source_resource_prop_to_prop(self, prop):
        pass

    def map_collection(self):
        pass

    def map_contributor(self):
        pass

    def map_date(self):
        pass

    def map_description(self):
        pass

    def map_extent(self):
        pass

    def map_format(self):
        pass

    def map_identifier(self):
        pass

    def map_language(self):
        pass

    def map_publisher(self):
        pass

    def map_relation(self):
        pass

    def map_rights(self):
        pass

    def map_subject(self):
        pass

    def map_title(self):
        pass

    def map_type(self):
        pass

    def map_spatial(self):
        pass