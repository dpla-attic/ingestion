from dplaingestion.mappers.mapper import *

class QDCMapper(Mapper):
    def __init__(self, data):
        super(QDCMapper, self).__init__(data)

    # root mapping
    def map_is_shown_at(self, index=None):
        if exists(self.provider_data, "handle"):
            is_shown_at = None
            identifiers = [id for id in
                           iterify(self.provider_data["handle"]) if
                           is_absolute(id)]
            if index:
                try:
                    is_shown_at = identifiers[int(index)]
                except:
                    pass
            if not is_shown_at:
                is_shown_at = identifiers[0]

            if is_shown_at:
                self.mapped_data.update({"isShownAt": is_shown_at})

    def map_data_provider(self):
        if exists(self.provider_data, "publisher"):
            self.mapped_data.update({"dataProvider":
                                     getprop(self.provider_data, "publisher")})

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
        if exists(self.provider_data, "medium"):
            self.update_source_resource({"format": getprop(self.provider_data,
                                                           "medium")})

    def map_language(self):
        self.source_resource_prop_to_prop("language")

    def map_spatial(self):
        if exists(self.provider_data, "spatial"):
            self.update_source_resource({"spatial":
                                         iterify(getprop(self.provider_data,
                                                         "spatial"))})

    def map_relation(self):
        self.source_resource_prop_to_prop("relation")

    def map_rights(self):
        self.source_resource_prop_to_prop("rights")

    def map_subject(self):
        self.source_resource_prop_to_prop("subject")

    def map_temporal(self):
        self.source_resource_prop_to_prop("temporal")

    def map_title(self):
        self.source_resource_prop_to_prop("title")

    def map_type(self):
        self.source_resource_prop_to_prop("type")

    def map_identifier(self):
        pass

    def map_publisher(self):
        pass
