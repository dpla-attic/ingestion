from dplaingestion.utilities import iterify
from dplaingestion.selector import getprop
from dplaingestion.mappers.qdc_mapper import QDCMapper


class MaineMapper(QDCMapper):

    def __init__(self, provider_data):
        super(MaineMapper, self).__init__(provider_data)

    # helper function to copy properties to specific destinations in
    # sourceResource.
    def copy_sr(self, source, dest):
        data = iterify(getprop(self.provider_data, source, True))
        if data:
            self.update_source_resource({dest: data})

    def map_creator(self):
        self.copy_sr("creator", "creator")

    def map_date(self):
        self.copy_sr("created", "date")

    def map_description(self):
        self.copy_sr("abstract", "description")

    def map_extent(self):
        self.copy_sr("extent", "extent")

    def map_language(self):
        self.copy_sr("language", "language")

    def map_spatial(self):
        self.copy_sr("spatial", "spatial")

    def map_publisher(self):
        self.copy_sr("publisher", "publisher")

    def map_subject(self):
        self.copy_sr("subject", "subject")

    def map_temporal(self):
        self.copy_sr("temporal", "temporal")

    def map_title(self):
        self.copy_sr("title", "title")

    def map_type(self):
        self.copy_sr("type", "type")

    # helper function to copy properties to specific destinations in
    # the root of the record.
    def copy_root(self, source, dest):
        data = iterify(getprop(self.provider_data, source, True))
        if data:
            self.mapped_data.update({dest: data})

    def map_data_provider(self):
        data = iterify(getprop(self.provider_data, "contributor"))
        if data:
            self.mapped_data.update({"dataProvider": data[0]})

    def map_is_shown_at(self, index=None):
        self.copy_root("handle", "isShownAt")

    def map_object(self):
        data = iterify(getprop(self.provider_data, "hasFormat", True))
        if data:
            self.mapped_data.update({"object": data[0]})

    def map_edm_rights(self):
        self.copy_root("rights", "rights")

    # non-applicable mappings

    def map_collection(self):
        pass

    def map_contributor(self):
        pass

    def map_format(self):
        pass

    def map_identifier(self):
        pass

    def map_relation(self):
        pass

    def map_rights(self):
        pass
