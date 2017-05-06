from dplaingestion.utilities import iterify
from dplaingestion.selector import getprop
from dplaingestion.mappers.qdc_mapper import QDCMapper


class MarylandMapper(QDCMapper):

    def __init__(self, provider_data):
        super(MarylandMapper, self).__init__(provider_data)

    def map_collection(self):
        collection = getprop(self.provider_data, "collection/title", True)
        if collection:
            self.update_source_resource({"isPartOf": collection})

    def map_semicolon_delimited_field(self, source_prop, dest_prop):
        result_set = set()

        field_data = iterify(
            getprop(self.provider_data, source_prop, True)
        )

        for entry in field_data:
            for value in entry.split(";"):
                stripped = value.strip(";")
                if stripped:
                    result_set.add(stripped)

        if result_set:
            self.mapped_data.get("sourceResource") \
                .update(({dest_prop: list(result_set)}))

    def map_contributor(self):
        self.map_semicolon_delimited_field("contributor", "contributor")

    def map_creator(self):
        self.map_semicolon_delimited_field("creator", "creator")

    def map_format(self):
        self.source_resource_prop_to_prop("format")

    def map_language(self):
        self.map_semicolon_delimited_field("language", "language")

    def map_publisher(self):
        self.map_semicolon_delimited_field("publisher", "publisher")

    def map_rights(self):
        rights = iterify(getprop(self.provider_data, "accessrights", True))
        if rights:
            self.update_source_resource({"rights": rights})

    def map_subject(self):
        self.map_semicolon_delimited_field("subject", "subject")

    def map_temporal(self):
        self.map_semicolon_delimited_field("temporal", "temporal")

    def map_type(self):
        self.map_semicolon_delimited_field("type", "type")

    def map_data_provider(self):
        sources = iterify(getprop(self.provider_data, "source", True))
        cleaned = map(lambda x: x.strip(';'), sources)
        if cleaned:
            self.mapped_data.update({"dataProvider": cleaned[0]})

    def map_is_shown_at(self):
        identifiers = iterify(getprop(self.provider_data, "handle", True))
        if identifiers and len(identifiers) > 1:
            self.mapped_data.update({"isShownAt": identifiers[1]})

    def map_object(self):
        url = getprop(self.mapped_data, "isShownAt", True)
        if url:
            parts = url.strip("/").split("/")
            if parts:
                collection = parts[-3]
                id = parts[-1]
                preview = "http://webconfig.digitalmaryland.org/utils/" \
                          "getthumbnail/collection/%s/id/%s" % (collection, id)
                self.mapped_data.update({"object": preview})

    def map_provider(self, prop="provider"):
        self.mapped_data.update({"provider": "Digital Maryland"})

    def map_edm_rights(self):
        rights = iterify(getprop(self.provider_data, "rights", True))
        if rights:
            self.mapped_data.update({"rights": rights})
