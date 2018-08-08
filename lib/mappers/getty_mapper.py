from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.primo_mapper import PrimoMapper
from akara import logger

class GettyMapper(PrimoMapper):
    def __init__(self, provider_data):
        super(GettyMapper, self).__init__(provider_data)
        self.is_shown_at_url = "http://primo.getty.edu/primo_library" + \
                               "/libweb/action/dlDisplay.do?vid=%s&" + \
                               "afterPDS=true&institution=01GRI&docId=%s"
        self.source_id = getprop(self.provider_data,
                                 self.root_key + "control/sourceid", True)

    def map_contributor(self):
        self._map_source_resource_prop("contributor",
                                       self.root_key + "display/contributor")

    def map_creator(self):
        props = (self.root_key + "display/creator",
                 self.root_key + "display/lds50")
        creators = []

        for prop in props:
            if exists(self.provider_data, prop):
                creators.append(getprop(self.provider_data, prop))

        if creators:
            self.update_source_resource({"creator": creators})

    def map_date(self):
        self._map_source_resource_prop("date",
                                       self.root_key + "display/creationdate")

    def map_description(self):
        description = []
        props = ("lds04", "lds28")
        for prop in props:
            values = getprop(self.provider_data,
                             self.root_key + "display/%s" % prop, True)
            if values:
                [description.append(v) for v in iterify(values) if v not in
                 description]

        if description:
            self.update_source_resource({"description": description})

    def map_extent(self):
        self._map_source_resource_prop("extent",
                                       self.root_key + "display/format")

    def map_format(self):
        self._map_source_resource_prop("format",
                                       self.root_key + "display/lds09")

    def map_identifier(self):
        self._map_source_resource_prop("identifier",
                                       self.root_key + "display/lds14")

    def map_language(self):
        self._map_source_resource_prop("language",
                                       self.root_key + "display/language")

    def map_publisher(self):
        self._map_source_resource_prop("publisher",
                                       self.root_key + "display/publisher")

    def map_relation(self):
        pass

    def map_rights(self):
        rights = []
        props = (self.root_key + "display/lds27",
                 self.root_key + "display/rights")
        for prop in props:
            values = getprop(self.provider_data, prop, True)
            if values:
                [rights.append(v) for v in iterify(values) if v not in
                 rights]

        if rights:
            self.update_source_resource({"rights": rights})

    def map_type(self):
        self._map_source_resource_prop("type",
                                       self.root_key + "display/lds26")

    def map_title(self):
        title = []
        props = ("title", "lds03")
        for prop in props:
            values = getprop(self.provider_data,
                             self.root_key + "display/%s" % prop, True)
            if values:
                [title.append(v) for v in iterify(values) if v not in title]

        if title:
            self.update_source_resource({"title": title})

    def map_is_shown_at(self):
        record_id = getprop(self.provider_data,
                            self.root_key + "control/recordid", True)
        if record_id and self.source_id:
            if self.source_id == "GETTY_ROSETTA":
                value = getprop(self.provider_data, self.root_key +
                                "display/lds29")
                if value:
                    self.mapped_data.update({"isShownAt": value})

            elif self.source_id == "GETTY_OCP":
                vid = "GRI-OCP"
                self.mapped_data.update({"isShownAt": self.is_shown_at_url %
                                                      (vid, record_id)})

    def map_data_provider(self):
        if self.source_id in ("GETTY_OCP", "GETTY_ROSETTA"):
            self.mapped_data.update({"dataProvider":
                                     "Getty Research Institute"})

    def map_spatial(self):
        prop = self.root_key + "display/coverage"

        values = getprop(self.provider_data, prop, True)
        if values:
            spatial = []
            [spatial.append(v) for v in iterify(values) if v not in spatial]

            logger.error("Spatial value : %s" % spatial)

            self.update_source_resource({"spatial": spatial})

    def map_subject(self):
        subjects = []
        props = (self.root_key + "display/lds49",
                 self.root_key + "display/subject")
        for prop in props:
            values = getprop(self.provider_data, prop, True)
            if values:
                [subjects.append(v) for v in iterify(values) if v not in
                 subjects]

        if subjects:
            self.update_source_resource({"subject": subjects})

    def map_object(self):
        prop = self.links_key + "sear:thumbnail"

        if exists(self.provider_data, prop):
            self.mapped_data.update({"object":
                                     getprop(self.provider_data, prop)})