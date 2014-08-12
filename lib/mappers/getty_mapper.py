from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.primo_mapper import PrimoMapper

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

    def map_date(self):
        self._map_source_resource_prop("date",
                                       self.root_key + "display/creationdate")

    def map_description(self):
        description = []
        props = ("lds04", "lds28", "rights")
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
        relation = []
        props = (self.links_key + "sear:lln04",
                 self.root_key + "display/ispartof")
        for prop in props:
            values = getprop(self.provider_data, prop, True)
            if values:
                [relation.append(v) for v in iterify(values) if v not in
                 relation]

        if relation:
            self.update_source_resource({"relation": relation})

    def map_rights(self):
        self._map_source_resource_prop("rights",
                                       self.root_key + "display/lds27")

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
                vid = "GRI"
            elif self.source_id == "GETTY_OCP":
                vid = "GRI-OCP"
            else:
                vid = None

            if vid:
                self.mapped_data.update({"isShownAt": self.is_shown_at_url %
                                         (vid, record_id)})

    def map_data_provider(self):
        if self.source_id in ("GETTY_OCP", "GETTY_ROSETTA"):
            self.mapped_data.update({"dataProvider":
                                     "Getty Research Institute"})
