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
        self.is_shown_at_url = "http://utah-primoprod.hosted." + \
                               "exlibrisgroup.com/" + \
                               "primo_library/libweb/action/" + \
                               "dlDisplay.do?vid=MWDL&afterPDS=true&docId="

    def _map_source_resource_prop(self, prop, provider_prop):
        if exists(self.provider_data, provider_prop):
            self.update_source_resource({prop:
                                         getprop(self.provider_data,
                                                 provider_prop)})

    def map_creator(self):
        self._map_source_resource_prop("creator",
                                       self.root_key + "display/creator")

    def map_date(self):
        self._map_source_resource_prop("date",
                                       self.root_key + "search/creationdate")

    def map_description(self):
        self._map_source_resource_prop("description",
                                       self.root_key + "search/description")

    def map_extent(self):
        self._map_source_resource_prop("extent",
                                       self.root_key + "display/lds05")

    def map_language(self):
        self._map_source_resource_prop("language",
                                       self.root_key + "facets/language")

    def map_relation(self):
        self._map_source_resource_prop("relation",
                                       self.root_key + "display/relation")

    def map_rights(self):
        self._map_source_resource_prop("rights",
                                       self.root_key + "display/rights")

    def map_subject(self):
        self._map_source_resource_prop("subject",
                                       self.root_key + "display/subject")

    def map_temporal(self):
        self._map_source_resource_prop("temporal",
                                       self.root_key + "display/lds09")

    def map_type(self):
        self._map_source_resource_prop("type",
                                       self.root_key + "display/lds17")

    def map_state_located_in(self):
        self._map_source_resource_prop("stateLocatedIn",
                                       self.root_key + "search/lsr03")

    def map_identifier(self):
        self._map_source_resource_prop("identifier",
                                       self.links_key + "sear:linktorsrc")

    def map_spatial(self):
        prop = self.root_key + "display/lds08"

        values = getprop(self.provider_data, prop, True)
        if values:
            spatial = []
            [spatial.append(v) for v in iterify(values) if v not in spatial]

            self.update_source_resource({"spatial": spatial})

    def map_is_part_of(self):
        prop = self.root_key + "display/lds04"

        values = getprop(self.provider_data, prop, True)
        if values:
            ipo = []
            [ipo.append(v) for v in iterify(values) if v not in ipo]

            self.update_source_resource({"isPartOf": "; ".join(ipo)})
 
    def map_title(self):
        props = (self.root_key + "display/title",
                 self.root_key + "display/lds10")

        title = []
        for prop in props:
            values = getprop(self.provider_data, prop, True)
            if values:
                [title.append(v) for v in iterify(values) if v not in title]

        if title:
            self.update_source_resource({"title": "; ".join(title)})

    def map_is_shown_at(self):
        record_id = getprop(self.provider_data,
                            self.root_key + "control/recordid", True)
        if record_id:
            self.mapped_data.update({"isShownAt": self.is_shown_at_url +
                                                  record_id})

    def map_object(self):
        prop = self.links_key + "sear:thumbnail"
        if exists(self.provider_data, prop):
            self.mapped_data.update({"object":
                                     getprop(self.provider_data, prop)})

    def map_data_provider(self):
        prop = self.root_key + "display/lds03"

        values = getprop(self.provider_data, prop, True)
        if values:
            dp = []
            [dp.append(v) for v in iterify(values) if v not in dp]

            self.mapped_data.update({"dataProvider": "; ".join(dp)})

    def map_collection(self):
        # Override super class map_collection method
        pass
