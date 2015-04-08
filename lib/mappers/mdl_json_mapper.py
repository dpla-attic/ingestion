from akara import logger
from amara.lib.iri import is_absolute
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mapper import Mapper

class MDLJSONMapper(Mapper):
    def __init__(self, provider_data):
        super(MDLJSONMapper, self).__init__(provider_data)
        self.root_key = "record/"

    # root mapping
    def root_prop_to_prop(self, prop):
        k = self.root_key + prop
        if exists(self.provider_data, k):
            self.mapped_data.update({prop: getprop(self.provider_data, k)})

    # def map_provider(self):
    #     self.mapped_data.update({"provider": self.provider_data.get("provider")})

    def map_data_provider(self):
        self.root_prop_to_prop("dataProvider")

    def map_is_shown_at(self):
        self.root_prop_to_prop("isShownAt")

    def map_has_view(self):
        self.root_prop_to_prop("hasView")

    def map_object(self):
        self.root_prop_to_prop("object")


    # sourceResource mapping
    def source_resource_prop_to_prop(self, prop):
        k = self.root_key + "sourceResource/" + prop
        if exists(self.provider_data, k):
            self.update_source_resource({prop: getprop(self.provider_data, k)})
            
    def map_collection(self):
        if exists(self.provider_data, "collection"):
            self.update_source_resource({"collection": self.provider_data.get("collection")})

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
        if exists(self.provider_data, "identifier"):
            self.update_source_resource({"identifier": self.provider_data.get("identifier")})

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

    def map_subject(self):
        self.source_resource_prop_to_prop("subject")

    def temporal(self):
        self.source_resource_prop_to_prop("temporal`")

    def map_title(self):
        self.source_resource_prop_to_prop("title")

    def map_type(self):
        self.source_resource_prop_to_prop("type")

    def update_date(self):
        dates = getprop(self.mapped_data, "sourceResource/date", True)
        out_dates = []
        if dates:
            for d in iterify(dates):
                if isinstance(d, dict):
                    if not d.get('displayDate'):
                        if d.get('begin'):
                            d['displayDate'] = begin
                            if d.get('end') and d.get('begin') != d.get('end'):
                                d['displayDate'] += '-%s' % d.get('end')
                        else:
                            d = None
                if d:
                    out_dates.append(d)
            self.update_source_resource({"date": out_dates})

    def update_is_shown_at(self):
        is_shown_at = getprop(self.mapped_data, "isShownAt", True)
        if isinstance(is_shown_at, basestring):
            if 'reflections.mndigital.org/cdm/ref/collection/' in is_shown_at:
                url_parts = is_shown_at.split('cdm/ref/collection/')
                url_parts[1] = url_parts[1].replace('/id/', ',')
                is_shown_at = 'u?/'.join(url_parts)
            elif 'api.artsmia.org' in is_shown_at:
                identifier = is_shown_at.split('/')[-1]
                is_shown_at = "https://collections.artsmia.org/index.php?" \
                              "page=detail&id=%s" % identifier
        self.mapped_data.update({'isShownAt': is_shown_at})

    def update_mapped_fields(self):
        self.update_date()
        self.update_is_shown_at()
