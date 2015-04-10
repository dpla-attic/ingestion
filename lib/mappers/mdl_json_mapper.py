from akara import logger
from amara.lib.iri import is_absolute
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mapv3_json_mapper import MAPV3JSONMapper

class MDLJSONMapper(MAPV3JSONMapper):
    def __init__(self, provider_data):
        super(MDLJSONMapper, self).__init__(provider_data)
        self.root_key = "record/"

    # sourceResource mapping
    def map_collection(self):
        if exists(self.provider_data, "collection"):
            self.update_source_resource({"collection": self.provider_data.get("collection")})

    def map_identifier(self):
        if exists(self.provider_data, "identifier"):
            self.update_source_resource({"identifier": self.provider_data.get("identifier")})

    def update_date(self):
        dates = getprop(self.mapped_data, "sourceResource/date", True)
        out_dates = []
        if dates:
            for d in iterify(dates):
                if isinstance(d, dict):
                    if not d.get('displayDate'):
                        if d.get('begin'):
                            d['displayDate'] = d.get('begin')
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

    def update_rights(self):
        orig_rights = None
        if not getprop(self.mapped_data, "sourceResource/rights", True):
            orig_rights = getprop(self.provider_data, "record/rights", True)
        if orig_rights:
            self.update_source_resource({"rights": orig_rights}) 

    def update_mapped_fields(self):
        self.update_date()
        self.update_is_shown_at()
        self.update_rights()