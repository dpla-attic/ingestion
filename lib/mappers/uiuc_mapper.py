from dplaingestion.mappers.qdc_mapper import *

class UIUCMapper(QDCMapper):
    def __init__(self, data):
        super(UIUCMapper, self).__init__(data)

    def map_is_shown_at(self):
        index = -1
        super(UIUCMapper, self).map_is_shown_at(index)

    def map_format(self):
        if exists(self.provider_data, "medium"):
            self.update_source_resource({"format": getprop(self.provider_data,
                                                           "medium")})
        elif exists(self.provider_data, "format"):
            self.update_source_resource({"format": getprop(self.provider_data,
                                                           "format")})

    def map_relation(self):
        relation = []
        for field in ("relation", "source", "isPartOf"):
            if field in self.provider_data:
                relation.append(self.provider_data.get(field))
        if relation:
            self.update_source_resource({"relation": relation})
            
    def map_date(self):
        date = []
        for field in ("date", "created"):
            if field in self.provider_data:
                # Some values can be lists so we iterify and extend
                date.extend(iterify(self.provider_data.get(field)))
        if date:
            self.update_source_resource({"date": date})
