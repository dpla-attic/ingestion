from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.qdc_mapper import QDCMapper

class MDLMapper(QDCMapper):
    def __init__(self, provider_data):
        super(MDLMapper, self).__init__(provider_data)

    def map_is_shown_at(self):
        index = 1
        super(MDLMapper, self).map_is_shown_at(index)

    def map_date(self):
        if exists(self.provider_data, "created"):
            self.update_source_resource(
                {"date": getprop(self.provider_data, "created")}
                )

    def map_relation(self):
        if exists(self.provider_data, "isPartOf"):
            self.update_source_resource(
                {"relation": getprop(self.provider_data, "isPartOf")}
                )

    def map_publisher(self):
        if exists(self.provider_data, "source"):
            self.update_source_resource(
                {"publisher": getprop(self.provider_data, "source")}
                )
