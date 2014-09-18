from akara import logger
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.qdc_mapper import QDCMapper

class SCDLMapper(QDCMapper):
    def __init__(self, provider_data):
        super(SCDLMapper, self).__init__(provider_data)

    def map_object(self):
        self.mapped_data.update({"object": self.provider_data.get("hasFormat")})

    def map_data_provider(self):
        if exists(self.provider_data, "publisher"):
            publisher = getprop(self.provider_data, "publisher")
            if isinstance(publisher, list):
                if ([p for p in publisher if "georgetown" in p.lower()]) \
                    or (len(publisher) == 1):
                    data_provider = publisher[0]
                else:
                    data_provider = publisher[-1]
            else:
                data_provider = publisher
        else:
            data_provider = getprop(self.mapped_data, "provider/name")
        self.mapped_data.update({"dataProvider": data_provider})

    def map_relation(self):
        relation = []
        if "source" in self.provider_data:
                relation.append(self.provider_data.get("source"))
        if relation:
            self.update_source_resource({"relation": relation})
