from akara import logger
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.qdc_mapper import QDCMapper

class SCDLMapper(QDCMapper):
    def __init__(self, provider_data):
        super(SCDLMapper, self).__init__(provider_data)

    def map_object(self):
        self.mapped_data.update({"object": self.provider_data.get("hasFormat")})

    def map_data_provider(self):
        # By default, "prime" edm:dataProvider with the the provider's name
        data_provider = getprop(self.mapped_data, "provider/name")
        if exists(self.provider_data, "publisher"):
            publisher = getprop(self.provider_data, "publisher")
            # Sometimes SCDL records have more than 1 dc:publisher, so make
            # ones that have only 1 into a single-element list
            if not isinstance(publisher, list):
                publisher = list(publisher)
            # Georgetown County Library/Georgetown County Museum
            # records should probably use the first dc:publisher as
            # edm:dataProvider
            if ([p for p in publisher if "georgetown" in p.lower()]):
                data_provider = publisher[0]
            # Otherwise use the last dc:publisher. This will work with
            # cases even when publisher is a one-element list.
            else:
                data_provider = publisher[-1]
        self.mapped_data.update({"dataProvider": data_provider})

    def map_relation(self):
        relation = []
        if "source" in self.provider_data:
                relation.append(self.provider_data.get("source"))
        if relation:
            self.update_source_resource({"relation": relation})
