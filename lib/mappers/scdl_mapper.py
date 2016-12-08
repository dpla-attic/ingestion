from akara import logger
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.qdc_mapper import QDCMapper

class SCDLMapper(QDCMapper):
    def __init__(self, provider_data):
        super(SCDLMapper, self).__init__(provider_data)

    def map_object(self):
        self.mapped_data.update({"object": self.provider_data.get("hasFormat")})

    def map_data_provider(self):
        data_provider = None
        if exists(self.provider_data, "publisher"):
            data_provider = getprop(self.provider_data, "publisher")
        if data_provider:
            if isinstance(data_provider, list):
                data_provider = data_provider[0]
            self.mapped_data.update({"dataProvider": data_provider})

    def map_collection(self):
        if exists(self.provider_data, "isPartOf"):
            self.update_source_resource({"collection":
                                         getprop(self.provider_data, 
                                                 "isPartOf")})

    def map_relation(self):
        relation = []
        if "source" in self.provider_data:
                relation.append(self.provider_data.get("source"))
        if relation:
            self.update_source_resource({"relation": relation})

    def map_rights_and_accessrights(self):
        rights_accessrights = []

        if exists(self.provider_data, "rights"):
            r = getprop(self.provider_data, "rights")
            if isinstance(r, basestring):
                r = [r]
            rights_accessrights.append(r)

        if exists(self.provider_data, "accessRights"):
            a = getprop(self.provider_data, "accessRights")
            if isinstance(a, basestring):
                a = [a]
            rights_accessrights.append(a)

        # Remove NoneType elements and unnest rights_accessrights
        rights_accessrights = filter(None, [i for sublist in rights_accessrights for i in sublist])

        if rights_accessrights:
            self.update_source_resource({"rights": rights_accessrights})

    def map_multiple_fields(self):
        self.map_rights_and_accessrights()
