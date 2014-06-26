from dplaingestion.mappers.qdc_mapper import QDCMapper

class SCDLMapper(QDCMapper):
    def __init__(self, provider_data):
        super(SCDLMapper, self).__init__(provider_data)
        self.provider_name = self.provider_data["_id"].split("--")[0]

    def map_is_shown_at(self):
        provider_to_index = {
            "clemson": 1,
            "scdl-usc": 1,
            "scdl-charleston": 0
        }

        index = provider_to_index.get(self.provider_name)
        if index is None:
            logger.error("Provider %s does not exist in provider_to_index" %
                         self.provider_name)
        else:
            super(SCDLMapper, self).map_is_shown_at(index)

    def map_relation(self):
        if self.provider_name == "scdl-usc":
            relation = []
            for field in ("relation", "source"):
                if field in self.provider_data:
                    relation.append(self.provider_data.get(field))
            if relation:
                self.update_source_resource({"relation": relation})
        else:
            super(SCDLMapper, self).map_relation()
            
    def map_date(self):
        if self.provider_name == "scdl-usc":
            if exists(self.provider_data, "created"):
                self.update_source_resource({"date":
                                             getprop(self.provider_data,
                                                     "created")})
        else:
            super(SCDLMapper, self).map_date()
