from urlparse import urlparse
from akara import logger
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.qdc_mapper import QDCMapper


class INMapper(QDCMapper):
    def __init__(self, provider_data):
        super(INMapper, self).__init__(provider_data)
    
    def map_alt_title(self):
        self.source_resource_prop_to_prop("alternative")

    def map_contributor(self):
        self.source_resource_prop_to_prop("contributor")

    def map_creator(self): 
        self.source_resource_prop_to_prop("creator")

    def map_data_provider(self):
        if exists(self.provider_data, "provenance"):
            self.mapped_data.update({
                "dataProvider": getprop(self.provider_data, "provenance")
            })

    def map_date(self):
        if exists(self.provider_data, "created"):
            self.update_source_resource({
                "date": getprop(self.provider_data, "created")
            })

    def map_edm_rights(self):
        prop = "rights"
        if exists(self.provider_data, prop):
            rights_uri, rights = "", self.provider_data.get(prop)
            try:
                # IN is giving us some rights as ["rightsstatement", None],
                # this is pretty kludgey but should suffice for now.
                if isinstance(list, rights):
                    # Should return a single string (may not be a URL)
                    rights = filter(None, rights)[0]

                if rights.lower().startswith("http"):
                    rights_uri = urlparse(rights).geturl()
            except Exception as e:
                logger.error("Unable to parse rights URI: %s\n%s" % (rights, e))

            if rights_uri:
                self.mapped_data.update({
                    "rights": rights_uri
                })

    def map_identifier(self):
        self.source_resource_prop_to_prop("identifier")

    def map_intermediate_provider(self):
        if exists(self.provider_data, "mediator"):
            self.mapped_data.update({
                "intermediateProvider": getprop(self.provider_data, "mediator")
            })

    def map_language(self):
        self.source_resource_prop_to_prop("language")

    def map_object(self):
        if exists(self.provider_data, "source"):
            self.mapped_data.update({
                "object": getprop(self.provider_data, "source")
            })

    def map_publisher(self):
        self.source_resource_prop_to_prop("publisher")

    def map_rights(self):
        if exists(self.provider_data, "accessRights"):
            self.update_source_resource({
                "rights": getprop(self.provider_data, "accessRights")
            })
