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

    def map_date(self):
      if exists(self.provider_data, "created"):
        self.mapped_data.update({
          "date": getprop(self.provider_data, "created")
        })

    def map_language(self):
      self.source_resource_prop_to_prop("language")
    
    def map_publisher(self):
      self.source_resource_prop_to_prop("publisher")
    
    def map_data_provider(self):
      if exists(self.provider_data, "provenance"):
          self.mapped_data.update({
            "dataProvider": getprop(self.provider_data, "provenance")
          })

    def map_intermediate_provider(self):
      if exists(self.provider_data, "mediator"):
        self.mapped_data.update({
          "intermediateProvider": getprop(self.provider_data, "mediator")
        })

    def map_object(self):
      if exists(self.provider_data, "source"):
        self.mapped_data.update({
          "object": getprop(self.provider_data, "source")
        })

    def map_identifier(self):
      self.source_resource_prop_to_prop("identifier")
