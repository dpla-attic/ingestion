from akara import logger
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.dublin_core_mapper import DublinCoreMapper
from dplaingestion.utilities import iterify
from dplaingestion.textnode import textnode


class RumseyMapper(DublinCoreMapper):
    def __init__(self, provider_data):
        super(RumseyMapper, self).__init__(provider_data)

    def map_is_referenced_by(self):
        url = []
        if exists(self.provider_data, "handle"):
            for s in iterify(getprop(self.provider_data, "handle")):
                i = textnode(s)
                if i.startswith("https://www.davidrumsey.com/luna/servlet/"):
                    i = i.replace("/detail/", "/iiif/m/") + "/manifest"
                    url.append(i)
        if url:
            self.mapped_data.update({"isReferencedBy": url[0]})

    def map_multiple_fields(self):
        self.map_is_referenced_by()
