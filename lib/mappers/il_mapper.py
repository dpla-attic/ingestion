from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.qdc_mapper import QDCMapper
from dplaingestion.textnode import textnode

class ILMapper(QDCMapper):
    def __init__(self, provider_data):
        super(ILMapper, self).__init__(provider_data)
    
    def map_alt_title(self):
        self.source_resource_prop_to_prop("alternative")

    def map_date(self):
        if exists(self.provider_data, "date"):
            self.update_source_resource({
                "date": getprop(self.provider_data, "date")
            })
        elif exists(self.provider_data, "created"):
            self.update_source_resource({
                "date": getprop(self.provider_data, "created")
            })

    def map_format_and_medium(self):
        format_medium = []

        if exists(self.provider_data, "format"):
            f = getprop(self.provider_data, "format")
            if isinstance(f, basestring):
                f = [f]
            format_medium.append(f)
        if exists(self.provider_data, "medium"):
            m = getprop(self.provider_data, "medium")
            if isinstance(m, basestring):
                m = [m]
            format_medium.append(m)

        # Remove NoneType elements and unnest format_medium
        format_medium = filter(None, [i for sublist in format_medium for i in sublist])

        if format_medium:
            self.update_source_resource({"format": format_medium})

    def map_is_shown_at(self, index=None):
        url = None
        if exists(self.provider_data, "isShownAt"):
            for s in iterify(getprop(self.provider_data, "isShownAt")):
                if "#text" in s:
                    url = s["#text"]
                    if url:
                        self.mapped_data.update({"isShownAt": url})

    def map_object(self):
        url = None
        if exists(self.provider_data, "preview"):
            for s in iterify(getprop(self.provider_data, "preview")):
                if "#text" in s:
                    url = s["#text"]
                    if url:
                        self.mapped_data.update({"object": url})
    
    def map_data_provider(self):
        dataProviders = []
        for d in iterify(getprop(self.provider_data, "provenance")):
            dataProviders.append(textnode(d))
        if dataProviders:
            self.mapped_data.update({"dataProvider": dataProviders[0]})

    def map_identifier(self):
        self.source_resource_prop_to_prop("identifier")

    def map_edm_rights(self):
        rights = []
        for right in iterify(getprop(self.provider_data, "edm:rights", True)):
            rights.append(right)
        if rights:
            self.mapped_data.update("rights", rights[0])

    def map_multiple_fields(self):
        self.map_format_and_medium()