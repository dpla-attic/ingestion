from dplaingestion.mappers.marc_mapper import MARCMapper

class UIUCMARCMapper(MARCMapper):                                                       
    def __init__(self, provider_data):                                                   
        super(UIUCMARCMapper, self).__init__(provider_data)

        self.mapping_dict.update({
            lambda t: t == "852": [(self.map_data_provider, "a")]
        })

    def map_data_provider(self, _dict, tag, codes):
        values = self._get_values(_dict, codes)
        if values:
            data_provider = values[0] + ", University Library"
            provider = {
                "@id": "http://dp.la/api/contributor/uiuc",
                "name": values[0]
            }
            setprop(self.mapped_data, "dataProvider", data_provider)
            setprop(self.mapped_data, "provider", provider)
