from dplaingestion.mappers.mapper import *

class MODSMapper(Mapper):
    def __init__(self, data, key_prefix="mods:"):
        super(MODSMapper, self).__init__(data, key_prefix)

    # Common methods
    def map_language(self):
        prop = "language/languageTerm"

        if exists(self.provider_data, prop):
            language = []
            for s in iterify(getprop(self.provider_data, prop)):
                if "#text" in s and s["#text"] not in language:
                    language.append(s["#text"])

            if language:
                self.update_source_resource({"language": language})