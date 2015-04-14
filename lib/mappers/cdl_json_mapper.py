from akara import logger
from amara.lib.iri import is_absolute
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop, delprop
from dplaingestion.mappers.mapv3_json_mapper import MAPV3JSONMapper

class CDLJSONMapper(MAPV3JSONMapper):
    def __init__(self, provider_data):
        super(CDLJSONMapper, self).__init__(provider_data)
        self.root_key = "doc/"

    # sourceResource mapping
    def map_collection(self):
        if exists(self.provider_data, "collection"):
            self.update_source_resource({"collection": self.provider_data.get("collection")})

    def update_subject(self):
        subjects = getprop(self.mapped_data, "sourceResource/subject", True)
        if subjects:
            subjects = iterify(subjects)
            for subject in subjects[:]:
                if isinstance(subject, basestring):
                    pass
                elif isinstance(subject, dict):
                    s = subject.get("name")
                    subjects.remove(subject)
                    if s:
                        subjects.append(s)
                else:
                    subjects.remove(subject)
            delprop(self.mapped_data, "sourceResource/subject", True)
        if subjects:
            self.update_source_resource({"subject": subjects})

    def update_data_provider(self):
        new_data_provider = getprop(self.mapped_data, "dataProvider", True)
        # if unset or dict or list
        if not isinstance(new_data_provider, basestring): 
            f = getprop(self.provider_data, "doc/originalRecord/facet-institution")
            if isinstance(f, dict):
                new_data_provider = f.pop("text", None)
            elif isinstance(f, list) and len(f) > 0:
                new_data_provider = f[0].pop("text", None)
            if not isinstance(new_data_provider, basestring):
                new_data_provider = None
        if new_data_provider:
            new_data_provider = new_data_provider.replace("::", ", ")
            self.mapped_data.update({"dataProvider": new_data_provider})
        else:
            delprop(self.mapped_data, "dataProvider", True)

    def update_language(self):
        out_languages = []
        for language in iterify(getprop(self.mapped_data, "sourceResource/language", True)):
            if isinstance(language, dict):
                out_languages.append(language)
            elif isinstance(language, basestring):
                out_languages.append({"name": language})
        if out_languages:
            self.update_source_resource({"language": out_languages})
        else:
            delprop(self.mapped_data, "language", True)

    def update_spatial(self):
        out_spatial = []
        for spatial in iterify(getprop(self.mapped_data, "sourceResource/spatial", True)):
            if isinstance(spatial, dict):
                spatial_text = spatial.pop("text", None)
                if spatial_text:
                    spatial["name"] = spatial_text
                    spatial.pop("attrib", None)
            if spatial:
                out_spatial.append(spatial)
        if out_spatial:
            self.update_source_resource({"spatial": out_spatial})

    def update_mapped_fields(self):
        self.update_subject()
        self.update_data_provider()
        self.update_language()
        self.update_spatial()
