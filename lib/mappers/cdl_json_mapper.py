from dplaingestion.utilities import iterify
from dplaingestion.selector import getprop
from dplaingestion.mappers.mapv3_json_mapper import MAPV3JSONMapper
from akara import logger


class CDLJSONMapper(MAPV3JSONMapper):
    def __init__(self, provider_data):
        super(CDLJSONMapper, self).__init__(provider_data)

    def map_collection(self):
        values = iterify(getprop(self.provider_data, "collection_name", True))
        collections = []
        for value in values:
            collection = {"title": value, "@id": "", "id": "", "description": ""}
            collections.append(collection)

        if collections:
            self.mapped_data["sourceResource"]["collection"] = collections

    def map_source_resource(self):
        super(CDLJSONMapper, self).map_source_resource()
        maps = {
            "alternative_title_ss": "alternative",
            "contributor_ss": "contributor",
            "creator_ss": "creator",
            "date_ss": "date",
            "description": "description",
            "extent_ss": "extent",
            "format_ss": "format",
            "genre_ss": "hasType",
            "identifier_ss": "identifier",
            "language_ss": "language",
            "coverage_ss": "spatial",
            "publisher_ss": "publisher",
            "relation_ss": "relation",
            "rights_ss": "rights",
            "rights_note_ss": "rights",
            "rights_date_ss": "rights",
            "rightsholder_ss": "rights",
            "subject_ss": "subject",
            "temporal_ss": "temporal",
            "title_ss": "title",
            "type_ss": "type"
        }

        for (source, dest) in maps.iteritems():
            values = \
                iterify(getprop(self.provider_data, source, True))
            if values:
                existing_values = \
                    getprop(self.mapped_data["sourceResource"], dest, True)
                if existing_values:
                    values = list(set(values + existing_values))
                self.update_source_resource({dest: values})

    def map_edm_rights(self):
        pass

    def map_is_shown_at(self):
        url_item = iterify(getprop(self.provider_data, "url_item", True))
        if url_item:
            self.mapped_data.update({"isShownAt": url_item[0]})

    def map_data_provider(self):
        data_provider = ""
        campus_name = iterify(getprop(self.provider_data, "campus_name", True))
        repository_name = \
            iterify(getprop(self.provider_data, "repository_name", True))
        if campus_name and repository_name:
            data_provider = "%s, %s" % (campus_name[0], repository_name[0])
        elif repository_name:
            data_provider = repository_name[0]
        if data_provider:
            self.mapped_data.update({"dataProvider": data_provider})

    def map_object(self):
        reference_image_md5 = \
            getprop(self.provider_data, "reference_image_md5", True)
        if reference_image_md5:
            url = "https://thumbnails.calisphere.org/clip/150x150/%s" \
                  % reference_image_md5
            self.mapped_data.update({"object": url})
