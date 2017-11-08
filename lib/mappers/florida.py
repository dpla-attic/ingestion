from dplaingestion.utilities import iterify
from dplaingestion.selector import getprop
from dplaingestion.mappers.mapper import Mapper
from dplaingestion.selector import exists
from akara import logger


class FloridaMapper(Mapper):

    def __init__(self, provider_data):
        super(FloridaMapper, self).__init__(provider_data)

    # There is a horrible problem in this codebase where errors are non-fatal
    # for records, and the original record gets subbed in instead. This hasn't
    # been a problem for other providers, since the way enrich.py decides a
    # record is valid is if there are _id and sourceResource fields, and
    # records in other schema don't have those fields. However, Florida is
    # giving us DPLA MAP records that do, so their original record is subbed
    # in as the mapped record if this mapping throws an exception.
    #
    # SO, overriding map here ensures that exceptions thrown from here don't
    # bubble up, and whatever is in self.mapped_data is returned.
    def map(self):
        try:
            super(FloridaMapper, self).map()
        except:
            logger.error("FloridaMapper threw an exception.", exc_info=1)

    def source_resource_prop_to_prop(self, prop):
        data = getprop(self.provider_data, "sourceResource/" + prop, True)
        if data:
            self.update_source_resource({prop: data})

    def root_prop_to_prop(self, prop):
        data = getprop(self.provider_data, prop, True)
        if data:
            self.mapped_data.update({prop: data})

    @staticmethod
    def get_names(items):
        results = []
        for item in items:
            if isinstance(item, dict) and exists(item, "name"):
                results.append(item["name"])
            elif isinstance(item, basestring):
                results.append(item)
        return results

    def map_multi_names(self, source_prop, dest_prop):
        parents = iterify(getprop(self.provider_data, source_prop, True))
        names = FloridaMapper.get_names(parents)
        if names:
            self.update_source_resource({dest_prop: names})

    def map_alternative_title(self):
        self.source_resource_prop_to_prop("alternative")

    def map_collection(self):
        collections = iterify(getprop(self.provider_data, "sourceResource/collection", True))
        if collections:
            collection_dicts = []
            for collection in collections:
                name = ""
                if type(collection) == dict and "name" in collection:
                    name = collection["name"]
                elif isinstance(collection, basestring):
                    name = collection
                if name:
                    logger.error("COLLECTION NAME: " + name)
                    collection_dicts.append({"name": name})
            self.update_source_resource({"collection": collection_dicts})

    def map_contributor(self):
        self.map_multi_names("sourceResource/contributor", "contributor")

    def map_creator(self):
        self.map_multi_names("sourceResource/creator", "creator")

    def map_date(self):
        date = getprop(self.provider_data, "sourceResource/date", True)
        if date:
            display_date = getprop(date, "displayDate", True)
            if display_date:
                self.update_source_resource({"date": display_date})


    def map_description(self):
        self.source_resource_prop_to_prop("description")

    def map_data_provider(self):
        self.root_prop_to_prop("dataProvider")

    def map_has_view(self):
        pass

    def map_intermediate_provider(self):
        pass

    def map_preview(self):
        data = iterify(getprop(self.provider_data, "preview", True))
        if data:
            self.mapped_data.update({"object": data[0]})

    def map_is_shown_at(self):
        self.root_prop_to_prop("isShownAt")

    def map_object(self):
        pass

    def map_edm_rights(self):
        edm_rights = \
            iterify(getprop(self.provider_data, "sourceResource/rights", True))
        if edm_rights and exists(edm_rights[0], "@id"):
            self.mapped_data.update({"rights": edm_rights[0]["@id"]})

    def map_extent(self):
        self.source_resource_prop_to_prop("extent")

    def map_format(self):
        both = []
        genres = FloridaMapper.get_names(iterify(
            getprop(self.provider_data, "sourceResource/genre", True)))

        if genres:
            both += genres

        formats = iterify(
            getprop(self.provider_data, "sourceResource/format", True))

        if formats:
            both += formats

        if both:
            self.update_source_resource({"format": both})

    def map_identifier(self):
        self.source_resource_prop_to_prop("identifier")

    def map_language(self):
        languages = iterify(
            getprop(self.provider_data, "sourceResource/language", True))
        language_names = FloridaMapper.get_names(languages)
        if language_names:
            filtered_language_names = []
            for language_name in language_names:
                filtered_language_names.append(language_name.replace("Greek, Ancient (to 1453)", "Ancient Greek (to 1453)"))
            self.update_source_resource({"language": filtered_language_names})
        else:
            isos = [getprop(language, "iso_639_3")
                    for language in languages if exists(language, "iso_639_3")]
            if isos:
                self.update_source_resource({"language": isos})

    def map_publisher(self):
        self.source_resource_prop_to_prop("publisher")

    def map_relation(self):
        pass

    def map_rights(self):
        rights_strings = []
        for rights in iterify(
                getprop(self.provider_data, "sourceResource/rights", True)):
            if exists(rights, "text"):
                rights_strings.append(rights["text"])
        if rights_strings:
            self.update_source_resource({"rights": rights_strings})

    def map_spatial(self):
        parents = iterify(getprop(self.provider_data, "sourceResource/spatial", True))
        names = FloridaMapper.get_names(parents)
        florida_names = []
        for name in names:
            florida_names.append(name.replace("Fla.", "Florida"))
        if names:
            self.update_source_resource({"spatial": florida_names})

    def map_spec_type(self):
        pass

    def map_state_located_in(self):
        pass

    def map_subject(self):
        self.map_multi_names("sourceResource/subject", "subject")

    def map_temporal(self):
        pass

    def map_title(self):
        self.source_resource_prop_to_prop("title")

    def map_type(self):
        type_map = {
            "cartographic": "image",
            "image": "image",
            "mixed material": "collection",
            "moving image": "video",
            "notated music": "image",
            "sound": "sound",
            "sound recording": "sound",
            "still image": "image",
            "text": "text",
            "three dimensional object": "physical object"
        }
        in_types = iterify(getprop(self.provider_data, "sourceResource/type", True))
        out_types = []
        for in_type in in_types:
            if in_type.lower() in type_map:
                out_type = type_map[in_type.lower()]
                if out_type:
                    out_types.append(out_type)
            else:
                logger.warn("Unable to map type: " + in_type)
        if out_types:
            self.update_source_resource({"type": out_types})

    def map_multiple_fields(self):
        self.map_alternative_title()
        self.map_preview()
