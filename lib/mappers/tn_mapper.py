from akara import logger
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mods_mapper import MODSMapper
from dplaingestion.textnode import textnode
import re


class TNMapper(MODSMapper):
    def __init__(self, provider_data):
        super(TNMapper, self).__init__(provider_data)

    def map_multiple_fields(self):
        super(MODSMapper, self).map_multiple_fields()
        self.map_spatial_and_subject_and_temporal()

    def map_is_part_of(self):
        path = "/metadata/mods/relatedItem"
        collections = []

        if exists(self.provider_data, path):
            for relatedItem in getprop(self.provider_data, path):
                title, description = "", ""
                if "displayLabel" in relatedItem:
                    if "titleInfo" in relatedItem:
                        title = relatedItem["titleInfo"]["title"]

                    if "abstract" in relatedItem:
                        description = relatedItem["abstract"]

                    collections.append({"title": title,
                                        "description": description,
                                        "@id": "",
                                        "id": ""})

            self.update_source_resource({"collection": collections})

    # helper function for the next two functions
    def name_part(self, type):
        prop = "/metadata/mods/name"
        results = []
        if exists(self.provider_data, prop):
            for name in getprop(self.provider_data, prop):
                if "role" in name and "namePart" in name:
                    for role in iterify(name["role"]):
                        role_prop = "roleTerm/#text"
                        if exists(role, role_prop) \
                                and getprop(role, role_prop) == type:
                            results.append(name["namePart"])

        return results

    def map_contributor(self):
        contributor = self.name_part("Contributor")
        if len(contributor) > 0:
            self.update_source_resource({"contributor": contributor})

    def map_creator(self):
        creators = self.name_part("Creator")
        if len(creators) > 0:
            self.update_source_resource({"creator": creators})

    def map_date(self):
        path = "/metadata/mods/originInfo/dateCreated/#text"
        if exists(self.provider_data, path):
            date_created = getprop(self.provider_data, path)
            self.update_source_resource({"date": {"displayDate": date_created}})

    def map_description(self):
        path = "/metadata/mods/abstract"
        if exists(self.provider_data, path):
            description = getprop(self.provider_data, path)
            self.update_source_resource({"description": description})

    def map_extent(self):
        path = "/metadata/mods/physicalDescription/extent"
        if exists(self.provider_data, path):
            extents = textnode(getprop(self.provider_data, path))
            self.update_source_resource({"extent": extents})

    def map_format(self):
        path = "/metadata/mods/physicalDescription/form"
        formats = []
        if exists(self.provider_data, path):
            for f in iterify(getprop(self.provider_data, path)):
                formats.append(textnode(f))
        if formats:
            self.update_source_resource({"format": formats})

    def map_identifier(self):
        path = "/metadata/mods/identifier"
        identifiers = []
        if exists(self.provider_data, path):
            for id in iterify(getprop(self.provider_data, path)):
                identifiers.append(textnode(id))
        if identifiers:
            self.update_source_resource({"identifier": identifiers})

    def map_language(self):
        path = "/metadata/mods/language/languageTerm"
        if exists(self.provider_data, path):
            language_term = getprop(self.provider_data, path)
            if "type" in language_term \
                    and "authority" in language_term \
                    and language_term["type"] == "code" \
                    and language_term["authority"] == "iso639-2b":
                self.update_source_resource({"language":
                                             textnode(language_term)})

    def map_publisher(self):
        path = "/metadata/mods/originInfo/publisher"
        if exists(self.provider_data, path):
            publisher = textnode(getprop(self.provider_data, path))
            self.update_source_resource({"publisher": textnode(publisher)})

    def map_rights(self):
        path = "/metadata/mods/accessCondition"
        if exists(self.provider_data, path):
            rights = textnode(getprop(self.provider_data, path))
            self.update_source_resource({"rights": textnode(rights)})


    def map_spatial_and_subject_and_temporal(self):
        # "<subject><geographic authority="""" valueURI="""">[text term]</geographic>"
        path = "/metadata/mods/subject"
        subject_props = ['topic', 'genre', 'occupation', 'titleInfo']
        spatials = []
        temporals = []
        subjects = []

        if exists(self.provider_data, path):
            for subject in iterify(getprop(self.provider_data, path)):
                if "cartographics" in subject and "coordinates" in subject["cartographics"]:
                    coord = subject["cartographics"]["coordinates"]
                    clean_coord = re.sub("[NnSsEeWw]", "", coord)
                    spatials.append({"coodinates": clean_coord})

                if "geographic" in subject:
                    spatials.append({"name": textnode(subject["geographic"])})

                if "temporals" in subject:
                    temporals.append(textnode(subject["temporal"]))

                for b in subject_props:
                    if b in subject:
                        subjects.append(textnode(subject[b]))

        self.update_source_resource({"spatial": spatials,
                                     "temporal": temporals,
                                     "subject": subjects})

    def map_title(self):
        path = "/metadata/mods/titleInfo/title"
        if exists(self.provider_data, path):
            self.update_source_resource(
                {"title": textnode(getprop(self.provider_data, path))}
            )

    def map_type(self):
        path = "/metadata/mods/physicalDescription/form"
        if exists(self.provider_data, path):
            type = textnode(getprop(self.provider_data, path))
            self.update_source_resource({"type": textnode(type)})

    def map_data_provider(self, prop="source"):
        path = "/metadata/mods/recordInfo/recordContentSource"
        if exists(self.provider_data, path):
            data_provider = getprop(self.provider_data, path)
            self.mapped_data.update({"dataProvider": textnode(data_provider)})

    def map_is_shown_at(self):
        path = "/metadata/mods/location/url"
        if exists(self.provider_data, path):
            for location_url in iterify(getprop(self.provider_data, path)):
                if "usage" in location_url \
                        and "access" in location_url \
                        and location_url["usage"].startswith("primary ") \
                        and location_url["access"] == "object in context":
                    self.mapped_data.update(
                        {"isShownAt": textnode(location_url)}
                    )

    def map_object(self):
        path = "/metadata/mods/location/url"
        if exists(self.provider_data, path):
            for url in iterify(getprop(self.provider_data, path)):
                if "access" in url and url["access"] == "preview":
                    self.mapped_data.update({"object": textnode(url)})

    def map_intermediate_provider(self):
        path = "/metadata/mods/note"
        intermediate_providers = []
        if exists(self.provider_data, path):
            for note in iterify(getprop(self.provider_data, path)):
                if "displayLabel" in note \
                        and "#text" in note \
                        and note["displayLabel"] == "Intermediate Provider":
                    intermediate_providers.append(textnode(note))

        if intermediate_providers:
            self.mapped_data.update(
                {"intermediateProvider": intermediate_providers}
            )

    def log(self, label, obj):
        logger.error(label + ": " + str(obj))
